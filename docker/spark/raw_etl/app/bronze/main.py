"""Apply raw landing batches to Bronze Delta tables."""

import logging
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from pyspark.sql import SparkSession, functions as F

from etl_utils import (
    configure_s3,
    exists,
    get_env,
    list_dirs,
    parse_table_list,
    parse_optional_datetime,
    partition_info,
    read_json,
    set_spark_log_level,
    setup_logging,
    state_path,
    table_base_path,
)
from bronze.apply_utils import load_registry_schema, write_registry_rows
from bronze.facts_events import apply_facts_events_batches
from bronze.mutable_dims import apply_mutable_dim_batches
from bronze.snapshots import apply_snapshot_batches
from bronze.uc import (
    align_df_to_uc_schema,
    ensure_uc_schema,
    ensure_uc_external_delta_table,
    parse_bool_flag,
)
from etl_parser import parse_bronze_args, resolve_bronze_groups

APP_NAME = "raw-to-bronze-etl"


def _list_partitions(
    spark: SparkSession, base_path: str, partition_key: str
) -> list[date]:
    """List available partition dates for a landing table.

    Args:
        spark: Active SparkSession, e.g. SparkSession.builder.getOrCreate().
        base_path: Table base path, e.g. "s3a://ampere-raw/postgres-pre-raw/source/orders".
        partition_key: Partition type, e.g. "snapshot_date" or "event_date".

    Examples:
        _list_partitions(spark, "s3a://ampere-raw/.../orders", "event_date")
    """
    mode_path = (
        f"{base_path}/mode=snapshot"
        if partition_key == "snapshot_date"
        else f"{base_path}/mode=incremental"
    )
    partition_dirs = list_dirs(spark, mode_path)
    dates = []
    prefix = f"{partition_key}="
    for entry in partition_dirs:
        if entry.startswith(prefix):
            dates.append(date.fromisoformat(entry.split("=", 1)[1]))
    return sorted(dates)


def _search_start_date(
    partition_key: str,
    registry_df,
    state_last_ingest: Optional[datetime],
    run_date: date,
    lookback_days: int,
    has_registry_rows: bool,
) -> Optional[date]:
    """Find the earliest date to consider for new batches.

    Args:
        partition_key: Partition type, e.g. "snapshot_date", "extract_date", "event_date".
        registry_df: Registry DataFrame or None, e.g. spark.read.table("`ampere`.`ops`.`bronze_apply_registry`").
        state_last_ingest: Last ingest timestamp, e.g. datetime(2026, 1, 24, tzinfo=UTC).
        run_date: Run date, e.g. date(2026, 1, 24).
        lookback_days: Lookback window in days, e.g. 2.
        has_registry_rows: Whether registry has any rows for the table, e.g. True.

    Examples:
        _search_start_date("event_date", registry_df, None, date(2026, 1, 24), 2, False)
    """
    registry_date = None
    if registry_df is not None:
        row = registry_df.select(F.max("partition_value").alias("pv")).first()
        if row and row.pv:
            registry_date = date.fromisoformat(row.pv)

    candidates = []
    if registry_date:
        candidates.append(registry_date)
    if state_last_ingest and not (
        partition_key == "extract_date" and not has_registry_rows
    ):
        candidates.append(state_last_ingest.date())

    if not candidates:
        if partition_key == "event_date":
            if not has_registry_rows and state_last_ingest is None:
                return None
            if lookback_days < 1:
                lookback_days = 1
            return run_date - timedelta(days=lookback_days - 1)
        return None

    start = min(candidates)
    buffer_days = 0
    if partition_key == "extract_date":
        buffer_days = 1
    elif partition_key == "event_date":
        buffer_days = lookback_days
    return start - timedelta(days=buffer_days)


def _candidate_runs(
    spark: SparkSession,
    base_path: str,
    partition_key: str,
    search_start: Optional[date],
) -> list[dict]:
    """Collect run folders that contain _SUCCESS and _manifest.json.

    Args:
        spark: Active SparkSession, e.g. SparkSession.builder.getOrCreate().
        base_path: Table base path, e.g. "s3a://ampere-raw/postgres-pre-raw/source/orders".
        partition_key: Partition type, e.g. "snapshot_date".
        search_start: Earliest date to consider, e.g. date(2026, 1, 20) or None.

    Examples:
        _candidate_runs(spark, "s3a://ampere-raw/.../orders", "event_date", date(2026, 1, 20))
    """
    candidates = []
    partitions = _list_partitions(spark, base_path, partition_key)

    for partition_date in partitions:
        if search_start and partition_date < search_start:
            continue
        if partition_key == "snapshot_date":
            partition_path = (
                f"{base_path}/mode=snapshot/snapshot_date={partition_date.isoformat()}"
            )
        else:
            partition_path = (
                f"{base_path}/mode=incremental/{partition_key}={partition_date.isoformat()}"
            )
        run_dirs = list_dirs(spark, partition_path)
        for run_dir in run_dirs:
            if not run_dir.startswith("run_id="):
                continue
            run_id = run_dir.split("=", 1)[1]
            run_path = f"{partition_path}/{run_dir}"
            success_path = f"{run_path}/_SUCCESS"
            manifest_path = f"{run_path}/_manifest.json"
            if not exists(spark, success_path):
                continue
            if not exists(spark, manifest_path):
                continue
            candidates.append(
                {
                    "run_id": run_id,
                    "manifest_path": manifest_path,
                    "partition_kind": partition_key,
                    "partition_value": partition_date.isoformat(),
                }
            )
    return candidates


def _apply_group_shuffle(
    spark: SparkSession,
    group_name: str,
    shuffle_partitions: Optional[int],
    files_max_partition_bytes: Optional[str],
    files_open_cost_bytes: Optional[str],
    adaptive_coalesce: Optional[bool],
) -> None:
    """Override spark.sql.shuffle.partitions for the current group when set.

    Args:
        spark: Active SparkSession, e.g. SparkSession.builder.getOrCreate().
        group_name: Group label, e.g. "snapshots" or "facts".
        shuffle_partitions: Partition count, e.g. 1 or None to keep Spark default.

    Examples:
        _apply_group_shuffle(spark, "facts", 4)
    """
    if shuffle_partitions:
        spark.conf.set("spark.sql.shuffle.partitions", str(shuffle_partitions))
        spark.conf.set("spark.default.parallelism", str(shuffle_partitions))
        logging.getLogger(APP_NAME).info(
            "Set spark.sql.shuffle.partitions=%s and spark.default.parallelism=%s for group=%s",
            shuffle_partitions,
            shuffle_partitions,
            group_name,
        )
    if files_max_partition_bytes:
        spark.conf.set(
            "spark.sql.files.maxPartitionBytes", files_max_partition_bytes
        )
        logging.getLogger(APP_NAME).info(
            "Set spark.sql.files.maxPartitionBytes=%s for group=%s",
            files_max_partition_bytes,
            group_name,
        )
    if files_open_cost_bytes:
        spark.conf.set("spark.sql.files.openCostInBytes", files_open_cost_bytes)
        logging.getLogger(APP_NAME).info(
            "Set spark.sql.files.openCostInBytes=%s for group=%s",
            files_open_cost_bytes,
            group_name,
        )
    if adaptive_coalesce is not None:
        spark.conf.set(
            "spark.sql.adaptive.coalescePartitions.enabled",
            "true" if adaptive_coalesce else "false",
        )
        logging.getLogger(APP_NAME).info(
            "Set spark.sql.adaptive.coalescePartitions.enabled=%s for group=%s",
            "true" if adaptive_coalesce else "false",
            group_name,
        )


def _run_bronze_maintenance(
    spark: SparkSession,
    *,
    catalog: str,
    schema: str,
    tables: list[str],
    logger: logging.Logger,
) -> None:
    """Run OPTIMIZE and VACUUM for the selected bronze tables."""
    for table in tables:
        fqtn = f"`{catalog}`.`{schema}`.`{table}`"
        if not spark.catalog.tableExists(f"{catalog}.{schema}.{table}"):
            raise ValueError(f"Bronze maintenance target is missing from UC: {fqtn}")
        logger.info("Starting OPTIMIZE for %s", fqtn)
        spark.sql(f"OPTIMIZE {fqtn}").collect()
        logger.info("Completed OPTIMIZE for %s", fqtn)
        logger.info("Starting VACUUM RETAIN 168 HOURS for %s", fqtn)
        spark.sql(f"VACUUM {fqtn} RETAIN 168 HOURS").collect()
        logger.info("Completed VACUUM RETAIN 168 HOURS for %s", fqtn)


def main() -> None:
    """Apply raw landing batches to bronze Delta tables.

    The flow loads registry metadata, discovers candidate runs, validates manifests,
    writes Delta data, and appends status rows into the registry.

    Examples:
        python raw_to_bronze_etl.py --groups-config '[{"group":"facts","tables":["orders"]}]'
    """
    # Step 1: Initialize logging and parse CLI inputs.
    # This fixes the runtime configuration for tables, groups, and run date.
    # The expected outcome is a fully populated args object before Spark starts.
    setup_logging()
    logger = logging.getLogger(APP_NAME)

    args = parse_bronze_args()
    uc_enabled = parse_bool_flag(args.uc_enabled, default=True)
    maintenance_only = parse_bool_flag(args.maintenance_only, default=False)
    uc_catalog = (args.uc_catalog or "").strip()
    uc_bronze_schema = (args.uc_bronze_schema or "bronze").strip()
    uc_ops_schema = (args.uc_ops_schema or "ops").strip()
    if not uc_enabled:
        raise ValueError("Bronze apply is UC-only. Set --uc-enabled=true.")
    if not uc_catalog:
        raise ValueError("--uc-catalog is required when --uc-enabled=true.")
    run_date_str = args.run_date or date.today().isoformat()
    run_date = date.fromisoformat(run_date_str)
    maintenance_tables = parse_table_list(args.maintenance_tables)

    # Step 2: Normalize group config and shuffle settings.
    # This decides how many groups run and which shuffle override each group gets.
    # The expected outcome is a structured list of groups ready for execution.
    groups = []
    default_tables = None
    using_groups_config = False
    if not maintenance_only:
        groups, default_tables, using_groups_config = resolve_bronze_groups(args)
    elif not maintenance_tables:
        raise ValueError(
            "--maintenance-tables is required when --maintenance-only=true."
        )

    minio_endpoint = get_env(
        "MINIO_S3_ENDPOINT", "http://minio.ampere.svc.cluster.local:9000"
    )
    minio_access_key = get_env("MINIO_ACCESS_KEY")
    minio_secret_key = get_env("MINIO_SECRET_KEY")
    if not minio_access_key or not minio_secret_key:
        raise ValueError("Missing MINIO_ACCESS_KEY/MINIO_SECRET_KEY for MinIO.")

    if maintenance_only:
        logger.info(
            "Starting bronze maintenance for %s",
            ",".join(maintenance_tables),
        )
    elif using_groups_config:
        group_names = ",".join([g.get("group", "group") for g in groups])
        logger.info(
            "Starting bronze load for %s groups (%s), run_date=%s",
            len(groups),
            group_names,
            run_date_str,
        )
    else:
        logger.info(
            "Starting bronze load for %s (mode=%s, run_date=%s)",
            ",".join(default_tables or []),
            args.mode,
            run_date_str,
        )

    # Step 3: Start Spark and configure MinIO access.
    # This prepares the session for reading raw data and writing Delta outputs.
    # The expected outcome is a SparkSession configured with S3A credentials.
    spark = (
        SparkSession.builder.appName(args.app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    configure_s3(spark, minio_endpoint, minio_access_key, minio_secret_key)
    set_spark_log_level(spark)
    ensure_uc_schema(spark, uc_catalog, uc_bronze_schema, logger)
    ensure_uc_schema(spark, uc_catalog, uc_ops_schema, logger)
    logger.info("UC-only bronze apply enabled: bronze targets and registry are resolved from UC.")

    if maintenance_only:
        _run_bronze_maintenance(
            spark,
            catalog=uc_catalog,
            schema=uc_bronze_schema,
            tables=maintenance_tables,
            logger=logger,
        )
        spark.stop()
        return

    # Step 4: Load the registry table to track applied batches.
    # This drives idempotency and prevents duplicate loads per partition.
    # The expected outcome is a Delta-backed DataFrame or None when missing.
    # Registry is the source of truth for applied landing batches.
    schema_path = get_env(
        "BRONZE_REGISTRY_SCHEMA_PATH",
        str(Path(__file__).with_name("bronze_apply_registry_schema.json")),
    )
    registry_schema = load_registry_schema(schema_path)
    registry_table_name = ensure_uc_external_delta_table(
        spark=spark,
        catalog=uc_catalog,
        schema=uc_ops_schema,
        table="bronze_apply_registry",
        logger=logger,
    )
    registry_df = spark.read.table(registry_table_name)

    def _align_to_uc_bronze_schema(table_name: str, df):
        """Align a table DataFrame to UC schema before bronze write/merge."""
        return align_df_to_uc_schema(
            spark=spark,
            df=df,
            catalog=uc_catalog,
            schema=uc_bronze_schema,
            table=table_name,
            logger=logger,
        )

    for group in groups:
        # Step 5: Apply group-level overrides and resolve tables to process.
        # This allows per-group tuning (like shuffle partitions) before table work.
        # The expected outcome is a non-empty table list for the group.
        group_name = group.get("group", "group")
        group_tables = group.get("tables", [])
        if not group_tables:
            logger.info("No tables configured for group %s", group_name)
            continue
        _apply_group_shuffle(
            spark,
            group_name,
            group.get("shuffle_partitions"),
            group.get("files_max_partition_bytes"),
            group.get("files_open_cost_bytes"),
            group.get("adaptive_coalesce"),
        )
        table_config = group.get("table_config", {})
        partition_key = group.get("partition_key", "snapshot_date")
        lookback_days = group.get("lookback_days", 0)
        logger.info(
            "Processing group %s (%s tables) partition_key=%s lookback_days=%s",
            group_name,
            ",".join(group_tables),
            partition_key,
            lookback_days,
        )

        for table in group_tables:
            # Step 6: Build per-table context from the registry and state files.
            # This determines the search window and expected schema/contract.
            # The expected outcome is a search_start date and applied batch set.
            registry_rows = []
            table_meta = table_config.get(table, {})
            merge_keys = table_meta.get("merge_keys", [])
            table_lookback_days = table_meta.get("lookback_days")
            if table_lookback_days is None:
                table_lookback_days = lookback_days
            table_lookback_days = int(table_lookback_days or 0)
            logger.info(
                "Table %s uses lookback_days=%s",
                table,
                table_lookback_days,
            )
            raw_base = table_base_path(
                args.raw_bucket, args.raw_prefix, args.schema, table
            )
            bronze_table_name = ensure_uc_external_delta_table(
                spark=spark,
                catalog=uc_catalog,
                schema=uc_bronze_schema,
                table=table,
                logger=logger,
            )

            table_registry = None
            applied_batches = set()
            expected_schema_hash = None
            expected_contract_version = None
            if registry_df is not None:
                table_registry = registry_df.filter(
                    (F.col("source_system") == args.source_system)
                    & (F.col("source_schema") == args.schema)
                    & (F.col("source_table") == table)
                )
                applied_batches = {
                    (row.run_id, row.partition_value)
                    for row in table_registry.filter(
                        F.col("status").isin("applied", "skipped")
                    )
                    .select("run_id", "partition_value")
                    .collect()
                }
                latest_row = (
                    table_registry.orderBy(F.col("apply_ts_utc").desc()).limit(1).collect()
                )
                if latest_row:
                    expected_schema_hash = latest_row[0].schema_hash
                    expected_contract_version = latest_row[0].contract_version
            has_registry_rows = bool(latest_row) if registry_df is not None else False

            state_last_ingest = None
            if partition_key == "extract_date":
                state_path_value = state_path(
                    args.raw_bucket, args.source_system, args.schema, table
                )
                state = read_json(spark, state_path_value, logger)
                if state and state.get("last_successful_ingest_ts_utc"):
                    state_last_ingest = parse_optional_datetime(
                        state["last_successful_ingest_ts_utc"]
                    )
    
            search_start = _search_start_date(
                partition_key,
                table_registry,
                state_last_ingest,
                run_date,
                table_lookback_days,
                has_registry_rows,
            )

            # Step 7: Discover candidate runs and load manifests.
            # This filters to batches with _SUCCESS and builds the apply queue.
            # The expected outcome is a list of candidate batches ready for validation.
            candidates = _candidate_runs(
                spark, raw_base, partition_key, search_start
            )
            if not candidates:
                logger.info("No candidate batches for %s", table)
                continue
    
            apply_queue = []
            seen_batches = set()
            for candidate in candidates:
                manifest = read_json(spark, candidate["manifest_path"], logger)
                if not manifest:
                    batch_apply_ts = datetime.now(timezone.utc).isoformat()
                    logger.warning(
                        "Manifest missing or invalid for %s run_id=%s path=%s",
                        table,
                        candidate["run_id"],
                        candidate["manifest_path"],
                    )
                    registry_rows.append(
                        {
                            "source_system": args.source_system,
                            "source_schema": args.schema,
                            "source_table": table,
                            "run_id": candidate["run_id"],
                            "manifest_path": candidate["manifest_path"],
                            "batch_type": None,
                            "partition_kind": candidate["partition_kind"],
                            "partition_value": candidate["partition_value"],
                            "ingest_ts_utc": None,
                            "schema_hash": None,
                            "contract_version": None,
                            "apply_ts_utc": batch_apply_ts,
                            "status": "failed",
                            "details": "missing or invalid manifest",
                            "watermark_from": None,
                            "watermark_to": None,
                            "lookback_days": None,
                            "window_from": None,
                            "window_to": None,
                            "row_count": None,
                            "file_count": None,
                        }
                    )
                    continue
                candidate["manifest"] = manifest
                candidate["ingest_ts_utc"] = manifest.get("ingest_ts_utc")
                candidate["run_id"] = manifest.get("run_id", candidate["run_id"])
                partition_kind, partition_value = partition_info(manifest)
                if not partition_kind or not partition_value:
                    partition_kind = candidate["partition_kind"]
                    partition_value = candidate["partition_value"]
                    logger.warning(
                        "Manifest missing partition info for %s run_id=%s, using path %s=%s",
                        table,
                        candidate["run_id"],
                        partition_kind,
                        partition_value,
                    )
                candidate["partition_kind"] = partition_kind
                candidate["partition_value"] = partition_value
                batch_key = (candidate["run_id"], candidate["partition_value"])
                if batch_key in applied_batches or batch_key in seen_batches:
                    continue
                seen_batches.add(batch_key)
                apply_queue.append(candidate)
            if not apply_queue:
                logger.info("No new batches to apply for %s", table)
                write_registry_rows(
                    spark,
                    registry_table_name,
                    registry_schema,
                    registry_rows,
                )
                continue

            def _apply_sort_key(batch: dict) -> tuple:
                """Sort by partition, ingest timestamp, then run id.

                Args:
                    batch: Candidate batch dict, e.g. {"partition_value": "2026-01-24"}.

                Examples:
                    _apply_sort_key({"partition_value": "2026-01-24", "run_id": "r1"})
                """
                ingest_dt = parse_optional_datetime(
                    batch.get("ingest_ts_utc") or ""
                )
                if ingest_dt and ingest_dt.tzinfo is None:
                    ingest_dt = ingest_dt.replace(tzinfo=timezone.utc)
                ingest_key = ingest_dt or datetime(1, 1, 1, tzinfo=timezone.utc)
                return (batch["partition_value"], ingest_key, batch["run_id"])



            # Step 8: Apply batches to bronze and record outcomes in the registry.
            # The ETL delegates to per-table-type writers to keep logic easy to follow.
            # The expected outcome is applied/skipped rows in the registry for each batch.
            sorted_queue = sorted(apply_queue, key=_apply_sort_key)
            if partition_key == "snapshot_date":
                apply_snapshot_batches(
                    spark=spark,
                    table=table,
                    bronze_table_name=bronze_table_name,
                    registry_schema=registry_schema,
                    registry_rows=registry_rows,
                    source_system=args.source_system,
                    source_schema=args.schema,
                    sorted_batches=sorted_queue,
                    expected_schema_hash=expected_schema_hash,
                    expected_contract_version=expected_contract_version,
                    logger=logger,
                    align_to_target_schema=_align_to_uc_bronze_schema,
                )
            elif partition_key == "extract_date":
                apply_mutable_dim_batches(
                    spark=spark,
                    table=table,
                    bronze_table_name=bronze_table_name,
                    merge_keys=merge_keys,
                    registry_schema=registry_schema,
                    registry_rows=registry_rows,
                    source_system=args.source_system,
                    source_schema=args.schema,
                    sorted_batches=sorted_queue,
                    expected_schema_hash=expected_schema_hash,
                    expected_contract_version=expected_contract_version,
                    logger=logger,
                    align_to_target_schema=_align_to_uc_bronze_schema,
                )
            else:
                apply_facts_events_batches(
                    spark=spark,
                    table=table,
                    bronze_table_name=bronze_table_name,
                    merge_keys=merge_keys,
                    registry_schema=registry_schema,
                    registry_rows=registry_rows,
                    source_system=args.source_system,
                    source_schema=args.schema,
                    sorted_batches=sorted_queue,
                    lookback_days=table_lookback_days,
                    append_only_override=table_meta.get("append_only"),
                    expected_schema_hash=expected_schema_hash,
                    expected_contract_version=expected_contract_version,
                    logger=logger,
                    align_to_target_schema=_align_to_uc_bronze_schema,
                )
            write_registry_rows(
                spark,
                registry_table_name,
                registry_schema,
                registry_rows,
            )
            # Release cached data between tables to limit driver memory growth.
            spark.catalog.clearCache()

    spark.stop()


if __name__ == "__main__":
    main()
