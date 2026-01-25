"""Apply raw landing batches to Bronze Delta tables."""

import argparse
import json
import logging
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from etl_utils import (
    bronze_registry_path,
    configure_s3,
    exists,
    get_env,
    list_dirs,
    load_registry_table,
    manifest_ok,
    parse_date,
    parse_optional_datetime,
    partition_info,
    read_json,
    set_spark_log_level,
    setup_logging,
    state_path,
    table_base_path,
)

try:
    from delta.tables import DeltaTable
except ImportError as exc:
    raise ImportError(
        "delta-spark is required for bronze writes. Ensure Delta jars are on the classpath."
    ) from exc

APP_NAME = "raw-to-bronze-etl"


def _parse_args() -> argparse.Namespace:
    """Parse CLI args for raw-to-bronze processing.

    Example CLI inputs:
        --tables "orders,customers"
        --groups-config '[{"group":"snapshots","tables":["orders"],"shuffle_partitions":1}]'
        --shuffle-partitions 4
        --run-date "2026-01-24"
    """
    parser = argparse.ArgumentParser(
        description="Transform raw landing data to bronze Delta tables."
    )
    parser.add_argument(
        "--tables",
        default="",
        help="Comma-separated table list for group extraction",
    )
    parser.add_argument(
        "--table-config",
        default="",
        help="JSON mapping of table-specific overrides",
    )
    parser.add_argument(
        "--groups-config",
        default="",
        help="JSON list with group settings for multi-group execution",
    )
    parser.add_argument("--schema", default="source", help="Source schema name")
    parser.add_argument("--run-date", type=parse_date, help="YYYY-MM-DD")
    parser.add_argument(
        "--mode",
        choices=("snapshot", "incremental"),
        default="snapshot",
        help="Storage mode for bronze tables",
    )
    parser.add_argument(
        "--partition-key",
        choices=("snapshot_date", "extract_date", "event_date"),
        default="snapshot_date",
        help="Partition key for raw landing layout",
    )
    parser.add_argument(
        "--event-date-column",
        default="",
        help="Date/timestamp column for event/fact extractions",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=0,
        help="Lookback window for event extraction (days)",
    )
    parser.add_argument(
        "--raw-bucket",
        default="ampere-raw",
        help="Raw landing bucket",
    )
    parser.add_argument(
        "--raw-prefix",
        default="postgres-pre-raw",
        help="Raw landing prefix",
    )
    parser.add_argument(
        "--bronze-bucket",
        default="ampere-bronze",
        help="Bronze bucket",
    )
    parser.add_argument(
        "--bronze-prefix",
        default="bronze",
        help="Bronze prefix",
    )
    parser.add_argument(
        "--source-system",
        default="postgres-pre-raw",
        help="Source system identifier",
    )
    parser.add_argument(
        "--shuffle-partitions",
        type=int,
        default=0,
        help="Override spark.sql.shuffle.partitions (0 keeps Spark default)",
    )
    parser.add_argument("--app-name", default=APP_NAME, help="Spark app name")
    parser.add_argument("--image", default="", help="Container image reference")
    return parser.parse_args()


def _parse_table_list(raw: str) -> list[str]:
    """Split a comma-separated table list into clean tokens.

    Args:
        raw: Comma-separated names, e.g. "orders, customers".
    """
    return [t.strip() for t in raw.split(",") if t.strip()]


def _parse_groups_config(
    raw: str,
    default_shuffle_partitions: Optional[int],
) -> list[dict]:
    """Parse group config JSON into a normalized list of group dicts.

    Args:
        raw: JSON list string, e.g. '[{"group":"snapshots","tables":["orders"]}]'.
        default_shuffle_partitions: Default override, e.g. 4 or None.
    """
    if not raw:
        return []
    data = json.loads(raw)
    if not isinstance(data, list):
        raise ValueError("groups-config must be a JSON list")
    groups = []
    for item in data:
        if not isinstance(item, dict):
            raise ValueError("groups-config entries must be objects")
        tables = item.get("tables", [])
        if isinstance(tables, str):
            tables = _parse_table_list(tables)
        shuffle_partitions = item.get("shuffle_partitions", default_shuffle_partitions)
        if shuffle_partitions in ("", None):
            shuffle_partitions = None
        else:
            shuffle_partitions = int(shuffle_partitions)
            if shuffle_partitions <= 0:
                shuffle_partitions = None
        group = {
            "group": item.get("group", "group"),
            "mode": item.get("mode", "snapshot"),
            "partition_key": item.get("partition_key", "snapshot_date"),
            "event_date_column": item.get("event_date_column", ""),
            "lookback_days": int(item.get("lookback_days", 0) or 0),
            "tables": tables,
            "table_config": item.get("table_config", {}) or {},
            "shuffle_partitions": shuffle_partitions,
        }
        groups.append(group)
    return groups


def _load_registry_schema(schema_path: str) -> StructType:
    """Load the registry schema JSON template into a Spark StructType.

    Args:
        schema_path: JSON schema path, e.g. "/opt/spark/app/bronze_apply_registry_schema.json".
    """
    data = json.loads(Path(schema_path).read_text())
    type_map = {
        "string": StringType(),
        "int": IntegerType(),
    }
    fields = []
    for field in data.get("fields", []):
        field_type = type_map.get(field.get("type"))
        if field_type is None:
            raise ValueError(f"Unsupported registry field type: {field.get('type')}")
        fields.append(
            StructField(
                field.get("name"),
                field_type,
                bool(field.get("nullable", True)),
            )
        )
    if not fields:
        raise ValueError("Registry schema template has no fields.")
    return StructType(fields)


def _append_registry_row(
    spark: SparkSession, path_str: str, schema: StructType, row: dict
) -> None:
    """Append a single apply-registry row with retry on concurrent commit errors.

    Args:
        spark: Active SparkSession, e.g. SparkSession.builder.getOrCreate().
        path_str: Registry table path, e.g. "s3a://ampere-bronze/bronze/ops/bronze_apply_registry".
        schema: Registry schema, e.g. StructType([...]).
        row: Row data, e.g. {"source_table": "orders", "status": "applied"}.
    """
    logger = logging.getLogger(APP_NAME)
    df = spark.createDataFrame([row], schema=schema)
    retries = 5
    last_exc = None
    retry_tokens = (
        "ProtocolChangedException",
        "ConcurrentAppendException",
        "ConcurrentWriteException",
        "CommitFailedException",
    )
    for attempt in range(retries):
        try:
            df.write.format("delta").mode("append").save(path_str)
            return
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if any(token in str(exc) for token in retry_tokens):
                logger.warning(
                    "Registry append conflict; retrying (%s/%s).",
                    attempt + 1,
                    retries,
                )
                time.sleep(1 + attempt)
                continue
            raise
    if last_exc:
        raise last_exc




def _list_partitions(
    spark: SparkSession, base_path: str, partition_key: str
) -> list[date]:
    """List available partition dates for a landing table.

    Args:
        spark: Active SparkSession, e.g. SparkSession.builder.getOrCreate().
        base_path: Table base path, e.g. "s3a://ampere-raw/postgres-pre-raw/source/orders".
        partition_key: Partition type, e.g. "snapshot_date" or "event_date".
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
        registry_df: Registry DataFrame or None, e.g. spark.read.format("delta").load(...).
        state_last_ingest: Last ingest timestamp, e.g. datetime(2026, 1, 24, tzinfo=UTC).
        run_date: Run date, e.g. date(2026, 1, 24).
        lookback_days: Lookback window in days, e.g. 2.
        has_registry_rows: Whether registry has any rows for the table, e.g. True.
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


def _merge_to_delta(
    spark: SparkSession,
    df,
    target_path: str,
    merge_keys: list[str],
) -> None:
    """Merge or overwrite a Delta table using stable business keys.

    Args:
        spark: Active SparkSession, e.g. SparkSession.builder.getOrCreate().
        df: Source DataFrame, e.g. spark.read.parquet("s3a://...").
        target_path: Delta table path, e.g. "s3a://ampere-bronze/bronze/source/orders".
        merge_keys: Business keys, e.g. ["order_id"].
    """
    if DeltaTable.isDeltaTable(spark, target_path):
        delta_table = DeltaTable.forPath(spark, target_path)
        conditions = " AND ".join([f"t.{k} = s.{k}" for k in merge_keys])
        (
            delta_table.alias("t")
            .merge(df.alias("s"), conditions)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        df.write.format("delta").mode("overwrite").save(target_path)


def _apply_group_shuffle(
    spark: SparkSession,
    group_name: str,
    shuffle_partitions: Optional[int],
) -> None:
    """Override spark.sql.shuffle.partitions for the current group when set.

    Args:
        spark: Active SparkSession, e.g. SparkSession.builder.getOrCreate().
        group_name: Group label, e.g. "snapshots" or "facts".
        shuffle_partitions: Partition count, e.g. 1 or None to keep Spark default.
    """
    if not shuffle_partitions:
        return
    spark.conf.set("spark.sql.shuffle.partitions", str(shuffle_partitions))
    spark.conf.set("spark.default.parallelism", str(shuffle_partitions))
    logging.getLogger(APP_NAME).info(
        "Set spark.sql.shuffle.partitions=%s and spark.default.parallelism=%s for group=%s",
        shuffle_partitions,
        shuffle_partitions,
        group_name,
    )


def main() -> None:
    """Apply raw landing batches to bronze Delta tables.

    The flow loads registry metadata, discovers candidate runs, validates manifests,
    writes Delta data, and appends status rows into the registry.
    """
    # Step 1: Initialize logging and parse CLI inputs.
    # This fixes the runtime configuration for tables, groups, and run date.
    # The expected outcome is a fully populated args object before Spark starts.
    setup_logging()
    logger = logging.getLogger(APP_NAME)

    args = _parse_args()
    run_date_str = args.run_date or date.today().isoformat()
    run_date = date.fromisoformat(run_date_str)

    # Step 2: Normalize group config and shuffle settings.
    # This decides how many groups run and which shuffle override each group gets.
    # The expected outcome is a structured list of groups ready for execution.
    default_shuffle_partitions = (
        args.shuffle_partitions if args.shuffle_partitions > 0 else None
    )
    groups_config = _parse_groups_config(
        args.groups_config, default_shuffle_partitions
    )
    default_tables = None
    if groups_config:
        groups = groups_config
    else:
        default_tables = _parse_table_list(args.tables)
        if not default_tables:
            raise ValueError("No tables provided. Use --tables or --groups-config.")
        table_config = json.loads(args.table_config) if args.table_config else {}
        groups = [
            {
                "group": "default",
                "mode": args.mode,
                "partition_key": args.partition_key,
                "event_date_column": args.event_date_column,
                "lookback_days": args.lookback_days,
                "tables": default_tables,
                "table_config": table_config,
                "shuffle_partitions": default_shuffle_partitions,
            }
        ]

    minio_endpoint = get_env(
        "MINIO_S3_ENDPOINT", "http://minio.ampere.svc.cluster.local:9000"
    )
    minio_access_key = get_env("MINIO_ACCESS_KEY")
    minio_secret_key = get_env("MINIO_SECRET_KEY")
    if not minio_access_key or not minio_secret_key:
        raise ValueError("Missing MINIO_ACCESS_KEY/MINIO_SECRET_KEY for MinIO.")

    if groups_config:
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

    # Step 4: Load the registry table to track applied batches.
    # This drives idempotency and prevents duplicate loads per partition.
    # The expected outcome is a Delta-backed DataFrame or None when missing.
    # Registry is the source of truth for applied landing batches.
    registry_path = bronze_registry_path(args.bronze_bucket, args.bronze_prefix)
    schema_path = get_env(
        "BRONZE_REGISTRY_SCHEMA_PATH",
        str(Path(__file__).with_name("bronze_apply_registry_schema.json")),
    )
    registry_schema = _load_registry_schema(schema_path)
    if not DeltaTable.isDeltaTable(spark, registry_path):
        raise ValueError("Registry table is missing; run bronze_registry_init first.")
    registry_df = load_registry_table(spark, registry_path)

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
            spark, group_name, group.get("shuffle_partitions")
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
            table_meta = table_config.get(table, {})
            merge_keys = table_meta.get("merge_keys", [])
            raw_base = table_base_path(
                args.raw_bucket, args.raw_prefix, args.schema, table
            )
            bronze_path = table_base_path(
                args.bronze_bucket, args.bronze_prefix, args.schema, table
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
                lookback_days,
                has_registry_rows,
            )

            # Step 7: Discover candidate runs and validate manifests.
            # This filters to batches with _SUCCESS and required metadata.
            # The expected outcome is an ordered apply queue of valid batches.
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
                    _append_registry_row(
                        spark,
                        registry_path,
                        registry_schema,
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
                        },
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
                continue

            def _apply_sort_key(batch: dict) -> tuple:
                """Sort by partition, ingest timestamp, then run id.

                Args:
                    batch: Candidate batch dict, e.g. {"partition_value": "2026-01-24"}.
                """
                ingest_dt = parse_optional_datetime(
                    batch.get("ingest_ts_utc") or ""
                )
                if ingest_dt and ingest_dt.tzinfo is None:
                    ingest_dt = ingest_dt.replace(tzinfo=timezone.utc)
                ingest_key = ingest_dt or datetime(1, 1, 1, tzinfo=timezone.utc)
                return (batch["partition_value"], ingest_key, batch["run_id"])

            # Step 8: Apply batches to bronze and record outcomes in the registry.
            # This writes Delta data and appends apply status for traceability.
            # The expected outcome is applied or skipped registry rows for each batch.
            for batch in sorted(
                apply_queue,
                key=_apply_sort_key,
            ):
                manifest = batch["manifest"]
                batch_apply_ts = datetime.now(timezone.utc).isoformat()

                partition_kind = batch.get("partition_kind")
                partition_value = batch.get("partition_value")
                ok, reason = manifest_ok(manifest)
                if not ok:
                    logger.warning(
                        "Manifest validation failed for %s run_id=%s %s=%s reason=%s",
                        table,
                        manifest.get("run_id", batch["run_id"]),
                        partition_kind,
                        partition_value,
                        reason,
                    )
                    _append_registry_row(
                        spark,
                        registry_path,
                        registry_schema,
                        {
                            "source_system": manifest.get("source_system", args.source_system),
                            "source_schema": manifest.get("source_schema", args.schema),
                            "source_table": manifest.get("source_table", table),
                            "run_id": manifest.get("run_id", batch["run_id"]),
                            "manifest_path": batch["manifest_path"],
                            "batch_type": manifest.get("batch_type"),
                            "partition_kind": batch["partition_kind"],
                            "partition_value": batch["partition_value"],
                            "ingest_ts_utc": manifest.get("ingest_ts_utc"),
                            "schema_hash": manifest.get("schema_hash"),
                            "contract_version": manifest.get("contract_version"),
                            "apply_ts_utc": batch_apply_ts,
                            "status": "failed",
                            "details": reason,
                            "watermark_from": None,
                            "watermark_to": None,
                            "lookback_days": manifest.get("lookback_days"),
                            "window_from": None,
                            "window_to": None,
                            "row_count": manifest.get("row_count"),
                            "file_count": manifest.get("file_count"),
                        },
                    )
                    continue

                if manifest.get("row_count", 0) == 0 or manifest.get("file_count", 0) == 0:
                    logger.info(
                        "Skipping empty batch for %s run_id=%s %s=%s",
                        table,
                        manifest.get("run_id", batch["run_id"]),
                        partition_kind,
                        partition_value,
                    )
                    watermark_from = None
                    watermark_to = None
                    if manifest.get("watermark"):
                        watermark_from = manifest["watermark"].get("from")
                        watermark_to = manifest["watermark"].get("to")
                    _append_registry_row(
                        spark,
                        registry_path,
                        registry_schema,
                        {
                            "source_system": manifest.get("source_system", args.source_system),
                            "source_schema": manifest.get("source_schema", args.schema),
                            "source_table": manifest.get("source_table", table),
                            "run_id": manifest.get("run_id", batch["run_id"]),
                            "manifest_path": batch["manifest_path"],
                            "batch_type": manifest.get("batch_type"),
                            "partition_kind": batch["partition_kind"],
                            "partition_value": batch["partition_value"],
                            "ingest_ts_utc": manifest.get("ingest_ts_utc"),
                            "schema_hash": manifest.get("schema_hash"),
                            "contract_version": manifest.get("contract_version"),
                            "apply_ts_utc": batch_apply_ts,
                            "status": "skipped",
                            "details": "empty batch",
                            "watermark_from": watermark_from,
                            "watermark_to": watermark_to,
                            "lookback_days": manifest.get("lookback_days"),
                            "window_from": (manifest.get("window") or {}).get("from"),
                            "window_to": (manifest.get("window") or {}).get("to"),
                            "row_count": manifest.get("row_count"),
                            "file_count": manifest.get("file_count"),
                        },
                    )
                    continue
    
                if expected_schema_hash and manifest.get("schema_hash") != expected_schema_hash:
                    logger.warning(
                        "Schema hash mismatch for %s run_id=%s expected=%s actual=%s",
                        table,
                        manifest.get("run_id"),
                        expected_schema_hash,
                        manifest.get("schema_hash"),
                    )
                    _append_registry_row(
                        spark,
                        registry_path,
                        registry_schema,
                        {
                            "source_system": manifest.get("source_system"),
                            "source_schema": manifest.get("source_schema"),
                            "source_table": manifest.get("source_table"),
                            "run_id": manifest.get("run_id"),
                            "manifest_path": batch["manifest_path"],
                            "batch_type": manifest.get("batch_type"),
                            "partition_kind": batch["partition_kind"],
                            "partition_value": batch["partition_value"],
                            "ingest_ts_utc": manifest.get("ingest_ts_utc"),
                            "schema_hash": manifest.get("schema_hash"),
                            "contract_version": manifest.get("contract_version"),
                            "apply_ts_utc": batch_apply_ts,
                            "status": "skipped",
                            "details": "schema_hash mismatch",
                            "watermark_from": None,
                            "watermark_to": None,
                            "lookback_days": manifest.get("lookback_days"),
                            "window_from": None,
                            "window_to": None,
                            "row_count": manifest.get("row_count"),
                            "file_count": manifest.get("file_count"),
                        },
                    )
                    continue
    
                if expected_contract_version and manifest.get("contract_version") != expected_contract_version:
                    logger.warning(
                        "Contract version mismatch for %s run_id=%s expected=%s actual=%s",
                        table,
                        manifest.get("run_id"),
                        expected_contract_version,
                        manifest.get("contract_version"),
                    )
                    _append_registry_row(
                        spark,
                        registry_path,
                        registry_schema,
                        {
                            "source_system": manifest.get("source_system"),
                            "source_schema": manifest.get("source_schema"),
                            "source_table": manifest.get("source_table"),
                            "run_id": manifest.get("run_id"),
                            "manifest_path": batch["manifest_path"],
                            "batch_type": manifest.get("batch_type"),
                            "partition_kind": batch["partition_kind"],
                            "partition_value": batch["partition_value"],
                            "ingest_ts_utc": manifest.get("ingest_ts_utc"),
                            "schema_hash": manifest.get("schema_hash"),
                            "contract_version": manifest.get("contract_version"),
                            "apply_ts_utc": batch_apply_ts,
                            "status": "skipped",
                            "details": "contract_version mismatch",
                            "watermark_from": None,
                            "watermark_to": None,
                            "lookback_days": manifest.get("lookback_days"),
                            "window_from": None,
                            "window_to": None,
                            "row_count": manifest.get("row_count"),
                            "file_count": manifest.get("file_count"),
                        },
                    )
                    continue
    
                if not partition_kind or not partition_value:
                    logger.warning(
                        "Missing partition info for %s run_id=%s",
                        table,
                        manifest.get("run_id"),
                    )
                    _append_registry_row(
                        spark,
                        registry_path,
                        registry_schema,
                        {
                            "source_system": manifest.get("source_system"),
                            "source_schema": manifest.get("source_schema"),
                            "source_table": manifest.get("source_table"),
                            "run_id": manifest.get("run_id"),
                            "manifest_path": batch["manifest_path"],
                            "batch_type": manifest.get("batch_type"),
                            "partition_kind": batch["partition_kind"],
                            "partition_value": batch["partition_value"],
                            "ingest_ts_utc": manifest.get("ingest_ts_utc"),
                            "schema_hash": manifest.get("schema_hash"),
                            "contract_version": manifest.get("contract_version"),
                            "apply_ts_utc": batch_apply_ts,
                            "status": "failed",
                            "details": "missing partition info",
                            "watermark_from": None,
                            "watermark_to": None,
                            "lookback_days": manifest.get("lookback_days"),
                            "window_from": None,
                            "window_to": None,
                            "row_count": manifest.get("row_count"),
                            "file_count": manifest.get("file_count"),
                        },
                    )
                    continue
    
                file_paths = [f["path"] for f in manifest.get("files", []) if f.get("path")]
                if not file_paths:
                    logger.warning(
                        "No file paths in manifest for %s run_id=%s",
                        table,
                        manifest.get("run_id"),
                    )
                    _append_registry_row(
                        spark,
                        registry_path,
                        registry_schema,
                        {
                            "source_system": manifest.get("source_system"),
                            "source_schema": manifest.get("source_schema"),
                            "source_table": manifest.get("source_table"),
                            "run_id": manifest.get("run_id"),
                            "manifest_path": batch["manifest_path"],
                            "batch_type": manifest.get("batch_type"),
                            "partition_kind": partition_kind,
                            "partition_value": partition_value,
                            "ingest_ts_utc": manifest.get("ingest_ts_utc"),
                            "schema_hash": manifest.get("schema_hash"),
                            "contract_version": manifest.get("contract_version"),
                            "apply_ts_utc": batch_apply_ts,
                            "status": "failed",
                            "details": "no files in manifest",
                            "watermark_from": None,
                            "watermark_to": None,
                            "lookback_days": manifest.get("lookback_days"),
                            "window_from": None,
                            "window_to": None,
                            "row_count": manifest.get("row_count"),
                            "file_count": manifest.get("file_count"),
                        },
                    )
                    continue
    
                try:
                    df = spark.read.parquet(*file_paths)
                    df = df.withColumn("_bronze_last_run_id", F.lit(manifest.get("run_id")))
                    df = df.withColumn("_bronze_last_apply_ts", F.lit(batch_apply_ts))
                    df = df.withColumn(
                        "_bronze_last_manifest_path", F.lit(batch["manifest_path"])
                    )
    
                    if partition_kind == "snapshot_date":
                        df = df.withColumn("snapshot_date", F.lit(partition_value))
                        if DeltaTable.isDeltaTable(spark, bronze_path):
                            (
                                df.write.format("delta")
                                .mode("overwrite")
                                .option(
                                    "replaceWhere",
                                    f"snapshot_date = '{partition_value}'",
                                )
                                .save(bronze_path)
                            )
                        else:
                            (
                                df.write.format("delta")
                                .mode("overwrite")
                                .partitionBy("snapshot_date")
                                .save(bronze_path)
                            )
                    elif partition_kind == "extract_date":
                        if not merge_keys:
                            raise ValueError(
                                f"merge_keys are required for mutable dim {table}."
                            )
                        _merge_to_delta(spark, df, bronze_path, merge_keys)
                    else:
                        if merge_keys:
                            df = df.dropDuplicates(merge_keys)
                            _merge_to_delta(spark, df, bronze_path, merge_keys)
                        else:
                            df.write.format("delta").mode("append").save(bronze_path)
    
                    watermark_from = None
                    watermark_to = None
                    if manifest.get("watermark"):
                        watermark_from = manifest["watermark"].get("from")
                        watermark_to = manifest["watermark"].get("to")
    
                    _append_registry_row(
                        spark,
                        registry_path,
                        registry_schema,
                        {
                            "source_system": manifest.get("source_system"),
                            "source_schema": manifest.get("source_schema"),
                            "source_table": manifest.get("source_table"),
                            "run_id": manifest.get("run_id"),
                            "manifest_path": batch["manifest_path"],
                            "batch_type": manifest.get("batch_type"),
                            "partition_kind": partition_kind,
                            "partition_value": partition_value,
                            "ingest_ts_utc": manifest.get("ingest_ts_utc"),
                            "schema_hash": manifest.get("schema_hash"),
                            "contract_version": manifest.get("contract_version"),
                            "apply_ts_utc": batch_apply_ts,
                            "status": "applied",
                            "details": "ok",
                            "watermark_from": watermark_from,
                            "watermark_to": watermark_to,
                            "lookback_days": manifest.get("lookback_days"),
                            "window_from": (manifest.get("window") or {}).get("from"),
                            "window_to": (manifest.get("window") or {}).get("to"),
                            "row_count": manifest.get("row_count"),
                            "file_count": manifest.get("file_count"),
                        },
                    )
                    logger.info(
                        "Applied batch run_id=%s %s=%s for %s manifest=%s",
                        manifest.get("run_id"),
                        partition_kind,
                        partition_value,
                        table,
                        batch["manifest_path"],
                    )
                except Exception as exc:  # noqa: BLE001
                    _append_registry_row(
                        spark,
                        registry_path,
                        registry_schema,
                        {
                            "source_system": manifest.get("source_system"),
                            "source_schema": manifest.get("source_schema"),
                            "source_table": manifest.get("source_table"),
                            "run_id": manifest.get("run_id"),
                            "manifest_path": batch["manifest_path"],
                            "batch_type": manifest.get("batch_type"),
                            "partition_kind": partition_kind,
                            "partition_value": partition_value,
                            "ingest_ts_utc": manifest.get("ingest_ts_utc"),
                            "schema_hash": manifest.get("schema_hash"),
                            "contract_version": manifest.get("contract_version"),
                            "apply_ts_utc": batch_apply_ts,
                            "status": "failed",
                            "details": f"bronze apply failed: {exc}",
                            "watermark_from": None,
                            "watermark_to": None,
                            "lookback_days": manifest.get("lookback_days"),
                            "window_from": (manifest.get("window") or {}).get("from"),
                            "window_to": (manifest.get("window") or {}).get("to"),
                            "row_count": manifest.get("row_count"),
                            "file_count": manifest.get("file_count"),
                        },
                    )
                    logger.exception(
                        "Failed applying batch run_id=%s %s=%s for %s",
                        manifest.get("run_id"),
                        partition_kind,
                        partition_value,
                        table,
                    )

            # Release cached data between tables to limit driver memory growth.
            spark.catalog.clearCache()
    
    spark.stop()


if __name__ == "__main__":
    main()
