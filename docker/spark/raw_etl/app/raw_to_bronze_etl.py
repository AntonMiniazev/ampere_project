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
    parse_date,
    parse_optional_datetime,
    read_json,
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
    parser.add_argument("--app-name", default=APP_NAME, help="Spark app name")
    parser.add_argument("--image", default="", help="Container image reference")
    return parser.parse_args()


def _parse_table_list(raw: str) -> list[str]:
    return [t.strip() for t in raw.split(",") if t.strip()]


def _parse_groups_config(raw: str) -> list[dict]:
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
        group = {
            "group": item.get("group", "group"),
            "mode": item.get("mode", "snapshot"),
            "partition_key": item.get("partition_key", "snapshot_date"),
            "event_date_column": item.get("event_date_column", ""),
            "lookback_days": int(item.get("lookback_days", 0) or 0),
            "tables": tables,
            "table_config": item.get("table_config", {}) or {},
        }
        groups.append(group)
    return groups


def _load_registry_schema(schema_path: str) -> StructType:
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
) -> Optional[date]:
    registry_date = None
    if registry_df is not None:
        row = registry_df.select(F.max("partition_value").alias("pv")).first()
        if row and row.pv:
            registry_date = date.fromisoformat(row.pv)

    candidates = []
    if registry_date:
        candidates.append(registry_date)
    if state_last_ingest:
        candidates.append(state_last_ingest.date())

    if not candidates:
        if partition_key == "event_date":
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


def _manifest_ok(manifest: dict) -> tuple[bool, str]:
    required = [
        "manifest_version",
        "source_system",
        "source_schema",
        "source_table",
        "contract_name",
        "contract_version",
        "run_id",
        "ingest_ts_utc",
        "batch_type",
        "storage_format",
        "schema_hash",
        "checksum",
        "file_count",
        "row_count",
        "files",
        "checks",
        "min_max",
        "null_counts",
        "producer",
        "source_extract",
    ]
    for key in required:
        if key not in manifest:
            return False, f"missing {key}"
        if manifest[key] is None:
            return False, f"null {key}"

    if manifest.get("batch_type") not in {"snapshot", "incremental"}:
        return False, "invalid batch_type"

    files = manifest.get("files", [])
    if not isinstance(files, list) or not files:
        return False, "missing files"
    for entry in files:
        for file_key in ("path", "size_bytes", "row_count", "checksum"):
            if file_key not in entry:
                return False, f"missing file.{file_key}"

    if isinstance(manifest.get("file_count"), int) and len(files) != manifest["file_count"]:
        return False, "file_count mismatch"

    checks = manifest.get("checks", [])
    if not isinstance(checks, list):
        return False, "checks not a list"
    for check in checks:
        if check.get("status") == "fail":
            return False, "manifest checks failed"

    partition_kind, _ = _partition_info(manifest)
    if not partition_kind:
        return False, "missing partition info"
    if partition_kind == "snapshot_date" and "snapshot_date" not in manifest:
        return False, "missing snapshot_date"
    if partition_kind == "extract_date":
        watermark = manifest.get("watermark")
        if not isinstance(watermark, dict):
            return False, "missing watermark"
        for key in ("column", "from", "to"):
            if not watermark.get(key):
                return False, f"missing watermark.{key}"
    if partition_kind == "event_date":
        lookback_days = manifest.get("lookback_days")
        if lookback_days:
            window = manifest.get("window") or {}
            if not window.get("from") or not window.get("to"):
                return False, "missing window"
            if not manifest.get("event_dates_covered"):
                return False, "missing event_dates_covered"

    return True, "ok"


def _partition_info(manifest: dict) -> tuple[str, str]:
    if "snapshot_date" in manifest:
        return "snapshot_date", manifest["snapshot_date"]
    if "extract_date" in manifest:
        return "extract_date", manifest["extract_date"]
    if "event_date" in manifest:
        return "event_date", manifest["event_date"]
    return "", ""


def _load_registry_table(spark: SparkSession, path_str: str):
    if DeltaTable.isDeltaTable(spark, path_str):
        return spark.read.format("delta").load(path_str)
    return None


def _merge_to_delta(
    spark: SparkSession,
    df,
    target_path: str,
    merge_keys: list[str],
) -> None:
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


def main() -> None:
    setup_logging()
    logger = logging.getLogger(APP_NAME)

    args = _parse_args()
    run_date_str = args.run_date or date.today().isoformat()
    run_date = date.fromisoformat(run_date_str)

    groups_config = _parse_groups_config(args.groups_config)
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
            }
        ]

    minio_endpoint = get_env(
        "MINIO_S3_ENDPOINT", "http://minio.ampere.svc.cluster.local:9000"
    )
    minio_access_key = get_env("MINIO_ACCESS_KEY")
    minio_secret_key = get_env("MINIO_SECRET_KEY")
    if not minio_access_key or not minio_secret_key:
        raise ValueError("Missing MINIO_ACCESS_KEY/MINIO_SECRET_KEY for MinIO.")
    logger.info(
        "Config raw=%s/%s bronze=%s/%s schema=%s source_system=%s endpoint=%s",
        args.raw_bucket,
        args.raw_prefix,
        args.bronze_bucket,
        args.bronze_prefix,
        args.schema,
        args.source_system,
        minio_endpoint,
    )

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

    spark = (
        SparkSession.builder.appName(args.app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    configure_s3(spark, minio_endpoint, minio_access_key, minio_secret_key)

    registry_path = bronze_registry_path(args.bronze_bucket, args.bronze_prefix)
    schema_path = get_env(
        "BRONZE_REGISTRY_SCHEMA_PATH",
        str(Path(__file__).with_name("bronze_apply_registry_schema.json")),
    )
    registry_schema = _load_registry_schema(schema_path)
    logger.info("Using registry table %s", registry_path)
    if not DeltaTable.isDeltaTable(spark, registry_path):
        raise ValueError("Registry table is missing; run bronze_registry_init first.")
    registry_df = _load_registry_table(spark, registry_path)

    for group in groups:
        group_name = group.get("group", "group")
        group_tables = group.get("tables", [])
        if not group_tables:
            logger.info("No tables configured for group %s", group_name)
            continue
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
            table_meta = table_config.get(table, {})
            merge_keys = table_meta.get("merge_keys", [])
            raw_base = table_base_path(
                args.raw_bucket, args.raw_prefix, args.schema, table
            )
            bronze_path = table_base_path(
                args.bronze_bucket, args.bronze_prefix, args.schema, table
            )
            logger.info(
                "Table %s raw_base=%s bronze_path=%s merge_keys=%s",
                table,
                raw_base,
                bronze_path,
                ",".join(merge_keys) if merge_keys else "none",
            )
    
            table_registry = None
            applied_run_ids = set()
            expected_schema_hash = None
            expected_contract_version = None
            if registry_df is not None:
                table_registry = registry_df.filter(
                    (F.col("source_system") == args.source_system)
                    & (F.col("source_schema") == args.schema)
                    & (F.col("source_table") == table)
                )
                applied_run_ids = {
                    row.run_id
                    for row in table_registry.filter(
                        F.col("status").isin("applied", "skipped")
                    )
                    .select("run_id")
                    .collect()
                }
                latest_row = (
                    table_registry.orderBy(F.col("apply_ts_utc").desc()).limit(1).collect()
                )
                if latest_row:
                    expected_schema_hash = latest_row[0].schema_hash
                    expected_contract_version = latest_row[0].contract_version
    
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
            )
            logger.info(
                "Search start for %s: %s (state_last_ingest=%s)",
                table,
                search_start,
                state_last_ingest,
            )

            candidates = _candidate_runs(
                spark, raw_base, partition_key, search_start
            )
            if not candidates:
                logger.info("No candidate batches for %s", table)
                continue
            candidate_ids = [candidate["run_id"] for candidate in candidates]
            logger.info(
                "Found %s candidate runs for %s (%s)",
                len(candidates),
                table,
                ",".join(candidate_ids[:5]),
            )
    
            apply_queue = []
            for candidate in candidates:
                if candidate["run_id"] in applied_run_ids:
                    continue
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
                apply_queue.append(candidate)
            if not apply_queue:
                logger.info("No new batches to apply for %s", table)
                continue
            logger.info("Applying %s new batches for %s", len(apply_queue), table)
    
            def _apply_sort_key(batch: dict) -> tuple:
                ingest_dt = parse_optional_datetime(
                    batch.get("ingest_ts_utc") or ""
                )
                if ingest_dt and ingest_dt.tzinfo is None:
                    ingest_dt = ingest_dt.replace(tzinfo=timezone.utc)
                ingest_key = ingest_dt or datetime(1, 1, 1, tzinfo=timezone.utc)
                return (batch["partition_value"], ingest_key, batch["run_id"])

            for batch in sorted(
                apply_queue,
                key=_apply_sort_key,
            ):
                manifest = batch["manifest"]
                batch_apply_ts = datetime.now(timezone.utc).isoformat()

                ok, reason = _manifest_ok(manifest)
                if not ok:
                    logger.warning(
                        "Manifest validation failed for %s run_id=%s reason=%s",
                        table,
                        manifest.get("run_id", batch["run_id"]),
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
    
                partition_kind, partition_value = _partition_info(manifest)
                if not partition_kind:
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
                        "Applied batch %s for %s", manifest.get("run_id"), table
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
                        "Failed applying batch %s for %s",
                        manifest.get("run_id"),
                        table,
                    )
    
    spark.stop()


if __name__ == "__main__":
    main()
