import argparse
import json
import logging
import os
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

try:
    from delta.tables import DeltaTable
except ImportError as exc:
    raise ImportError(
        "delta-spark is required for bronze writes. Ensure Delta jars are on the classpath."
    ) from exc

APP_NAME = "raw-to-bronze-etl"


def setup_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=level,
        format=("%(asctime)s | %(levelname)s | %(name)s | %(message)s"),
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout),
        ],
        force=True,
    )


def _get_env(name: str, default: Optional[str] = None) -> Optional[str]:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return value


def _parse_date(value: str) -> str:
    datetime.strptime(value, "%Y-%m-%d")
    return value


def _parse_optional_datetime(value: str) -> Optional[datetime]:
    if not value:
        return None
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        datetime.strptime(normalized, "%Y-%m-%d")
        return datetime.combine(date.fromisoformat(normalized), datetime.min.time())


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
    parser.add_argument("--schema", default="source", help="Source schema name")
    parser.add_argument("--run-date", type=_parse_date, help="YYYY-MM-DD")
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


def _configure_s3(
    spark: SparkSession,
    endpoint: str,
    access_key: str,
    secret_key: str,
) -> None:
    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.endpoint", endpoint)
    hadoop_conf.set("fs.s3a.access.key", access_key)
    hadoop_conf.set("fs.s3a.secret.key", secret_key)
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set(
        "fs.s3a.connection.ssl.enabled",
        "true" if endpoint.startswith("https://") else "false",
    )
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set(
        "fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    )


def _parse_table_list(raw: str) -> list[str]:
    return [t.strip() for t in raw.split(",") if t.strip()]


def _raw_table_base(
    bucket: str, prefix: str, schema: str, table: str
) -> str:
    prefix = prefix.strip("/")
    if prefix:
        return f"s3a://{bucket}/{prefix}/{schema}/{table}"
    return f"s3a://{bucket}/{schema}/{table}"


def _bronze_table_path(
    bucket: str, prefix: str, schema: str, table: str
) -> str:
    prefix = prefix.strip("/")
    if prefix:
        return f"s3a://{bucket}/{prefix}/{schema}/{table}"
    return f"s3a://{bucket}/{schema}/{table}"


def _raw_state_path(
    bucket: str, source_system: str, schema: str, table: str
) -> str:
    return (
        f"s3a://{bucket}/{source_system}/{schema}/_state/"
        f"_state_{table}.json"
    )


def _bronze_registry_path(bucket: str, prefix: str) -> str:
    prefix = prefix.strip("/")
    if prefix:
        return f"s3a://{bucket}/{prefix}/ops/bronze_apply_registry"
    return f"s3a://{bucket}/ops/bronze_apply_registry"


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


def _ensure_registry_table(
    spark: SparkSession, path_str: str, schema: StructType
) -> None:
    if DeltaTable.isDeltaTable(spark, path_str):
        return
    empty_df = spark.createDataFrame([], schema=schema)
    empty_df.write.format("delta").mode("overwrite").save(path_str)


def _append_registry_row(
    spark: SparkSession, path_str: str, schema: StructType, row: dict
) -> None:
    df = spark.createDataFrame([row], schema=schema)
    df.write.format("delta").mode("append").save(path_str)


def _list_dirs(spark: SparkSession, path_str: str) -> list[str]:
    jvm = spark._jvm
    path = jvm.org.apache.hadoop.fs.Path(path_str)
    fs = path.getFileSystem(spark._jsc.hadoopConfiguration())
    if not fs.exists(path):
        return []
    statuses = fs.listStatus(path)
    return [status.getPath().getName() for status in statuses if status.isDirectory()]


def _exists(spark: SparkSession, path_str: str) -> bool:
    jvm = spark._jvm
    path = jvm.org.apache.hadoop.fs.Path(path_str)
    fs = path.getFileSystem(spark._jsc.hadoopConfiguration())
    return fs.exists(path)


def _read_json(spark: SparkSession, path_str: str) -> Optional[dict]:
    jvm = spark._jvm
    path = jvm.org.apache.hadoop.fs.Path(path_str)
    fs = path.getFileSystem(spark._jsc.hadoopConfiguration())
    if not fs.exists(path):
        return None
    input_stream = fs.open(path)
    data = bytearray()
    buffer = jvm.java.nio.ByteBuffer.allocate(8192)
    while True:
        read_bytes = input_stream.read(buffer.array())
        if read_bytes <= 0:
            break
        data.extend(buffer.array()[:read_bytes])
    input_stream.close()
    return json.loads(data.decode("utf-8"))


def _list_partitions(
    spark: SparkSession, base_path: str, partition_key: str
) -> list[date]:
    mode_path = (
        f"{base_path}/mode=snapshot"
        if partition_key == "snapshot_date"
        else f"{base_path}/mode=incremental"
    )
    partition_dirs = _list_dirs(spark, mode_path)
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
        run_dirs = _list_dirs(spark, partition_path)
        for run_dir in run_dirs:
            if not run_dir.startswith("run_id="):
                continue
            run_id = run_dir.split("=", 1)[1]
            run_path = f"{partition_path}/{run_dir}"
            success_path = f"{run_path}/_SUCCESS"
            manifest_path = f"{run_path}/_manifest.json"
            if not _exists(spark, success_path):
                continue
            if not _exists(spark, manifest_path):
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
        "source_system",
        "source_schema",
        "source_table",
        "run_id",
        "ingest_ts_utc",
        "batch_type",
        "schema_hash",
        "contract_version",
        "files",
        "checks",
    ]
    for key in required:
        if key not in manifest:
            return False, f"missing {key}"
    for check in manifest.get("checks", []):
        if check.get("status") == "fail":
            return False, "manifest checks failed"
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

    tables = _parse_table_list(args.tables)
    if not tables:
        raise ValueError("No tables provided. Use --tables.")

    table_config = json.loads(args.table_config) if args.table_config else {}

    minio_endpoint = _get_env(
        "MINIO_S3_ENDPOINT", "http://minio.ampere.svc.cluster.local:9000"
    )
    minio_access_key = _get_env("MINIO_ACCESS_KEY")
    minio_secret_key = _get_env("MINIO_SECRET_KEY")
    if not minio_access_key or not minio_secret_key:
        raise ValueError("Missing MINIO_ACCESS_KEY/MINIO_SECRET_KEY for MinIO.")

    logger.info(
        "Starting bronze load for %s (mode=%s, run_date=%s)",
        ",".join(tables),
        args.mode,
        run_date_str,
    )

    spark = (
        SparkSession.builder.appName(args.app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    _configure_s3(spark, minio_endpoint, minio_access_key, minio_secret_key)

    registry_path = _bronze_registry_path(args.bronze_bucket, args.bronze_prefix)
    schema_path = _get_env(
        "BRONZE_REGISTRY_SCHEMA_PATH",
        str(Path(__file__).with_name("bronze_apply_registry_schema.json")),
    )
    registry_schema = _load_registry_schema(schema_path)
    _ensure_registry_table(spark, registry_path, registry_schema)
    registry_df = _load_registry_table(spark, registry_path)

    apply_ts_utc = datetime.now(timezone.utc).isoformat()

    for table in tables:
        table_meta = table_config.get(table, {})
        merge_keys = table_meta.get("merge_keys", [])
        raw_base = _raw_table_base(args.raw_bucket, args.raw_prefix, args.schema, table)
        bronze_path = _bronze_table_path(
            args.bronze_bucket, args.bronze_prefix, args.schema, table
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
                row.run_id for row in table_registry.select("run_id").collect()
            }
            latest_row = (
                table_registry.orderBy(F.col("apply_ts_utc").desc()).limit(1).collect()
            )
            if latest_row:
                expected_schema_hash = latest_row[0].schema_hash
                expected_contract_version = latest_row[0].contract_version

        state_last_ingest = None
        if args.partition_key == "extract_date":
            state_path = _raw_state_path(
                args.raw_bucket, args.source_system, args.schema, table
            )
            state = _read_json(spark, state_path)
            if state and state.get("last_successful_ingest_ts_utc"):
                state_last_ingest = _parse_optional_datetime(
                    state["last_successful_ingest_ts_utc"]
                )

        search_start = _search_start_date(
            args.partition_key,
            table_registry,
            state_last_ingest,
            run_date,
            args.lookback_days,
        )

        candidates = _candidate_runs(
            spark, raw_base, args.partition_key, search_start
        )
        if not candidates:
            logger.info("No candidate batches for %s", table)
            continue

        apply_queue = [
            c for c in candidates if c["run_id"] not in applied_run_ids
        ]
        if not apply_queue:
            logger.info("No new batches to apply for %s", table)
            continue

        for batch in sorted(
            apply_queue,
            key=lambda b: (b["partition_value"], b["run_id"]),
        ):
            manifest = _read_json(spark, batch["manifest_path"])
            if not manifest:
                _append_registry_row(
                    spark,
                    registry_path,
                    registry_schema,
                    {
                        "source_system": args.source_system,
                        "source_schema": args.schema,
                        "source_table": table,
                        "run_id": batch["run_id"],
                        "manifest_path": batch["manifest_path"],
                        "batch_type": None,
                        "partition_kind": batch["partition_kind"],
                        "partition_value": batch["partition_value"],
                        "ingest_ts_utc": None,
                        "schema_hash": None,
                        "contract_version": None,
                        "apply_ts_utc": apply_ts_utc,
                        "status": "failed",
                        "details": "missing manifest",
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

            ok, reason = _manifest_ok(manifest)
            if not ok:
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
                        "apply_ts_utc": apply_ts_utc,
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
                        "apply_ts_utc": apply_ts_utc,
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
                        "apply_ts_utc": apply_ts_utc,
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
                        "apply_ts_utc": apply_ts_utc,
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
                        "apply_ts_utc": apply_ts_utc,
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
                df = df.withColumn("_bronze_last_apply_ts", F.lit(apply_ts_utc))
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
                        "apply_ts_utc": apply_ts_utc,
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
                        "apply_ts_utc": apply_ts_utc,
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

