"""Extract source tables to raw landing (parquet + manifest + success marker)."""

import argparse
import hashlib
import json
import logging
import re
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Optional

from pyspark.sql import SparkSession, functions as F

from etl_utils import (
    configure_s3,
    exists,
    get_env,
    parse_date,
    parse_optional_datetime,
    read_json,
    setup_logging,
    state_path,
    table_base_path,
    write_bytes,
    write_marker,
)

APP_NAME = "source-to-raw-etl"


def _parse_args() -> argparse.Namespace:
    """Parse CLI args for source-to-raw extraction.

    Example CLI inputs:
        --tables "orders,customers"
        --mode incremental
        --partition-key event_date
        --run-date "2026-01-24"
    """
    parser = argparse.ArgumentParser(
        description="Extract source tables to MinIO-backed parquet."
    )
    parser.add_argument("--table", default="", help="Source table name")
    parser.add_argument(
        "--tables",
        default="",
        help="Comma-separated table list for group extraction",
    )
    parser.add_argument(
        "--groups-config",
        default="",
        help="JSON list with group settings for multi-group execution",
    )
    parser.add_argument(
        "--table-config",
        default="",
        help="JSON mapping of table-specific overrides",
    )
    parser.add_argument("--schema", default="source", help="Source schema name")
    parser.add_argument("--run-date", type=parse_date, help="YYYY-MM-DD")
    parser.add_argument(
        "--mode",
        choices=("snapshot", "incremental"),
        default="snapshot",
        help="Extraction mode",
    )
    parser.add_argument(
        "--partition-key",
        choices=("snapshot_date", "extract_date", "event_date"),
        default="snapshot_date",
        help="Partition key for raw landing layout",
    )
    parser.add_argument(
        "--snapshot-partitioned",
        default="true",
        help="Whether snapshot output is partitioned by snapshot_date",
    )
    parser.add_argument(
        "--event-date-column",
        default="",
        help="Date/timestamp column for event/fact extractions",
    )
    parser.add_argument(
        "--watermark-column",
        default="",
        help="Updated-at column for mutable dimensions",
    )
    parser.add_argument(
        "--watermark-from",
        default="",
        help="Lower watermark boundary (ISO-8601 timestamp, exclusive)",
    )
    parser.add_argument(
        "--watermark-to",
        default="",
        help="Upper watermark boundary (ISO-8601 timestamp, inclusive)",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=0,
        help="Lookback window for event extraction (days)",
    )
    parser.add_argument(
        "--shuffle-partitions",
        type=int,
        default=0,
        help="Override spark.sql.shuffle.partitions (0 keeps Spark default)",
    )
    parser.add_argument("--run-id", default="", help="Unique run identifier")
    parser.add_argument(
        "--source-system",
        default="postgres-pre-raw",
        help="Source system identifier",
    )
    parser.add_argument("--bucket", default="ampere-raw", help="MinIO bucket name")
    parser.add_argument(
        "--output-prefix",
        default="postgres-pre-raw",
        help="Output prefix in the bucket",
    )
    parser.add_argument("--app-name", default=APP_NAME, help="Spark app name")
    parser.add_argument("--image", default="", help="Container image reference")
    return parser.parse_args()


def _sanitize_run_id(value: str) -> str:
    """Normalize run_id to a safe folder name.

    Args:
        value: Raw run id, e.g. "manual__2026-01-24T12:00:00+00:00".
    """
    if not value:
        return value
    return re.sub(r"[^a-zA-Z0-9._-]", "_", value)


def _date_range(run_date: date, lookback_days: int) -> tuple[date, date, list[date]]:
    """Return [start, end) window and the list of dates in the window.

    Args:
        run_date: Logical run date, e.g. date(2026, 1, 24).
        lookback_days: Lookback size, e.g. 2.
    """
    if lookback_days < 1:
        lookback_days = 1
    end_date = run_date + timedelta(days=1)
    start_date = run_date - timedelta(days=lookback_days - 1)
    dates = [start_date + timedelta(days=i) for i in range(lookback_days)]
    return start_date, end_date, dates


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
            "watermark_column": item.get("watermark_column", ""),
            "lookback_days": int(item.get("lookback_days", 0) or 0),
            "tables": tables,
            "table_config": item.get("table_config", {}) or {},
            "snapshot_partitioned": item.get("snapshot_partitioned", "true"),
            "shuffle_partitions": shuffle_partitions,
        }
        groups.append(group)
    return groups




def _format_ts(value: datetime) -> str:
    """Format a datetime in ISO-8601 with timezone awareness.

    Args:
        value: Datetime value, e.g. datetime(2026, 1, 24, tzinfo=UTC).
    """
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.isoformat()


def _parse_bool(value: str) -> bool:
    """Parse common truthy strings into a boolean.

    Args:
        value: Boolean-like string, e.g. "true", "1", "yes".
    """
    normalized = value.strip().lower()
    return normalized in {"1", "true", "yes", "y", "on"}


def _format_date_window(start_date: date, end_date: date) -> tuple[str, str]:
    """Build UTC window timestamps for a [start, end) date range.

    Args:
        start_date: Start date, e.g. date(2026, 1, 23).
        end_date: End date, e.g. date(2026, 1, 25).
    """
    start_ts = datetime.combine(start_date, datetime.min.time(), tzinfo=timezone.utc)
    end_ts = datetime.combine(end_date, datetime.min.time(), tzinfo=timezone.utc)
    return _format_ts(start_ts), _format_ts(end_ts)


def _build_where_clause(
    mode: str,
    partition_key: str,
    run_date: date,
    event_date_column: str | None,
    watermark_column: str | None,
    lookback_days: int,
    watermark_from: Optional[datetime],
    watermark_to: Optional[datetime],
) -> Optional[str]:
    """Build a WHERE clause for incremental pulls based on partition strategy.

    Args:
        mode: Extract mode, e.g. "snapshot" or "incremental".
        partition_key: Partition type, e.g. "extract_date" or "event_date".
        run_date: Logical run date, e.g. date(2026, 1, 24).
        event_date_column: Event column, e.g. "order_date".
        watermark_column: Watermark column, e.g. "updated_at".
        lookback_days: Lookback size, e.g. 2.
        watermark_from: Lower watermark, e.g. datetime(2026, 1, 23, tzinfo=UTC).
        watermark_to: Upper watermark, e.g. datetime(2026, 1, 24, tzinfo=UTC).
    """
    if mode != "incremental":
        return None

    if partition_key == "extract_date" and watermark_column:
        upper = watermark_to
        lower = watermark_from
        if upper is None:
            upper = datetime.combine(run_date, datetime.min.time(), tzinfo=timezone.utc)
        if lower is None:
            lower = upper - timedelta(days=1)
        return (
            f"{watermark_column} > TIMESTAMPTZ '{_format_ts(lower)}' "
            f"AND {watermark_column} <= TIMESTAMPTZ '{_format_ts(upper)}'"
        )

    if partition_key == "event_date" and event_date_column:
        start_date, end_date, _ = _date_range(run_date, lookback_days)
        return (
            f"{event_date_column} >= DATE '{start_date.isoformat()}' "
            f"AND {event_date_column} < DATE '{end_date.isoformat()}'"
        )

    return None


def _build_dbtable(schema: str, table: str, where_clause: Optional[str]) -> str:
    """Build a JDBC dbtable string, optionally wrapping a filtered subquery.

    Args:
        schema: Source schema, e.g. "source".
        table: Table name, e.g. "orders".
        where_clause: Optional SQL filter, e.g. "updated_at > TIMESTAMPTZ ...".
    """
    source_table = f'"{schema}"."{table}"'
    if where_clause:
        return f"(SELECT * FROM {source_table} WHERE {where_clause}) AS src"
    return source_table


def _hash_schema(schema_json: str) -> str:
    """Hash a Spark schema JSON string.

    Args:
        schema_json: Spark schema JSON, e.g. df.schema.json().
    """
    return hashlib.md5(schema_json.encode("utf-8")).hexdigest()


def _compute_batch_checksum(files: list[dict]) -> str:
    """Compute a stable checksum across file entries.

    Args:
        files: File list, e.g. [{"path": "...", "checksum": "..."}].
    """
    ordered = sorted(files, key=lambda entry: entry.get("path", ""))
    payload = "|".join(
        f"{entry.get('path','')}:{entry.get('checksum','')}" for entry in ordered
    )
    return hashlib.md5(payload.encode("utf-8")).hexdigest()


def _collect_file_details(
    spark: SparkSession, output_path: str
) -> list[dict]:
    """Collect file-level metadata for part files in an output folder.

    Args:
        spark: Active SparkSession, e.g. SparkSession.builder.getOrCreate().
        output_path: Output folder, e.g. "s3a://ampere-raw/.../run_id=...".
    """
    jvm = spark._jvm
    path = jvm.org.apache.hadoop.fs.Path(output_path)
    fs = path.getFileSystem(spark._jsc.hadoopConfiguration())
    file_statuses = fs.listStatus(path)
    files = []
    for status in file_statuses:
        if not status.isFile():
            continue
        filename = status.getPath().getName()
        if not filename.startswith("part-"):
            continue
        file_path = status.getPath().toString()
        size_bytes = status.getLen()
        modified_ms = status.getModificationTime()
        row_count = spark.read.parquet(file_path).count()
        checksum = f"len:{size_bytes}-mtime:{modified_ms}"
        files.append(
            {
                "path": file_path,
                "size_bytes": size_bytes,
                "row_count": row_count,
                "checksum": checksum,
            }
        )
    return files


def _collect_stats(df, base_columns: list[str]) -> tuple[dict, dict]:
    """Compute null counts and min/max stats for each column.

    Args:
        df: Spark DataFrame, e.g. spark.read.parquet("s3a://...").
        base_columns: Column names, e.g. ["id", "created_at"].
    """
    null_exprs = [
        F.sum(F.when(F.col(col_name).isNull(), 1).otherwise(0)).alias(
            f"{col_name}__nulls"
        )
        for col_name in base_columns
    ]
    min_exprs = [F.min(col_name).alias(f"{col_name}__min") for col_name in base_columns]
    max_exprs = [F.max(col_name).alias(f"{col_name}__max") for col_name in base_columns]
    stats_row = df.agg(*null_exprs, *min_exprs, *max_exprs).first()

    null_counts = {}
    min_max = {}
    for col_name in base_columns:
        null_value = getattr(stats_row, f"{col_name}__nulls")
        null_counts[col_name] = int(null_value) if null_value is not None else 0
        min_val = getattr(stats_row, f"{col_name}__min")
        max_val = getattr(stats_row, f"{col_name}__max")
        min_max[col_name] = {
            "min": None if min_val is None else str(min_val),
            "max": None if max_val is None else str(max_val),
        }
    return null_counts, min_max


def _write_batch(
    spark: SparkSession,
    df,
    output_path: str,
    manifest_context: dict,
    base_columns: list[str],
    logger: logging.Logger,
    allow_empty: bool = True,
) -> dict:
    """Write parquet output plus manifest and _SUCCESS for a single batch.

    Args:
        spark: Active SparkSession, e.g. SparkSession.builder.getOrCreate().
        df: Source DataFrame, e.g. spark.read.jdbc(...).
        output_path: Output folder, e.g. "s3a://ampere-raw/.../run_id=...".
        manifest_context: Base manifest metadata, e.g. {"source_table": "orders"}.
        base_columns: Base column list, e.g. ["id", "created_at"].
        logger: Logger instance, e.g. logging.getLogger("source-to-raw-etl").
        allow_empty: Whether to emit empty batches, e.g. False for event partitions.
    """
    row_count = df.count()
    if row_count == 0 and not allow_empty:
        logger.info("Skipping empty batch at %s", output_path)
        return {
            "success": False,
            "manifest_path": output_path.rstrip("/") + "/_manifest.json",
            "row_count": row_count,
            "file_count": 0,
            "empty": True,
        }
    df.write.mode("overwrite").parquet(output_path)

    files = _collect_file_details(spark, output_path)
    file_count = len(files)
    batch_checksum = _compute_batch_checksum(files)

    null_counts, min_max = _collect_stats(df, base_columns)

    manifest = {
        **manifest_context,
        "file_count": file_count,
        "row_count": row_count,
        "checksum": batch_checksum,
        "files": files,
        "checks": [
            {
                "name": "row_count_non_negative",
                "status": "warn" if row_count == 0 else "pass",
                "details": f"row_count={row_count}",
            }
        ],
        "min_max": min_max,
        "null_counts": null_counts,
    }

    manifest_path = output_path.rstrip("/") + "/_manifest.json"
    manifest_payload = json.dumps(manifest, indent=2, sort_keys=True).encode(
        "utf-8"
    )
    write_bytes(spark, manifest_path, manifest_payload)
    if not exists(spark, manifest_path):
        logger.error("Manifest write failed: %s", manifest_path)
        return {
            "success": False,
            "manifest_path": manifest_path,
            "row_count": row_count,
            "file_count": file_count,
        }
    if read_json(spark, manifest_path, logger) is None:
        logger.error("Manifest invalid after write: %s", manifest_path)
        return {
            "success": False,
            "manifest_path": manifest_path,
            "row_count": row_count,
            "file_count": file_count,
        }

    if any(check["status"] == "fail" for check in manifest["checks"]):
        logger.warning("Skipping _SUCCESS due to failed checks.")
        return {
            "success": False,
            "manifest_path": manifest_path,
            "row_count": row_count,
            "file_count": file_count,
        }
    write_marker(spark, output_path, "_SUCCESS", b"")
    return {
        "success": True,
        "manifest_path": manifest_path,
        "row_count": row_count,
        "file_count": file_count,
    }


def _apply_group_shuffle(
    spark: SparkSession,
    group_name: str,
    shuffle_partitions: Optional[int],
) -> None:
    """Override spark.sql.shuffle.partitions for the current group when set.

    Args:
        spark: Active SparkSession, e.g. SparkSession.builder.getOrCreate().
        group_name: Group label, e.g. "snapshots" or "events".
        shuffle_partitions: Partition count, e.g. 1 or None to keep Spark default.
    """
    if not shuffle_partitions:
        return
    spark.conf.set("spark.sql.shuffle.partitions", str(shuffle_partitions))
    logging.getLogger(APP_NAME).info(
        "Set spark.sql.shuffle.partitions=%s for group=%s",
        shuffle_partitions,
        group_name,
    )


def _has_existing_incremental(
    spark: SparkSession, output_base: str
) -> bool:
    """Return True when incremental data already exists for a table.

    Args:
        spark: Active SparkSession, e.g. SparkSession.builder.getOrCreate().
        output_base: Table base path, e.g. "s3a://ampere-raw/postgres-pre-raw/source/orders".
    """
    return exists(spark, f"{output_base}/mode=incremental")


def _collect_event_dates(df, event_col: str) -> list[str]:
    """Collect distinct event dates from the DataFrame.

    Args:
        df: Spark DataFrame, e.g. spark.read.jdbc(...).
        event_col: Event column name, e.g. "order_date".
    """
    rows = (
        df.select(F.to_date(F.col(event_col)).alias("event_date"))
        .where(F.col("event_date").isNotNull())
        .distinct()
        .collect()
    )
    return sorted({row.event_date.isoformat() for row in rows if row.event_date})


def main() -> None:
    """Extract source tables to raw landing parquet with manifests and state."""
    # Step 1: Initialize logging and parse CLI inputs for the run.
    # This makes sure run_date, mode, and partition settings are resolved consistently.
    # The expected outcome is a stable run_id and validated arguments before work begins.
    setup_logging()
    logger = logging.getLogger(APP_NAME)

    args = _parse_args()
    run_date_str = (
        args.run_date
        or get_env("RUN_DATE")
        or date.today().isoformat()
    )
    run_date = date.fromisoformat(run_date_str)
    run_id = _sanitize_run_id(args.run_id or str(uuid.uuid4()))

    # Step 2: Resolve per-run configuration and credentials.
    # This prepares event/watermark settings and verifies DB/MinIO secrets.
    # The expected outcome is non-empty credentials and normalized inputs.
    event_date_column = args.event_date_column.strip() or None
    watermark_column = args.watermark_column.strip() or None
    lookback_days = max(args.lookback_days, 0)
    watermark_from = parse_optional_datetime(args.watermark_from.strip())
    watermark_to = parse_optional_datetime(args.watermark_to.strip())
    snapshot_partitioned = _parse_bool(args.snapshot_partitioned)

    pg_host = get_env("PGHOST", "postgres-service")
    pg_port = get_env("PGPORT", "5432")
    pg_db = get_env("PGDATABASE", "ampere_db")
    pg_user = get_env("PGUSER")
    pg_password = get_env("PGPASSWORD")
    if not pg_user or not pg_password:
        raise ValueError("Missing PGUSER/PGPASSWORD for source database access.")

    minio_endpoint = get_env(
        "MINIO_S3_ENDPOINT", "http://minio.ampere.svc.cluster.local:9000"
    )
    minio_access_key = get_env("MINIO_ACCESS_KEY")
    minio_secret_key = get_env("MINIO_SECRET_KEY")
    if not minio_access_key or not minio_secret_key:
        raise ValueError("Missing MINIO_ACCESS_KEY/MINIO_SECRET_KEY for MinIO.")

    # Step 3: Build the table list and optional per-table config.
    # This allows a single job to handle multiple tables with overrides.
    # The expected outcome is a non-empty table list before starting Spark.
    default_shuffle_partitions = (
        args.shuffle_partitions if args.shuffle_partitions > 0 else None
    )
    groups_config = _parse_groups_config(
        args.groups_config, default_shuffle_partitions
    )
    if groups_config:
        groups = groups_config
    else:
        table_list = []
        if args.tables:
            table_list = _parse_table_list(args.tables)
        if not table_list and args.table:
            table_list = [args.table]
        if not table_list:
            raise ValueError("No tables provided. Use --table or --tables.")

        table_config = {}
        if args.table_config:
            table_config = json.loads(args.table_config)
        groups = [
            {
                "group": "default",
                "mode": args.mode,
                "partition_key": args.partition_key,
                "event_date_column": event_date_column or "",
                "watermark_column": watermark_column or "",
                "lookback_days": lookback_days,
                "tables": table_list,
                "table_config": table_config,
                "snapshot_partitioned": args.snapshot_partitioned,
                "shuffle_partitions": default_shuffle_partitions,
            }
        ]

    if groups_config:
        group_names = ",".join([g.get("group", "group") for g in groups])
        logger.info(
            "Starting ETL for %s groups (%s) run_date=%s",
            len(groups),
            group_names,
            run_date_str,
        )
    else:
        logger.info(
            "Starting ETL for %s (run_date=%s, mode=%s)",
            ",".join(groups[0]["tables"]),
            run_date_str,
            args.mode,
        )
    logger.info(
        "Postgres target host=%s port=%s db=%s user=%s",
        pg_host,
        pg_port,
        pg_db,
        pg_user,
    )
    logger.info("MinIO endpoint=%s bucket=%s", minio_endpoint, args.bucket)

    # Step 4: Initialize Spark, configure S3, and build the JDBC URL.
    # This sets the execution context used by every table extract.
    # The expected outcome is a ready SparkSession and valid JDBC target.
    spark = SparkSession.builder.appName(args.app_name).getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    configure_s3(spark, minio_endpoint, minio_access_key, minio_secret_key)

    jdbc_url = f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_db}"
    ingest_ts_utc = datetime.now(timezone.utc).isoformat()

    for group in groups:
        # Step 5: Resolve per-group settings and apply shuffle overrides.
        # This sets the execution strategy before per-table reads begin.
        # The expected outcome is a configured group context for table processing.
        group_name = group.get("group", "group")
        group_tables = group.get("tables", [])
        if not group_tables:
            logger.info("No tables configured for group %s", group_name)
            continue
        group_mode = group.get("mode", "snapshot")
        group_partition_key = group.get("partition_key", "snapshot_date")
        group_event_col = group.get("event_date_column") or None
        group_watermark_col = group.get("watermark_column") or None
        group_lookback_days = int(group.get("lookback_days", 0) or 0)
        group_snapshot_partitioned = _parse_bool(
            str(group.get("snapshot_partitioned", "true"))
        )
        _apply_group_shuffle(
            spark, group_name, group.get("shuffle_partitions")
        )

        table_config = group.get("table_config", {}) or {}

        for table in group_tables:
            # Step 6: Resolve per-table settings and detect initial-load conditions.
            # This decides how we filter incremental loads and whether to pull full history.
            # The expected outcome is a clear per-table extraction strategy before reading.
            table_meta = table_config.get(table, {})
            table_event_col = table_meta.get("event_date_column") or group_event_col
            table_watermark_col = table_meta.get("watermark_column") or group_watermark_col

            state_path_value = None
            table_watermark_from = watermark_from
            table_watermark_to = watermark_to
            if group_mode == "incremental" and group_partition_key == "extract_date":
                if not table_watermark_col:
                    raise ValueError(
                        "watermark_column is required for extract_date batches."
                    )
                state_path_value = state_path(
                    args.bucket, args.source_system, args.schema, table
                )
                if table_watermark_from is None:
                    state = read_json(spark, state_path_value, logger)
                    if state and state.get("last_watermark"):
                        table_watermark_from = parse_optional_datetime(
                            state["last_watermark"]
                        )
                if table_watermark_to is None:
                    table_watermark_to = datetime.now(timezone.utc)
                if table_watermark_from is None:
                    table_watermark_from = datetime(1900, 1, 1, tzinfo=timezone.utc)

            output_base = table_base_path(
                args.bucket, args.output_prefix, args.schema, table
            )
            initial_event_load = False
            if group_mode == "incremental" and group_partition_key == "event_date":
                if not _has_existing_incremental(spark, output_base):
                    initial_event_load = True
                    logger.info(
                        "Initial event load detected; extracting full table for %s",
                        table,
                    )

            # Step 7: Build the query filter and read source data via JDBC.
            # This ensures we only read the required slice when incrementing.
            # The expected outcome is a DataFrame covering the intended window.
            where_clause = None
            if not initial_event_load:
                where_clause = _build_where_clause(
                    group_mode,
                    group_partition_key,
                    run_date,
                    table_event_col,
                    table_watermark_col,
                    group_lookback_days,
                    table_watermark_from,
                    table_watermark_to,
                )
            dbtable = _build_dbtable(args.schema, table, where_clause)

            logger.info("Reading %s via JDBC", dbtable)
            df = (
                spark.read.format("jdbc")
                .option("url", jdbc_url)
                .option("dbtable", dbtable)
                .option("user", pg_user)
                .option("password", pg_password)
                .option("driver", "org.postgresql.Driver")
                .load()
            )

            # Step 8: Add technical columns and construct manifest metadata.
            # This makes raw batches traceable and consistent for downstream validation.
            # The expected outcome is a DataFrame with lineage fields and a manifest base.
            base_columns = df.columns
            hash_expr = F.sha2(
                F.concat_ws(
                    "||",
                    *[
                        F.coalesce(F.col(col_name).cast("string"), F.lit("<NULL>"))
                        for col_name in base_columns
                    ],
                ),
                256,
            )
            # Add technical columns used for lineage, idempotency, and audits.
            df = df.withColumn("_row_hash", hash_expr)
            df = df.withColumn("_ingest_run_id", F.lit(run_id))
            df = df.withColumn("_ingest_ts", F.current_timestamp())
            df = df.withColumn("_source_system", F.lit(args.source_system))
            df = df.withColumn("_source_table", F.lit(table))
            df = df.withColumn(
                "_op",
                F.lit("snapshot" if group_mode == "snapshot" else "incremental"),
            )

            schema_hash = _hash_schema(df.schema.json())

            manifest_base = {
                "manifest_version": "1",
                "source_system": args.source_system,
                "source_schema": args.schema,
                "source_table": table,
                "contract_name": f"{args.schema}.{table}",
                "contract_version": "1",
                "run_id": run_id,
                "ingest_ts_utc": ingest_ts_utc,
                "batch_type": group_mode,
                "storage_format": "parquet",
                "schema_hash": schema_hash,
                "producer": {
                    "tool": "spark",
                    "image": args.image or "unknown",
                    "app_name": args.app_name,
                    "git_sha": get_env("GIT_SHA", "unknown"),
                },
                "source_extract": {
                    "query": dbtable,
                    "read_mode": "jdbc",
                    "partitioning": None,
                },
            }

            # Step 9: Write output batches based on the partition strategy.
            # This emits parquet, manifest, and _SUCCESS markers in the expected paths.
            # The expected outcome is a complete batch per partition and updated state.
            if group_mode == "snapshot":
                partition_value = run_date_str
                if group_snapshot_partitioned:
                    output_path = (
                        f"{output_base}/mode=snapshot/snapshot_date={partition_value}/"
                        f"run_id={run_id}/"
                    )
                else:
                    output_path = f"{output_base}/mode=snapshot/run_id={run_id}/"
                manifest_context = {
                    **manifest_base,
                    "snapshot_date": partition_value,
                }
                _write_batch(
                    spark,
                    df,
                    output_path,
                    manifest_context,
                    base_columns,
                    logger,
                )
            elif group_partition_key == "extract_date":
                partition_value = run_date_str
                output_path = (
                    f"{output_base}/mode=incremental/extract_date={partition_value}/"
                    f"run_id={run_id}/"
                )
                manifest_context = {
                    **manifest_base,
                    "extract_date": partition_value,
                }
                upper = table_watermark_to or datetime.now(timezone.utc)
                lower = table_watermark_from or (upper - timedelta(days=1))
                if table_watermark_col:
                    window_from = _format_ts(lower)
                    window_to = _format_ts(upper)
                    manifest_context["watermark"] = {
                        "column": table_watermark_col,
                        "from": window_from,
                        "to": window_to,
                    }
                batch_result = _write_batch(
                    spark,
                    df,
                    output_path,
                    manifest_context,
                    base_columns,
                    logger,
                )
                if state_path_value and batch_result["success"]:
                    state_payload = {
                        "source_system": args.source_system,
                        "source_schema": args.schema,
                        "source_table": table,
                        "watermark_column": table_watermark_col,
                        "last_watermark": _format_ts(upper),
                        "last_successful_run_id": run_id,
                        "last_successful_ingest_ts_utc": ingest_ts_utc,
                        "last_manifest_path": batch_result["manifest_path"],
                    }
                    write_bytes(
                        spark,
                        state_path_value,
                        json.dumps(state_payload, indent=2, sort_keys=True).encode("utf-8"),
                    )
            else:
                event_col = table_event_col
                if not event_col:
                    raise ValueError(
                        "event_date_column is required for event_date batches."
                    )

                if initial_event_load:
                    event_date_strings = _collect_event_dates(df, event_col)
                else:
                    _, _, event_dates = _date_range(run_date, group_lookback_days)
                    event_date_strings = [d.isoformat() for d in event_dates]

                df_with_event_date = df.withColumn(
                    "_event_date", F.to_date(F.col(event_col))
                )
                missing_event_dates = df_with_event_date.filter(
                    F.col("_event_date").isNull()
                ).limit(1).count()
                if missing_event_dates:
                    logger.warning(
                        "Null event_date detected for %s.%s", args.schema, table
                    )

                for event_date in event_date_strings:
                    output_path = (
                        f"{output_base}/mode=incremental/event_date={event_date}/"
                        f"run_id={run_id}/"
                    )
                    manifest_context = {
                        **manifest_base,
                        "event_date": event_date,
                    }
                    if not initial_event_load and group_lookback_days > 1:
                        start_date, end_date, _ = _date_range(
                            run_date, group_lookback_days
                        )
                        manifest_context["lookback_days"] = group_lookback_days
                        window_from, window_to = _format_date_window(
                            start_date, end_date
                        )
                        manifest_context["window"] = {
                            "from": window_from,
                            "to": window_to,
                        }
                        manifest_context["event_dates_covered"] = event_date_strings
                    df_partition = df_with_event_date.filter(
                        F.col("_event_date") == F.lit(event_date)
                    ).drop("_event_date")
                    _write_batch(
                        spark,
                        df_partition,
                        output_path,
                        manifest_context,
                        base_columns,
                        logger,
                        allow_empty=False,
                    )

            logger.info("ETL completed for %s.%s", args.schema, table)

    spark.stop()


if __name__ == "__main__":
    main()
