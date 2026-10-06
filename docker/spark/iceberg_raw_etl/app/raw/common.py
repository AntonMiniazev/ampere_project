"""Shared source-to-raw batch helpers used by the per-group modules."""

from __future__ import annotations

from dataclasses import dataclass
import json
import logging
from datetime import date, datetime, timedelta, timezone
from typing import Optional

from pyspark.sql import SparkSession, functions as F

from etl_utils import (
    date_range,
    exists,
    format_date_window,
    format_ts,
    md5_hexdigest,
    read_json,
    write_bytes,
    write_marker,
)


@dataclass(frozen=True)
class RawTablePlan:
    """Normalized per-table extraction plan used by the raw writers."""

    group_name: str
    group_mode: str
    group_partition_key: str
    snapshot_partitioned: bool
    output_base: str
    table_event_col: str | None
    table_lookback_days: int
    table_watermark_col: str | None
    table_created_col: str | None
    table_cursor_granularity: str
    state_path_value: str | None
    table_watermark_from: datetime | None
    table_created_from: datetime | None
    table_watermark_to: datetime | None
    bootstrap_full_extract: bool
    initial_event_load: bool
    where_clause: str | None
    dbtable: str


def build_where_clause(
    mode: str,
    partition_key: str,
    run_date: date,
    event_date_column: str | None,
    watermark_column: str | None,
    created_column: str | None,
    cursor_granularity: str,
    lookback_days: int,
    watermark_from: Optional[datetime],
    created_from: Optional[datetime],
    watermark_to: Optional[datetime],
) -> Optional[str]:
    """Build a WHERE clause for incremental pulls based on partition strategy."""
    if mode != "incremental":
        return None

    if partition_key == "extract_date" and watermark_column:
        upper = watermark_to
        lower = watermark_from
        if upper is None:
            upper = datetime.combine(run_date, datetime.min.time(), tzinfo=timezone.utc)
        if lookback_days > 0:
            lower = upper - timedelta(days=lookback_days)
            created_lower = lower
        else:
            if lower is None:
                lower = upper - timedelta(days=1)
            created_lower = created_from
        if cursor_granularity == "date":
            upper_date = upper.date()
            if lookback_days > 0:
                lower_date = upper_date - timedelta(days=max(lookback_days - 1, 0))
                created_lower_date = lower_date
            else:
                lower_date = (lower or upper).date()
                created_lower_date = (
                    created_lower.date() if created_lower is not None else lower_date
                )
            if created_column:
                return (
                    f"(({watermark_column} IS NOT NULL AND "
                    f"{watermark_column} >= DATE '{lower_date.isoformat()}' AND "
                    f"{watermark_column} <= DATE '{upper_date.isoformat()}') "
                    f"OR ({watermark_column} IS NULL AND "
                    f"{created_column} >= DATE '{created_lower_date.isoformat()}' AND "
                    f"{created_column} <= DATE '{upper_date.isoformat()}'))"
                )
            return (
                f"{watermark_column} >= DATE '{lower_date.isoformat()}' "
                f"AND {watermark_column} <= DATE '{upper_date.isoformat()}'"
            )
        if created_column:
            if created_lower is None:
                created_lower = lower
            return (
                f"(({watermark_column} IS NOT NULL AND "
                f"{watermark_column} > TIMESTAMPTZ '{format_ts(lower)}' AND "
                f"{watermark_column} <= TIMESTAMPTZ '{format_ts(upper)}') "
                f"OR ({watermark_column} IS NULL AND "
                f"{created_column} > TIMESTAMPTZ '{format_ts(created_lower)}' AND "
                f"{created_column} <= TIMESTAMPTZ '{format_ts(upper)}'))"
            )
        return (
            f"{watermark_column} > TIMESTAMPTZ '{format_ts(lower)}' "
            f"AND {watermark_column} <= TIMESTAMPTZ '{format_ts(upper)}'"
        )

    if partition_key == "event_date" and event_date_column:
        start_date, end_date, _ = date_range(run_date, lookback_days)
        return (
            f"{event_date_column} >= DATE '{start_date.isoformat()}' "
            f"AND {event_date_column} < DATE '{end_date.isoformat()}'"
        )

    return None


def build_dbtable(schema: str, table: str, where_clause: Optional[str]) -> str:
    """Build a JDBC dbtable string, optionally wrapping a filtered subquery."""
    source_table = f'"{schema}"."{table}"'
    if where_clause:
        return f"(SELECT * FROM {source_table} WHERE {where_clause}) AS src"
    return source_table


def add_ingest_columns(
    df,
    *,
    run_id: str,
    source_system: str,
    table: str,
    mode: str,
):
    """Add technical lineage columns used by the raw landing contract."""
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
    df = df.withColumn("_row_hash", hash_expr)
    df = df.withColumn("_ingest_run_id", F.lit(run_id))
    df = df.withColumn("_ingest_ts", F.current_timestamp())
    df = df.withColumn("_source_system", F.lit(source_system))
    df = df.withColumn("_source_table", F.lit(table))
    df = df.withColumn("_op", F.lit("snapshot" if mode == "snapshot" else "incremental"))
    return df, base_columns


def build_manifest_base(
    *,
    source_system: str,
    schema: str,
    table: str,
    run_id: str,
    ingest_ts_utc: str,
    group_mode: str,
    schema_hash: str,
    image: str,
    app_name: str,
    dbtable: str,
    git_sha: str,
) -> dict:
    """Build the shared manifest payload used by all raw group writers."""
    return {
        "manifest_version": "1",
        "source_system": source_system,
        "source_schema": schema,
        "source_table": table,
        "contract_name": f"{schema}.{table}",
        "contract_version": "1",
        "run_id": run_id,
        "ingest_ts_utc": ingest_ts_utc,
        "batch_type": group_mode,
        "storage_format": "parquet",
        "schema_hash": schema_hash,
        "producer": {
            "tool": "spark",
            "image": image or "unknown",
            "app_name": app_name,
            "git_sha": git_sha,
        },
        "source_extract": {
            "query": dbtable,
            "read_mode": "jdbc",
            "partitioning": None,
        },
    }


def hash_schema_json(schema_json: str) -> str:
    """Hash a Spark schema JSON string."""
    return md5_hexdigest(schema_json)


def apply_group_shuffle(
    spark: SparkSession,
    group_name: str,
    shuffle_partitions: Optional[int],
) -> None:
    """Override spark.sql.shuffle.partitions for the current group when set."""
    if not shuffle_partitions:
        return
    spark.conf.set("spark.sql.shuffle.partitions", str(shuffle_partitions))
    logging.getLogger("source-to-raw-etl").info(
        "Set spark.sql.shuffle.partitions=%s for group=%s",
        shuffle_partitions,
        group_name,
    )


def collect_event_dates(df, event_col: str) -> list[str]:
    """Collect distinct event dates from a DataFrame."""
    rows = (
        df.select(F.to_date(F.col(event_col)).alias("event_date"))
        .where(F.col("event_date").isNotNull())
        .distinct()
        .collect()
    )
    return sorted({row.event_date.isoformat() for row in rows if row.event_date})


def has_incremental_output(spark: SparkSession, output_base: str) -> bool:
    """Return True when incremental output already exists for a table."""
    return exists(spark, f"{output_base}/mode=incremental")


def _compute_batch_checksum(files: list[dict]) -> str:
    ordered = sorted(files, key=lambda entry: entry.get("path", ""))
    payload = "|".join(
        f"{entry.get('path', '')}:{entry.get('checksum', '')}" for entry in ordered
    )
    return md5_hexdigest(payload)


def _collect_file_details(spark: SparkSession, output_path: str) -> list[dict]:
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


def write_batch(
    spark: SparkSession,
    df,
    output_path: str,
    manifest_context: dict,
    base_columns: list[str],
    logger: logging.Logger,
    allow_empty: bool = True,
) -> dict:
    """Write parquet output plus manifest and _SUCCESS for a single batch."""
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
    manifest_payload = json.dumps(manifest, indent=2, sort_keys=True).encode("utf-8")
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


def write_event_partition_batches(
    *,
    spark: SparkSession,
    df,
    output_base: str,
    event_col: str,
    run_date: date,
    run_id: str,
    manifest_base: dict,
    base_columns: list[str],
    lookback_days: int,
    initial_event_load: bool,
    logger: logging.Logger,
) -> None:
    """Write one raw batch per event_date partition from a shared DataFrame."""
    if initial_event_load:
        event_date_strings = collect_event_dates(df, event_col)
    else:
        _, _, event_dates = date_range(run_date, lookback_days)
        event_date_strings = [d.isoformat() for d in event_dates]

    df_with_event_date = df.withColumn("_event_date", F.to_date(F.col(event_col)))
    missing_event_dates = (
        df_with_event_date.filter(F.col("_event_date").isNull()).limit(1).count()
    )
    if missing_event_dates:
        logger.warning("Null event_date detected for event column %s", event_col)

    for event_date in event_date_strings:
        output_path = (
            f"{output_base}/mode=incremental/event_date={event_date}/run_id={run_id}/"
        )
        manifest_context = {
            **manifest_base,
            "event_date": event_date,
        }
        if not initial_event_load and lookback_days > 1:
            start_date, end_date, _ = date_range(run_date, lookback_days)
            manifest_context["lookback_days"] = lookback_days
            window_from, window_to = format_date_window(start_date, end_date)
            manifest_context["window"] = {
                "from": window_from,
                "to": window_to,
            }
            manifest_context["event_dates_covered"] = event_date_strings
        df_partition = df_with_event_date.filter(F.col("_event_date") == F.lit(event_date)).drop(
            "_event_date"
        )
        write_batch(
            spark,
            df_partition,
            output_path,
            manifest_context,
            base_columns,
            logger,
            allow_empty=False,
        )
