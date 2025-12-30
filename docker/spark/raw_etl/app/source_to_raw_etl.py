import argparse
import hashlib
import json
import logging
import os
import re
import sys
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Optional

from pyspark.sql import SparkSession, functions as F

APP_NAME = "source-to-raw-etl"


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


def _parse_args() -> argparse.Namespace:
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
        help="Extraction mode",
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


def _sanitize_run_id(value: str) -> str:
    if not value:
        return value
    return re.sub(r"[^a-zA-Z0-9._-]", "_", value)


def _build_output_base(
    bucket: str, prefix: str, schema: str, table: str
) -> str:
    prefix = prefix.strip("/")
    if prefix:
        return f"s3a://{bucket}/{prefix}/{schema}/{table}"
    return f"s3a://{bucket}/{schema}/{table}"


def _date_range(run_date: date, lookback_days: int) -> tuple[date, date, list[date]]:
    if lookback_days < 1:
        lookback_days = 1
    end_date = run_date + timedelta(days=1)
    start_date = run_date - timedelta(days=lookback_days - 1)
    dates = [start_date + timedelta(days=i) for i in range(lookback_days)]
    return start_date, end_date, dates


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


def _format_ts(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.isoformat()


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
    source_table = f'"{schema}"."{table}"'
    if where_clause:
        return f"(SELECT * FROM {source_table} WHERE {where_clause}) AS src"
    return source_table


def _hash_schema(schema_json: str) -> str:
    return hashlib.md5(schema_json.encode("utf-8")).hexdigest()


def _collect_file_details(
    spark: SparkSession, output_path: str
) -> list[dict]:
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


def _write_marker(
    spark: SparkSession, output_path: str, filename: str, content: bytes
) -> None:
    jvm = spark._jvm
    path = jvm.org.apache.hadoop.fs.Path(
        output_path.rstrip("/") + "/" + filename
    )
    fs = path.getFileSystem(spark._jsc.hadoopConfiguration())
    output_stream = fs.create(path, True)
    output_stream.write(content)
    output_stream.close()


def _write_file(spark: SparkSession, path_str: str, content: bytes) -> None:
    jvm = spark._jvm
    path = jvm.org.apache.hadoop.fs.Path(path_str)
    fs = path.getFileSystem(spark._jsc.hadoopConfiguration())
    output_stream = fs.create(path, True)
    output_stream.write(content)
    output_stream.close()


def _read_state(spark: SparkSession, path_str: str) -> Optional[dict]:
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


def _state_path(
    bucket: str, source_system: str, schema: str, table: str
) -> str:
    return (
        f"s3a://{bucket}/{source_system}/{schema}/_state/"
        f"_state_{table}.json"
    )


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


def _write_batch(
    spark: SparkSession,
    df,
    output_path: str,
    manifest_context: dict,
    base_columns: list[str],
    logger: logging.Logger,
) -> dict:
    row_count = df.count()
    df.write.mode("overwrite").parquet(output_path)

    files = _collect_file_details(spark, output_path)
    file_count = len(files)

    null_counts, min_max = _collect_stats(df, base_columns)

    manifest = {
        **manifest_context,
        "file_count": file_count,
        "row_count": row_count,
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

    manifest_payload = json.dumps(manifest, indent=2, sort_keys=True).encode(
        "utf-8"
    )
    _write_marker(spark, output_path, "_manifest.json", manifest_payload)

    if any(check["status"] == "fail" for check in manifest["checks"]):
        logger.warning("Skipping _SUCCESS due to failed checks.")
        return {
            "success": False,
            "manifest_path": output_path.rstrip("/") + "/_manifest.json",
            "row_count": row_count,
            "file_count": file_count,
        }
    _write_marker(spark, output_path, "_SUCCESS", b"")
    return {
        "success": True,
        "manifest_path": output_path.rstrip("/") + "/_manifest.json",
        "row_count": row_count,
        "file_count": file_count,
    }


def main() -> None:
    setup_logging()
    logger = logging.getLogger(APP_NAME)

    args = _parse_args()
    run_date_str = (
        args.run_date
        or _get_env("RUN_DATE")
        or date.today().isoformat()
    )
    run_date = date.fromisoformat(run_date_str)
    run_id = _sanitize_run_id(args.run_id or str(uuid.uuid4()))

    event_date_column = args.event_date_column.strip() or None
    watermark_column = args.watermark_column.strip() or None
    lookback_days = max(args.lookback_days, 0)
    watermark_from = _parse_optional_datetime(args.watermark_from.strip())
    watermark_to = _parse_optional_datetime(args.watermark_to.strip())

    pg_host = _get_env("PGHOST", "postgres-service")
    pg_port = _get_env("PGPORT", "5432")
    pg_db = _get_env("PGDATABASE", "ampere_db")
    pg_user = _get_env("PGUSER")
    pg_password = _get_env("PGPASSWORD")
    if not pg_user or not pg_password:
        raise ValueError("Missing PGUSER/PGPASSWORD for source database access.")

    minio_endpoint = _get_env(
        "MINIO_S3_ENDPOINT", "http://minio.ampere.svc.cluster.local:9000"
    )
    minio_access_key = _get_env("MINIO_ACCESS_KEY")
    minio_secret_key = _get_env("MINIO_SECRET_KEY")
    if not minio_access_key or not minio_secret_key:
        raise ValueError("Missing MINIO_ACCESS_KEY/MINIO_SECRET_KEY for MinIO.")

    table_list = []
    if args.tables:
        table_list = [t.strip() for t in args.tables.split(",") if t.strip()]
    if not table_list and args.table:
        table_list = [args.table]
    if not table_list:
        raise ValueError("No tables provided. Use --table or --tables.")

    table_config = {}
    if args.table_config:
        table_config = json.loads(args.table_config)

    logger.info(
        "Starting ETL for %s (run_date=%s, mode=%s)",
        ",".join(table_list),
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

    spark = SparkSession.builder.appName(args.app_name).getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    _configure_s3(spark, minio_endpoint, minio_access_key, minio_secret_key)

    jdbc_url = f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_db}"
    ingest_ts_utc = datetime.now(timezone.utc).isoformat()

    for table in table_list:
        table_meta = table_config.get(table, {})
        table_event_col = table_meta.get("event_date_column") or event_date_column
        table_watermark_col = table_meta.get("watermark_column") or watermark_column

        state_path = None
        table_watermark_from = watermark_from
        table_watermark_to = watermark_to
        if args.mode == "incremental" and args.partition_key == "extract_date":
            if not table_watermark_col:
                raise ValueError(
                    "watermark_column is required for extract_date batches."
                )
            state_path = _state_path(
                args.bucket, args.source_system, args.schema, table
            )
            if table_watermark_from is None:
                state = _read_state(spark, state_path)
                if state and state.get("last_watermark"):
                    table_watermark_from = _parse_optional_datetime(
                        state["last_watermark"]
                    )
            if table_watermark_to is None:
                table_watermark_to = datetime.now(timezone.utc)
            if table_watermark_from is None:
                table_watermark_from = datetime(1900, 1, 1, tzinfo=timezone.utc)

        where_clause = _build_where_clause(
            args.mode,
            args.partition_key,
            run_date,
            table_event_col,
            table_watermark_col,
            lookback_days,
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
        df = df.withColumn("_source_system", F.lit(args.source_system))
        df = df.withColumn("_source_table", F.lit(table))
        df = df.withColumn(
            "_op", F.lit("snapshot" if args.mode == "snapshot" else "incremental")
        )

        output_base = _build_output_base(
            args.bucket, args.output_prefix, args.schema, table
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
            "batch_type": args.mode,
            "storage_format": "parquet",
            "schema_hash": schema_hash,
            "checksum": schema_hash,
            "producer": {
                "tool": "spark",
                "image": args.image or "unknown",
                "app_name": args.app_name,
                "git_sha": _get_env("GIT_SHA", "unknown"),
            },
            "source_extract": {
                "query": dbtable,
                "read_mode": "jdbc",
                "partitioning": None,
            },
        }

        if args.mode == "snapshot":
            partition_value = run_date_str
            output_path = (
                f"{output_base}/mode=snapshot/snapshot_date={partition_value}/"
                f"run_id={run_id}/"
            )
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
        elif args.partition_key == "extract_date":
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
            if state_path and batch_result["success"]:
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
                _write_file(
                    spark,
                    state_path,
                    json.dumps(state_payload, indent=2, sort_keys=True).encode("utf-8"),
                )
        else:
            event_col = table_event_col
            if not event_col:
                raise ValueError(
                    "event_date_column is required for event_date batches."
                )

            _, _, event_dates = _date_range(run_date, lookback_days)
            event_date_strings = [d.isoformat() for d in event_dates]
            df_with_event_date = df.withColumn(
                "_event_date", F.to_date(F.col(event_col))
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
                if lookback_days > 1:
                    start_date, end_date, _ = _date_range(run_date, lookback_days)
                    manifest_context["lookback_days"] = lookback_days
                    manifest_context["window"] = {
                        "from": start_date.isoformat(),
                        "to": end_date.isoformat(),
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
                )

        logger.info("ETL completed for %s.%s", args.schema, table)

    spark.stop()


if __name__ == "__main__":
    main()
