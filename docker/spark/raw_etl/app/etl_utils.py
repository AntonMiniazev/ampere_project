"""Shared ETL helpers for Spark + MinIO IO, pathing, and metadata validation."""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import date, datetime
from typing import Optional

from pyspark.sql import DataFrame, SparkSession

# --- Logging and environment helpers ---


def setup_logging(level: str | None = None) -> None:
    """Configure root logging for ETL jobs.

    This keeps a consistent format across local runs and Spark driver output.

    Args:
        level: Optional override for log level, e.g. "INFO", "DEBUG", or None.
    """
    resolved = level or os.getenv("LOG_LEVEL", "INFO")
    logging.basicConfig(
        level=resolved,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
        force=True,
    )


def get_env(name: str, default: Optional[str] = None) -> Optional[str]:
    """Return an environment variable or fallback when missing.

    Empty strings are treated as missing to avoid accidental blanks.

    Args:
        name: Environment variable name, e.g. "MINIO_ACCESS_KEY".
        default: Fallback value, e.g. "minioadmin" or None.
    """
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return value

# --- Parsing helpers ---


def parse_date(value: str) -> str:
    """Validate a YYYY-MM-DD date string and return it unchanged.

    Args:
        value: Date string, e.g. "2026-01-24".
    """
    datetime.strptime(value, "%Y-%m-%d")
    return value


def parse_optional_datetime(value: str) -> Optional[datetime]:
    """Parse ISO-8601 or YYYY-MM-DD into datetime, or None when blank.

    Args:
        value: Datetime string, e.g. "2026-01-24T12:00:00Z" or "2026-01-24".
    """
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

# --- Spark and storage configuration ---


def configure_s3(
    spark: SparkSession,
    endpoint: str,
    access_key: str,
    secret_key: str,
) -> None:
    """Configure Hadoop S3A settings for MinIO or S3-compatible storage.

    Args:
        spark: Active SparkSession, e.g. SparkSession.builder.getOrCreate().
        endpoint: S3 endpoint URL, e.g. "http://minio.ampere.svc.cluster.local:9000".
        access_key: Access key ID, e.g. "minioadmin".
        secret_key: Secret access key, e.g. "minioadmin".
    """
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

# --- Path helpers ---


def s3_path(bucket: str, *parts: str) -> str:
    """Build an s3a:// path from a bucket and optional path parts.

    Args:
        bucket: Bucket name, e.g. "ampere-raw".
        parts: Path segments, e.g. "postgres-pre-raw", "source", "orders".
    """
    cleaned = [part.strip("/") for part in parts if part]
    if not cleaned:
        return f"s3a://{bucket}"
    return f"s3a://{bucket}/" + "/".join(cleaned)


def table_base_path(bucket: str, prefix: str, schema: str, table: str) -> str:
    """Return the base path for a table under bucket/prefix/schema.

    Args:
        bucket: Bucket name, e.g. "ampere-raw".
        prefix: Optional prefix, e.g. "postgres-pre-raw".
        schema: Source schema, e.g. "source".
        table: Table name, e.g. "orders".
    """
    prefix = prefix.strip("/")
    if prefix:
        return s3_path(bucket, prefix, schema, table)
    return s3_path(bucket, schema, table)


def state_path(bucket: str, source_system: str, schema: str, table: str) -> str:
    """Return the landing _state JSON path for a given table.

    Args:
        bucket: Bucket name, e.g. "ampere-raw".
        source_system: Source system id, e.g. "postgres-pre-raw".
        schema: Source schema, e.g. "source".
        table: Table name, e.g. "orders".
    """
    return s3_path(
        bucket, source_system, schema, "_state", f"_state_{table}.json"
    )


def bronze_registry_path(bucket: str, prefix: str) -> str:
    """Return the Delta registry table path for bronze apply tracking.

    Args:
        bucket: Bucket name, e.g. "ampere-bronze".
        prefix: Optional prefix, e.g. "bronze".
    """
    prefix = prefix.strip("/")
    if prefix:
        return s3_path(bucket, prefix, "ops", "bronze_apply_registry")
    return s3_path(bucket, "ops", "bronze_apply_registry")

# --- Spark filesystem helpers ---


def list_dirs(spark: SparkSession, path_str: str) -> list[str]:
    """List child directory names under a Hadoop FS path.

    Args:
        spark: Active SparkSession, e.g. SparkSession.builder.getOrCreate().
        path_str: Directory path, e.g. "s3a://ampere-raw/source/orders".
    """
    jvm = spark._jvm
    path = jvm.org.apache.hadoop.fs.Path(path_str)
    fs = path.getFileSystem(spark._jsc.hadoopConfiguration())
    if not fs.exists(path):
        return []
    statuses = fs.listStatus(path)
    return [status.getPath().getName() for status in statuses if status.isDirectory()]


def exists(spark: SparkSession, path_str: str) -> bool:
    """Check whether a Hadoop FS path exists.

    Args:
        spark: Active SparkSession, e.g. SparkSession.builder.getOrCreate().
        path_str: Path to check, e.g. "s3a://ampere-raw/.../_manifest.json".
    """
    jvm = spark._jvm
    path = jvm.org.apache.hadoop.fs.Path(path_str)
    fs = path.getFileSystem(spark._jsc.hadoopConfiguration())
    return fs.exists(path)


def write_bytes(spark: SparkSession, path_str: str, content: bytes) -> None:
    """Write raw bytes to a Hadoop FS path, overwriting existing data.

    Args:
        spark: Active SparkSession, e.g. SparkSession.builder.getOrCreate().
        path_str: Target path, e.g. "s3a://ampere-raw/.../_manifest.json".
        content: File payload, e.g. b"{\"ok\": true}".
    """
    jvm = spark._jvm
    path = jvm.org.apache.hadoop.fs.Path(path_str)
    fs = path.getFileSystem(spark._jsc.hadoopConfiguration())
    output_stream = fs.create(path, True)
    output_stream.write(bytearray(content))
    output_stream.close()


def write_marker(
    spark: SparkSession, output_path: str, filename: str, content: bytes
) -> None:
    """Write a marker file within an output directory.

    Args:
        spark: Active SparkSession, e.g. SparkSession.builder.getOrCreate().
        output_path: Directory path, e.g. "s3a://ampere-raw/.../run_id=xyz".
        filename: Marker name, e.g. "_SUCCESS".
        content: Marker payload, e.g. b"".
    """
    path_str = output_path.rstrip("/") + "/" + filename
    write_bytes(spark, path_str, content)

# --- JSON read helpers ---


def read_json(
    spark: SparkSession,
    path_str: str,
    logger: Optional[logging.Logger] = None,
) -> Optional[dict]:
    """Read JSON from storage with Hadoop, boto3, and Spark fallbacks.

    This detects NUL-filled payloads and logs diagnostics for bad JSON.

    Args:
        spark: Active SparkSession, e.g. SparkSession.builder.getOrCreate().
        path_str: JSON path, e.g. "s3a://ampere-raw/.../_manifest.json".
        logger: Logger override, e.g. logging.getLogger("raw-to-bronze").
    """
    logger = logger or logging.getLogger(__name__)
    hadoop_payload = _read_bytes_hadoop(spark, path_str, logger)
    if hadoop_payload is not None:
        if _is_all_nulls(hadoop_payload):
            _log_null_payload(logger, path_str, hadoop_payload, "hadoop")
            boto_payload = _read_bytes_boto3(path_str, logger)
            if boto_payload is None or _is_all_nulls(boto_payload):
                if boto_payload is not None:
                    _log_null_payload(logger, path_str, boto_payload, "boto3")
                return None
            parsed = _try_parse_json(path_str, boto_payload, "boto3", logger)
            if parsed is not None:
                return parsed
            return None
        parsed = _try_parse_json(path_str, hadoop_payload, "hadoop", logger)
        if parsed is not None:
            return parsed

    boto_payload = _read_bytes_boto3(path_str, logger)
    if boto_payload is not None:
        parsed = _try_parse_json(path_str, boto_payload, "boto3", logger)
        if parsed is not None:
            return parsed

    spark_payload = _read_bytes_spark(spark, path_str, logger)
    if spark_payload is not None:
        parsed = _try_parse_json(path_str, spark_payload, "spark", logger)
        if parsed is not None:
            return parsed
    return None


def _read_bytes_hadoop(
    spark: SparkSession,
    path_str: str,
    logger: logging.Logger,
) -> Optional[bytes]:
    """Read raw bytes via Hadoop FS; returns None if the file does not exist.

    Args:
        spark: Active SparkSession, e.g. SparkSession.builder.getOrCreate().
        path_str: File path, e.g. "s3a://ampere-raw/.../_manifest.json".
        logger: Logger instance, e.g. logging.getLogger("raw-to-bronze").
    """
    jvm = spark._jvm
    path = jvm.org.apache.hadoop.fs.Path(path_str)
    fs = path.getFileSystem(spark._jsc.hadoopConfiguration())
    if not fs.exists(path):
        return None
    file_size = None
    try:
        file_size = fs.getFileStatus(path).getLen()
    except Exception as exc:  # noqa: BLE001
        logger.warning("Failed to stat JSON at %s: %s", path_str, exc)
    input_stream = fs.open(path)
    try:
        output_stream = jvm.java.io.ByteArrayOutputStream()
        jvm.org.apache.hadoop.io.IOUtils.copyBytes(
            input_stream, output_stream, 4096, False
        )
        data = output_stream.toByteArray()
        return bytes(bytearray(data))
    except Exception as exc:  # noqa: BLE001
        logger.warning("Hadoop read failed at %s: %s", path_str, exc)
        return None
    finally:
        input_stream.close()


def _read_bytes_spark(
    spark: SparkSession,
    path_str: str,
    logger: logging.Logger,
) -> Optional[bytes]:
    """Read raw bytes via Spark whole-text read; returns None on failure.

    Args:
        spark: Active SparkSession, e.g. SparkSession.builder.getOrCreate().
        path_str: File path, e.g. "s3a://ampere-raw/.../_manifest.json".
        logger: Logger instance, e.g. logging.getLogger("raw-to-bronze").
    """
    try:
        rows = (
            spark.read.option("wholetext", "true").text(path_str).collect()
        )
        if not rows:
            return b""
        payload = "\n".join(
            row.value for row in rows if row.value is not None
        )
        return payload.encode("utf-8")
    except Exception as exc:  # noqa: BLE001
        logger.warning("Spark text read failed at %s: %s", path_str, exc)
        return None


def _read_bytes_boto3(
    path_str: str,
    logger: logging.Logger,
) -> Optional[bytes]:
    """Read raw bytes from s3a:// paths using boto3.

    Args:
        path_str: File path, e.g. "s3a://ampere-raw/.../_manifest.json".
        logger: Logger instance, e.g. logging.getLogger("raw-to-bronze").
    """
    try:
        import boto3
        from botocore.client import Config
    except Exception as exc:  # noqa: BLE001
        logger.warning("boto3 unavailable for manifest fallback: %s", exc)
        return None
    endpoint = get_env(
        "MINIO_S3_ENDPOINT",
        "http://minio.ampere.svc.cluster.local:9000",
    )
    access_key = get_env("MINIO_ACCESS_KEY")
    secret_key = get_env("MINIO_SECRET_KEY")
    if not access_key or not secret_key:
        logger.warning("Missing MinIO creds for boto3 manifest fallback.")
        return None
    if not path_str.startswith("s3a://"):
        logger.warning("Unsupported manifest path for boto3 fallback: %s", path_str)
        return None
    bucket_key = path_str[len("s3a://") :]
    bucket, _, key = bucket_key.partition("/")
    if not bucket or not key:
        logger.warning("Invalid manifest path for boto3 fallback: %s", path_str)
        return None
    try:
        client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(signature_version="s3v4"),
            region_name="us-east-1",
        )
        body = client.get_object(Bucket=bucket, Key=key)["Body"].read()
        if not body:
            logger.warning("Empty manifest via boto3 at %s", path_str)
            return b""
        return body
    except Exception as exc:  # noqa: BLE001
        logger.warning("boto3 manifest read failed at %s: %s", path_str, exc)
        return None


def _clean_payload(payload: bytes) -> str:
    """Normalize payloads by stripping null bytes, BOM, and whitespace.

    Args:
        payload: Raw bytes, e.g. b"\x00\x00{...}\n".
    """
    text = payload.decode("utf-8", errors="replace")
    return text.replace("\x00", "").lstrip("\ufeff").strip()


def _is_all_nulls(payload: bytes) -> bool:
    """Return True when every byte in the payload is NUL.

    Args:
        payload: Raw bytes, e.g. b"\x00\x00\x00".
    """
    if not payload:
        return False
    return payload.count(b"\x00") == len(payload)


def _json_kind(path_str: str) -> str:
    """Classify JSON payloads for logging purposes.

    Args:
        path_str: File path, e.g. "s3a://.../_manifest.json".
    """
    if "_state_" in path_str:
        return "state"
    if path_str.endswith("/_manifest.json") or path_str.endswith("_manifest.json"):
        return "manifest"
    return "json"


def _try_parse_json(
    path_str: str,
    payload: bytes,
    source: str,
    logger: logging.Logger,
) -> Optional[dict]:
    """Parse JSON payloads with logging on errors.

    Args:
        path_str: File path, e.g. "s3a://.../_manifest.json".
        payload: Raw bytes, e.g. b"{\"a\": 1}".
        source: Reader name, e.g. "hadoop", "boto3", or "spark".
        logger: Logger instance, e.g. logging.getLogger("raw-to-bronze").
    """
    cleaned = _clean_payload(payload)
    if not cleaned:
        _log_empty_payload(logger, path_str, payload, source)
        return None
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError as exc:
        preview = cleaned[:200].replace("\n", "\\n")
        logger.warning(
            "Invalid JSON via %s at %s: %s | preview=%s",
            source,
            path_str,
            exc,
            preview,
        )
        return None


def _log_empty_payload(
    logger: logging.Logger, path_str: str, payload: bytes, source: str
) -> None:
    """Log diagnostics for payloads that become empty after cleanup.

    Args:
        logger: Logger instance, e.g. logging.getLogger("raw-to-bronze").
        path_str: File path, e.g. "s3a://.../_manifest.json".
        payload: Raw bytes, e.g. b"" or b"\x00\x00".
        source: Reader name, e.g. "hadoop".
    """
    size = len(payload)
    null_count = payload.count(b"\x00")
    preview = payload[:32].hex()
    logger.warning(
        "JSON empty after cleanup via %s at %s (size=%s nulls=%s preview_hex=%s)",
        source,
        path_str,
        size,
        null_count,
        preview,
    )


def _log_null_payload(
    logger: logging.Logger, path_str: str, payload: bytes, source: str
) -> None:
    """Log a clear warning for NUL-filled JSON objects.

    Args:
        logger: Logger instance, e.g. logging.getLogger("raw-to-bronze").
        path_str: File path, e.g. "s3a://.../_manifest.json".
        payload: Raw bytes, e.g. b"\x00\x00".
        source: Reader name, e.g. "hadoop".
    """
    size = len(payload)
    kind = _json_kind(path_str)
    logger.warning(
        "NUL-filled %s JSON via %s at %s (size=%s)",
        kind,
        source,
        path_str,
        size,
    )

# --- Metadata validation helpers ---


def partition_info(manifest: dict) -> tuple[str, str]:
    """Return (partition_kind, partition_value) from a manifest.

    Args:
        manifest: Manifest dict, e.g. {"snapshot_date": "2026-01-24"}.
    """
    if "snapshot_date" in manifest:
        return "snapshot_date", manifest["snapshot_date"]
    if "extract_date" in manifest:
        return "extract_date", manifest["extract_date"]
    if "event_date" in manifest:
        return "event_date", manifest["event_date"]
    return "", ""


def manifest_ok(manifest: dict) -> tuple[bool, str]:
    """Validate a landing manifest and return (ok, reason).

    This enforces required fields, consistent file counts, and partition metadata.

    Args:
        manifest: Parsed manifest dict, e.g. {"batch_type": "snapshot", ...}.
    """
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

    partition_kind, _ = partition_info(manifest)
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


def load_registry_table(
    spark: SparkSession, path_str: str
) -> Optional[DataFrame]:
    """Load a Delta registry table when the path is a Delta table.

    Args:
        spark: Active SparkSession, e.g. SparkSession.builder.getOrCreate().
        path_str: Table path, e.g. "s3a://ampere-bronze/bronze/ops/bronze_apply_registry".
    """
    try:
        from delta.tables import DeltaTable
    except Exception:
        return None
    if DeltaTable.isDeltaTable(spark, path_str):
        return spark.read.format("delta").load(path_str)
    return None
