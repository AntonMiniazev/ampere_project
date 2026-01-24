"""Shared ETL helpers for Spark + MinIO IO, pathing, and JSON parsing."""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import date, datetime
from typing import Optional

from pyspark.sql import SparkSession


def setup_logging(level: str | None = None) -> None:
    """Configure root logging using LOG_LEVEL or the provided level."""
    resolved = level or os.getenv("LOG_LEVEL", "INFO")
    logging.basicConfig(
        level=resolved,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
        force=True,
    )


def get_env(name: str, default: Optional[str] = None) -> Optional[str]:
    """Return environment variable or default; empty strings are treated as missing."""
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return value


def parse_date(value: str) -> str:
    """Validate a YYYY-MM-DD date string and return it unchanged."""
    datetime.strptime(value, "%Y-%m-%d")
    return value


def parse_optional_datetime(value: str) -> Optional[datetime]:
    """Parse ISO-8601 or YYYY-MM-DD into datetime; returns None for blank input."""
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


def configure_s3(
    spark: SparkSession,
    endpoint: str,
    access_key: str,
    secret_key: str,
) -> None:
    """Configure Spark's Hadoop S3A settings for MinIO or S3-compatible storage."""
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


def s3_path(bucket: str, *parts: str) -> str:
    """Build an s3a:// path from a bucket and optional path parts."""
    cleaned = [part.strip("/") for part in parts if part]
    if not cleaned:
        return f"s3a://{bucket}"
    return f"s3a://{bucket}/" + "/".join(cleaned)


def table_base_path(bucket: str, prefix: str, schema: str, table: str) -> str:
    """Return the base path for a table under bucket/prefix/schema."""
    prefix = prefix.strip("/")
    if prefix:
        return s3_path(bucket, prefix, schema, table)
    return s3_path(bucket, schema, table)


def state_path(bucket: str, source_system: str, schema: str, table: str) -> str:
    """Return the landing _state JSON path for a given table."""
    return s3_path(
        bucket, source_system, schema, "_state", f"_state_{table}.json"
    )


def bronze_registry_path(bucket: str, prefix: str) -> str:
    """Return the Delta registry table path for Bronze apply tracking."""
    prefix = prefix.strip("/")
    if prefix:
        return s3_path(bucket, prefix, "ops", "bronze_apply_registry")
    return s3_path(bucket, "ops", "bronze_apply_registry")


def list_dirs(spark: SparkSession, path_str: str) -> list[str]:
    """List child directory names under a Hadoop FS path."""
    jvm = spark._jvm
    path = jvm.org.apache.hadoop.fs.Path(path_str)
    fs = path.getFileSystem(spark._jsc.hadoopConfiguration())
    if not fs.exists(path):
        return []
    statuses = fs.listStatus(path)
    return [status.getPath().getName() for status in statuses if status.isDirectory()]


def exists(spark: SparkSession, path_str: str) -> bool:
    """Check whether a Hadoop FS path exists."""
    jvm = spark._jvm
    path = jvm.org.apache.hadoop.fs.Path(path_str)
    fs = path.getFileSystem(spark._jsc.hadoopConfiguration())
    return fs.exists(path)


def write_bytes(spark: SparkSession, path_str: str, content: bytes) -> None:
    """Write raw bytes to a Hadoop FS path, overwriting existing data."""
    jvm = spark._jvm
    path = jvm.org.apache.hadoop.fs.Path(path_str)
    fs = path.getFileSystem(spark._jsc.hadoopConfiguration())
    output_stream = fs.create(path, True)
    output_stream.write(bytearray(content))
    output_stream.close()


def write_marker(
    spark: SparkSession, output_path: str, filename: str, content: bytes
) -> None:
    """Write a marker file within an output directory."""
    path_str = output_path.rstrip("/") + "/" + filename
    write_bytes(spark, path_str, content)


def read_json(
    spark: SparkSession,
    path_str: str,
    logger: Optional[logging.Logger] = None,
) -> Optional[dict]:
    """Read JSON from storage with Hadoop/Spark/boto3 fallbacks."""
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
    """Read raw bytes via Hadoop FS; returns None if the file does not exist."""
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
    head_bytes = None
    if file_size and file_size > 0:
        head_size = min(2048, int(file_size))
        head_bytes = bytearray(head_size)
        read_head = input_stream.read(head_bytes)
        if read_head > 0:
            head_bytes = head_bytes[:read_head]
        else:
            head_bytes = bytearray()
        if head_bytes and _is_all_nulls(bytes(head_bytes)):
            input_stream.close()
            return bytes(head_bytes)
    data = bytearray()
    buffer = jvm.java.nio.ByteBuffer.allocate(8192)
    while True:
        read_bytes = input_stream.read(buffer.array())
        if read_bytes <= 0:
            break
        chunk = buffer.array()[:read_bytes]
        data.extend(chunk)
    input_stream.close()
    if head_bytes:
        data = head_bytes + data
    return bytes(data)


def _read_bytes_spark(
    spark: SparkSession,
    path_str: str,
    logger: logging.Logger,
) -> Optional[bytes]:
    """Read raw bytes via Spark whole-text read; returns None on failure."""
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
    """Read raw bytes from s3a:// paths using boto3."""
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
    """Normalize payloads by stripping null bytes, BOM, and whitespace."""
    text = payload.decode("utf-8", errors="replace")
    return text.replace("\x00", "").lstrip("\ufeff").strip()


def _is_all_nulls(payload: bytes) -> bool:
    if not payload:
        return False
    return payload.count(b"\x00") == len(payload)


def _json_kind(path_str: str) -> str:
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
    """Log diagnostics for payloads that become empty after cleanup."""
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
    """Log a clear warning for NUL-filled JSON objects."""
    size = len(payload)
    kind = _json_kind(path_str)
    logger.warning(
        "NUL-filled %s JSON via %s at %s (size=%s)",
        kind,
        source,
        path_str,
        size,
    )
