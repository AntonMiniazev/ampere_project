"""Initialize the bronze apply registry Delta table with a fast S3 existence check."""

import argparse
import json
import logging
from pathlib import Path
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from etl_utils import (
    bronze_registry_path,
    configure_s3,
    get_env,
    set_spark_log_level,
    setup_logging,
)

try:
    from delta.tables import DeltaTable
except ImportError:
    DeltaTable = None

APP_NAME = "bronze-registry-init"


def _parse_args() -> argparse.Namespace:
    """Parse CLI args for registry initialization.

    Example CLI inputs:
        --bronze-bucket ampere-bronze
        --bronze-prefix bronze
        --app-name bronze-registry-init
    """
    parser = argparse.ArgumentParser(
        description="Initialize bronze apply registry Delta table."
    )
    parser.add_argument("--bronze-bucket", default="ampere-bronze")
    parser.add_argument("--bronze-prefix", default="bronze")
    parser.add_argument("--app-name", default=APP_NAME)
    return parser.parse_args()


def _load_registry_schema(schema_path: str) -> StructType:
    """Load the registry schema JSON template into a Spark StructType.

    Args:
        schema_path: JSON schema path, e.g. "/opt/spark/app/bronze_apply_registry_schema.json".
    """
    data = json.loads(Path(schema_path).read_text())
    type_map = {"string": StringType(), "int": IntegerType()}
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


def _parse_s3a_path(path_str: str) -> tuple[str, str]:
    """Split an s3a://bucket/key path into bucket and key prefix.

    Args:
        path_str: S3A path, e.g. "s3a://ampere-bronze/bronze/ops/bronze_apply_registry".
    """
    if not path_str.startswith("s3a://"):
        raise ValueError(f"Expected s3a:// path, got {path_str}")
    bucket_key = path_str[len("s3a://") :]
    bucket, _, key = bucket_key.partition("/")
    if not bucket:
        raise ValueError(f"Missing bucket in path {path_str}")
    return bucket, key


def _delta_log_exists(
    registry_path: str,
    endpoint: str,
    access_key: str,
    secret_key: str,
    logger: logging.Logger,
) -> Optional[bool]:
    """Check for _delta_log objects via boto3; returns None if unavailable.

    Args:
        registry_path: Registry path, e.g. "s3a://ampere-bronze/bronze/ops/bronze_apply_registry".
        endpoint: MinIO endpoint, e.g. "http://minio.ampere.svc.cluster.local:9000".
        access_key: Access key ID, e.g. "minioadmin".
        secret_key: Secret access key, e.g. "minioadmin".
        logger: Logger instance, e.g. logging.getLogger("bronze-registry-init").
    """
    try:
        import boto3
        from botocore.client import Config
    except Exception as exc:  # noqa: BLE001
        logger.warning("boto3 unavailable for registry check: %s", exc)
        return None

    bucket, key_prefix = _parse_s3a_path(registry_path)
    delta_prefix = key_prefix.strip("/")
    if delta_prefix:
        delta_prefix = f"{delta_prefix}/_delta_log/"
    else:
        delta_prefix = "_delta_log/"

    try:
        client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(signature_version="s3v4"),
            region_name="us-east-1",
        )
        response = client.list_objects_v2(
            Bucket=bucket,
            Prefix=delta_prefix,
            MaxKeys=1,
        )
        return bool(response.get("Contents"))
    except Exception as exc:  # noqa: BLE001
        logger.warning("Registry existence check failed: %s", exc)
        return None


def _try_create_registry_without_spark(
    registry_path: str,
    registry_schema: StructType,
    endpoint: str,
    access_key: str,
    secret_key: str,
    logger: logging.Logger,
) -> bool:
    """Attempt to create an empty Delta table using delta-rs if available.

    Args:
        registry_path: Registry path, e.g. "s3a://ampere-bronze/bronze/ops/bronze_apply_registry".
        registry_schema: StructType schema, e.g. StructType([...]).
        endpoint: MinIO endpoint, e.g. "http://minio.ampere.svc.cluster.local:9000".
        access_key: Access key ID, e.g. "minioadmin".
        secret_key: Secret access key, e.g. "minioadmin".
        logger: Logger instance, e.g. logging.getLogger("bronze-registry-init").
    """
    try:
        import pyarrow as pa
        from deltalake import write_deltalake
    except Exception as exc:  # noqa: BLE001
        logger.info("delta-rs not available for fast init: %s", exc)
        return False

    arrow_fields = []
    arrays = []
    for field in registry_schema.fields:
        if isinstance(field.dataType, StringType):
            arrow_type = pa.string()
        elif isinstance(field.dataType, IntegerType):
            arrow_type = pa.int32()
        else:
            logger.warning(
                "Unsupported field type for delta-rs init: %s",
                field.dataType,
            )
            return False
        arrow_fields.append(pa.field(field.name, arrow_type, nullable=field.nullable))
        arrays.append(pa.array([], type=arrow_type))

    table = pa.Table.from_arrays(arrays, schema=pa.schema(arrow_fields))
    storage_options = {
        "AWS_ACCESS_KEY_ID": access_key,
        "AWS_SECRET_ACCESS_KEY": secret_key,
        "AWS_ENDPOINT_URL": endpoint,
        "AWS_REGION": "us-east-1",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }
    if endpoint.startswith("http://"):
        storage_options["AWS_ALLOW_HTTP"] = "true"

    uri = registry_path.replace("s3a://", "s3://", 1)
    try:
        write_deltalake(
            uri,
            table,
            mode="overwrite",
            partition_by=["source_table"],
            storage_options=storage_options,
        )
        logger.info("Registry table created without Spark: %s", registry_path)
        return True
    except Exception as exc:  # noqa: BLE001
        logger.warning("delta-rs registry init failed: %s", exc)
        return False


def main() -> None:
    """Initialize the bronze registry table, avoiding Spark when possible."""
    # Step 1: Initialize logging and parse CLI arguments.
    # This keeps the driver output consistent and captures the registry inputs early.
    # The expected outcome is a fully populated args object before touching storage.
    setup_logging()
    logger = logging.getLogger(APP_NAME)

    args = _parse_args()

    # Step 2: Resolve MinIO credentials and registry schema metadata.
    # These values must be present to check existence and optionally create the table.
    # The expected outcome is a valid endpoint, credentials, and schema path.
    minio_endpoint = get_env(
        "MINIO_S3_ENDPOINT", "http://minio.ampere.svc.cluster.local:9000"
    )
    minio_access_key = get_env("MINIO_ACCESS_KEY")
    minio_secret_key = get_env("MINIO_SECRET_KEY")
    if not minio_access_key or not minio_secret_key:
        raise ValueError("Missing MINIO_ACCESS_KEY/MINIO_SECRET_KEY for MinIO.")

    registry_path = bronze_registry_path(args.bronze_bucket, args.bronze_prefix)
    schema_path = get_env(
        "BRONZE_REGISTRY_SCHEMA_PATH",
        str(Path(__file__).with_name("bronze_apply_registry_schema.json")),
    )
    registry_schema = _load_registry_schema(schema_path)

    # Step 3: Fast existence check against the Delta log without Spark.
    # This avoids driver startup cost when the registry already exists.
    # The expected outcome is a boolean or None when the check cannot be performed.
    # Fast path: check for _delta_log without Spark.
    exists = _delta_log_exists(
        registry_path,
        minio_endpoint,
        minio_access_key,
        minio_secret_key,
        logger,
    )
    if exists:
        logger.info("Registry table already exists: %s", registry_path)
        return

    # Step 4: Attempt a fast delta-rs init when the registry is missing.
    # This writes an empty Delta table directly to storage for quick bootstrap.
    # The expected outcome is a successful creation without Spark when libraries exist.
    if exists is False:
        # Try to create with delta-rs for a faster init when available.
        if _try_create_registry_without_spark(
            registry_path,
            registry_schema,
            minio_endpoint,
            minio_access_key,
            minio_secret_key,
            logger,
        ):
            return

    # Step 5: Fall back to Spark-based initialization when needed.
    # This guarantees registry creation even when delta-rs or boto3 are unavailable.
    # The expected outcome is an empty Delta table partitioned by source_table.
    if DeltaTable is None:
        raise ImportError("delta-spark is required for Spark-based registry init.")

    spark = (
        SparkSession.builder.appName(args.app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    configure_s3(spark, minio_endpoint, minio_access_key, minio_secret_key)
    set_spark_log_level(spark)

    if DeltaTable.isDeltaTable(spark, registry_path):
        logger.info("Registry table already exists: %s", registry_path)
        spark.stop()
        return

    logger.info("Creating registry table with Spark: %s", registry_path)
    empty_df = spark.createDataFrame([], schema=registry_schema)
    empty_df.write.format("delta").mode("overwrite").partitionBy("source_table").save(
        registry_path
    )
    logger.info("Registry table created: %s", registry_path)
    spark.stop()


if __name__ == "__main__":
    main()
