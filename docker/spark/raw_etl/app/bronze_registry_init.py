import argparse
import json
import logging
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

try:
    from delta.tables import DeltaTable
except ImportError as exc:
    raise ImportError(
        "delta-spark is required for bronze registry init."
    ) from exc

APP_NAME = "bronze-registry-init"


def setup_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=level,
        format=("%(asctime)s | %(levelname)s | %(name)s | %(message)s"),
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
        force=True,
    )


def _get_env(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return value


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Initialize bronze apply registry Delta table."
    )
    parser.add_argument("--bronze-bucket", default="ampere-bronze")
    parser.add_argument("--bronze-prefix", default="bronze")
    parser.add_argument("--app-name", default=APP_NAME)
    return parser.parse_args()


def _configure_s3(spark: SparkSession, endpoint: str, access_key: str, secret_key: str) -> None:
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


def _registry_path(bucket: str, prefix: str) -> str:
    prefix = prefix.strip("/")
    if prefix:
        return f"s3a://{bucket}/{prefix}/ops/bronze_apply_registry"
    return f"s3a://{bucket}/ops/bronze_apply_registry"


def _load_registry_schema(schema_path: str) -> StructType:
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


def main() -> None:
    setup_logging()
    logger = logging.getLogger(APP_NAME)

    args = _parse_args()

    minio_endpoint = _get_env(
        "MINIO_S3_ENDPOINT", "http://minio.ampere.svc.cluster.local:9000"
    )
    minio_access_key = _get_env("MINIO_ACCESS_KEY")
    minio_secret_key = _get_env("MINIO_SECRET_KEY")
    if not minio_access_key or not minio_secret_key:
        raise ValueError("Missing MINIO_ACCESS_KEY/MINIO_SECRET_KEY for MinIO.")

    spark = (
        SparkSession.builder.appName(args.app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    _configure_s3(spark, minio_endpoint, minio_access_key, minio_secret_key)

    registry_path = _registry_path(args.bronze_bucket, args.bronze_prefix)
    schema_path = _get_env(
        "BRONZE_REGISTRY_SCHEMA_PATH",
        str(Path(__file__).with_name("bronze_apply_registry_schema.json")),
    )
    registry_schema = _load_registry_schema(schema_path)

    if DeltaTable.isDeltaTable(spark, registry_path):
        logger.info("Registry table already exists: %s", registry_path)
        spark.stop()
        return

    logger.info("Creating registry table: %s", registry_path)
    empty_df = spark.createDataFrame([], schema=registry_schema)
    empty_df.write.format("delta").mode("overwrite").save(registry_path)
    logger.info("Registry table created: %s", registry_path)
    spark.stop()


if __name__ == "__main__":
    main()
