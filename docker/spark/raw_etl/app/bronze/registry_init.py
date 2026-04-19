"""Ensure the bronze apply registry exists as a UC-managed external Delta table."""

import argparse
import logging
from pathlib import Path

from pyspark.sql import SparkSession

from etl_utils import configure_s3, get_env, set_spark_log_level, setup_logging
from bronze.apply_utils import load_registry_schema
from bronze.uc import (
    ensure_uc_external_delta_table,
    ensure_uc_schema,
    get_uc_table_payload,
    parse_bool_flag,
    uc_table_name,
)

APP_NAME = "bronze-registry-init"


def _parse_args() -> argparse.Namespace:
    """Parse CLI args for registry bootstrap and validation."""
    parser = argparse.ArgumentParser(
        description="Ensure bronze apply registry Delta table exists in Unity Catalog."
    )
    parser.add_argument("--uc-enabled", default="true")
    parser.add_argument("--uc-catalog", default="")
    parser.add_argument("--uc-ops-schema", default="ops")
    parser.add_argument("--registry-location", default="")
    parser.add_argument("--app-name", default=APP_NAME)
    return parser.parse_args()


def _to_uc_s3_path(path_str: str) -> str:
    """Normalize storage paths to s3:// for Unity Catalog API compatibility."""
    if path_str.startswith("s3a://"):
        return "s3://" + path_str[len("s3a://") :]
    return path_str


def _bootstrap_registry_storage(
    spark: SparkSession,
    table_name: str,
    registry_location: str,
) -> None:
    """Create an empty Delta registry table in storage and register it in UC."""
    schema_path = get_env(
        "BRONZE_REGISTRY_SCHEMA_PATH",
        str(Path(__file__).with_name("bronze_apply_registry_schema.json")),
    )
    registry_schema = load_registry_schema(schema_path)
    empty_df = spark.createDataFrame([], schema=registry_schema)
    uc_location = _to_uc_s3_path(registry_location)
    (
        empty_df.write.format("delta")
        .mode("overwrite")
        .option("path", uc_location)
        .partitionBy("source_table")
        .saveAsTable(table_name)
    )


def _repair_registry_storage(
    spark: SparkSession,
    registry_location: str,
) -> None:
    """Recreate an empty Delta log at the existing UC registry storage location."""
    schema_path = get_env(
        "BRONZE_REGISTRY_SCHEMA_PATH",
        str(Path(__file__).with_name("bronze_apply_registry_schema.json")),
    )
    registry_schema = load_registry_schema(schema_path)
    empty_df = spark.createDataFrame([], schema=registry_schema)
    uc_location = _to_uc_s3_path(registry_location)
    (
        empty_df.write.format("delta")
        .mode("overwrite")
        .partitionBy("source_table")
        .save(uc_location)
    )


def _registry_table_exists(
    spark: SparkSession,
    registry_table_name: str,
    logger: logging.Logger,
) -> bool:
    """Return True when registry table exists, tolerating broken Delta path metadata.

    In UC mode a table entry may exist while its underlying Delta path is missing.
    Spark can raise AnalysisException during tableExists() in that case.
    """
    try:
        return bool(spark.catalog.tableExists(registry_table_name))
    except Exception as exc:  # noqa: BLE001
        message = str(exc)
        if "DELTA_PATH_DOES_NOT_EXIST" in message or "doesn't exist" in message:
            logger.warning(
                "Registry table metadata exists but Delta path is missing; treating as absent: %s",
                registry_table_name,
            )
            return False
        raise


def main() -> None:
    """Ensure the bronze registry table exists and is readable from Unity Catalog."""
    setup_logging()
    logger = logging.getLogger(APP_NAME)

    args = _parse_args()
    uc_enabled = parse_bool_flag(args.uc_enabled, default=True)
    uc_catalog = (args.uc_catalog or "").strip()
    uc_ops_schema = (args.uc_ops_schema or "ops").strip()
    registry_location = (args.registry_location or "").strip()
    if not uc_enabled:
        raise ValueError("Bronze registry init is UC-only. Set --uc-enabled=true.")
    if not uc_catalog:
        raise ValueError("--uc-catalog is required when --uc-enabled=true.")
    if not registry_location:
        raise ValueError("--registry-location is required for registry bootstrap.")

    minio_endpoint = get_env(
        "MINIO_S3_ENDPOINT", "http://minio.ampere.svc.cluster.local:9000"
    )
    minio_access_key = get_env("MINIO_ACCESS_KEY")
    minio_secret_key = get_env("MINIO_SECRET_KEY")
    if not minio_access_key or not minio_secret_key:
        raise ValueError("Missing MINIO_ACCESS_KEY/MINIO_SECRET_KEY for MinIO.")

    spark = (
        SparkSession.builder.appName(args.app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    configure_s3(spark, minio_endpoint, minio_access_key, minio_secret_key)
    set_spark_log_level(spark)
    ensure_uc_schema(spark, uc_catalog, uc_ops_schema, logger)
    registry_table_name = uc_table_name(
        uc_catalog,
        uc_ops_schema,
        "bronze_apply_registry",
    )

    if not _registry_table_exists(
        spark=spark,
        registry_table_name=registry_table_name,
        logger=logger,
    ):
        logger.warning(
            "UC registry table is missing; creating %s at %s",
            registry_table_name,
            registry_location,
        )
        _bootstrap_registry_storage(
            spark=spark,
            table_name=registry_table_name,
            registry_location=registry_location,
        )
    else:
        registry_table_name = ensure_uc_external_delta_table(
            spark=spark,
            catalog=uc_catalog,
            schema=uc_ops_schema,
            table="bronze_apply_registry",
            logger=logger,
        )
        try:
            spark.read.table(registry_table_name).limit(1).count()
        except Exception as exc:  # noqa: BLE001
            payload = get_uc_table_payload(
                spark=spark,
                catalog=uc_catalog,
                schema=uc_ops_schema,
                table="bronze_apply_registry",
                logger=logger,
            )
            existing_location = str(payload.get("storage_location") or "").strip()
            if not existing_location:
                raise RuntimeError(
                    f"UC registry table {registry_table_name} is unreadable and does not "
                    "expose a storage_location for recovery."
                ) from exc
            logger.warning(
                "UC registry table %s exists but is unreadable; recreating Delta files at %s",
                registry_table_name,
                existing_location,
            )
            _repair_registry_storage(
                spark=spark,
                registry_location=existing_location,
            )

    registry_table_name = ensure_uc_external_delta_table(
        spark=spark,
        catalog=uc_catalog,
        schema=uc_ops_schema,
        table="bronze_apply_registry",
        logger=logger,
    )
    spark.read.table(registry_table_name).limit(1).count()
    logger.info("Registry table is ready in UC: %s", registry_table_name)
    spark.stop()


if __name__ == "__main__":
    main()
