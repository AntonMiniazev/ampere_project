"""Validate the bronze apply registry as a UC-managed external Delta table."""

import argparse
import logging

from pyspark.sql import SparkSession

from etl_utils import set_spark_log_level, setup_logging
from bronze.uc import ensure_uc_external_delta_table, ensure_uc_schema, parse_bool_flag

APP_NAME = "bronze-registry-init"


def _parse_args() -> argparse.Namespace:
    """Parse CLI args for registry validation."""
    parser = argparse.ArgumentParser(
        description="Validate bronze apply registry Delta table in Unity Catalog."
    )
    parser.add_argument("--uc-enabled", default="true")
    parser.add_argument("--uc-catalog", default="")
    parser.add_argument("--uc-ops-schema", default="ops")
    parser.add_argument("--app-name", default=APP_NAME)
    return parser.parse_args()


def main() -> None:
    """Initialize the bronze registry table in Unity Catalog."""
    setup_logging()
    logger = logging.getLogger(APP_NAME)

    args = _parse_args()
    uc_enabled = parse_bool_flag(args.uc_enabled, default=True)
    uc_catalog = (args.uc_catalog or "").strip()
    uc_ops_schema = (args.uc_ops_schema or "ops").strip()
    if not uc_enabled:
        raise ValueError("Bronze registry init is UC-only. Set --uc-enabled=true.")
    if not uc_catalog:
        raise ValueError("--uc-catalog is required when --uc-enabled=true.")

    spark = (
        SparkSession.builder.appName(args.app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    set_spark_log_level(spark)
    ensure_uc_schema(spark, uc_catalog, uc_ops_schema, logger)

    if not spark.catalog.tableExists(f"`{uc_catalog}`.`{uc_ops_schema}`.`bronze_apply_registry`"):
        raise RuntimeError(
            f"UC registry table {uc_catalog}.{uc_ops_schema}.bronze_apply_registry is "
            "missing. Pre-create it in Unity Catalog before running the bronze pipeline."
        )

    registry_table_name = ensure_uc_external_delta_table(
        spark=spark,
        catalog=uc_catalog,
        schema=uc_ops_schema,
        table="bronze_apply_registry",
        logger=logger,
    )
    logger.info("Registry table is ready in UC: %s", registry_table_name)
    spark.stop()


if __name__ == "__main__":
    main()
