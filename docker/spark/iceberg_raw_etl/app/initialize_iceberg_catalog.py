"""Create all empty Ampere Iceberg tables from the canonical v3 contract."""

from __future__ import annotations

import logging
import os

from pyspark.sql import SparkSession

from tools.contracts.ampere_contract import AmpereContract, ResolvedTable, load_contract
from iceberg_bronze.catalog import configure_lakekeeper_catalog, quote_ident
from tools.contracts.spark_conformance import validate_spark_table
from etl_utils import configure_s3


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _catalog_name(layer: str) -> str:
    return f"iceberg_{layer}"


def _partition_expression(spec: dict[str, str]) -> str:
    source = quote_ident(spec["source"])
    transform = spec["transform"]
    return source if transform == "identity" else f"{transform}({source})"


def _create_table(spark: SparkSession, contract: AmpereContract, table: ResolvedTable) -> None:
    catalog = _catalog_name(table.layer)
    namespace = table.namespace
    target = f"{quote_ident(catalog)}.{quote_ident(namespace)}.{quote_ident(table.name)}"
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {quote_ident(catalog)}.{quote_ident(namespace)}")
    columns = ", ".join(
        f"{quote_ident(column['name'])} {column['type_text']}"
        for column in table.columns
    )
    partitions = (
        " PARTITIONED BY (" + ", ".join(_partition_expression(item) for item in table.partition_spec) + ")"
        if table.partition_spec else ""
    )
    properties = {
        "format-version": str(table.format_version),
        "write.distribution-mode": table.write.get("distribution", "none"),
    }
    target_size = table.write.get("target_file_size_bytes")
    if target_size is not None:
        properties["write.target-file-size-bytes"] = str(target_size)
    properties_sql = ", ".join(
        f"{_sql_literal(key)} = {_sql_literal(value)}"
        for key, value in sorted(properties.items())
    )
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {target} ({columns}) USING iceberg"
        f"{partitions} TBLPROPERTIES ({properties_sql})"
    )
    if table.sort_order:
        ordering = ", ".join(
            f"{quote_ident(item['source'])} {item.get('direction', 'asc').upper()}"
            for item in table.sort_order
        )
        spark.sql(f"ALTER TABLE {target} WRITE ORDERED BY {ordering}")
    # CREATE IF NOT EXISTS is idempotent but does not reconcile conflicts.
    # Validate the selected layer's full spec and fail clearly on any conflict.
    try:
        validate_spark_table(spark, catalog, table)
    except Exception as exc:
        target_name = f"{catalog}.{namespace}.{table.name}"
        raise RuntimeError(
            f"Initialized catalog table {target_name} conflicts with contract v{contract.version}"
        ) from exc
    logging.info("Initialized %s.%s.%s from contract v%s", catalog, namespace, table.name, contract.version)


def initialize() -> None:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
    logger = logging.getLogger("iceberg-catalog-init")
    contract = load_contract()
    logger.info("Initializing %s contract v%s (%s tables)", contract.catalog_namespace, contract.version, len(contract.tables))
    spark = (
        SparkSession.builder.appName("ampere-iceberg-catalog-init")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.redaction.regex", "(?i)secret|password|token|credential|access.key")
        .getOrCreate()
    )
    try:
        configure_s3(
            spark,
            os.environ["MINIO_S3_ENDPOINT"],
            os.environ["MINIO_ACCESS_KEY"],
            os.environ["MINIO_SECRET_KEY"],
        )
        for layer in ("bronze", "silver", "gold"):
            configure_lakekeeper_catalog(
                spark,
                catalog=_catalog_name(layer),
                warehouse=os.environ[f"ICEBERG_{layer.upper()}_WAREHOUSE"],
                uri=os.environ["LAKEKEEPER_CATALOG_URI"],
                oauth_uri=os.environ["LAKEKEEPER_OAUTH_URI"],
                scope=os.environ["LAKEKEEPER_SCOPE"],
                client_id=os.environ["LAKEKEEPER_CLIENT_ID"],
                client_secret=os.environ["LAKEKEEPER_CLIENT_SECRET"],
                minio_endpoint=os.environ["MINIO_S3_ENDPOINT"],
            )
        for layer in ("bronze", "silver", "gold"):
            for table in contract.layer_tables(layer):
                _create_table(spark, contract, table)
        logger.info("Initialized and validated all %s contract tables", len(contract.tables))
    finally:
        spark.stop()


if __name__ == "__main__":
    initialize()
