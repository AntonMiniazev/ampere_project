"""Create all empty Ampere Iceberg tables from the canonical v3 contract."""

from __future__ import annotations

import logging
import os

from pyspark.sql import SparkSession

from tools.contracts.ampere_contract import AmpereContract, ResolvedTable, load_contract
from iceberg_bronze.catalog import quote_ident
from tools.contracts.spark_conformance import validate_spark_table


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


def initialize(spark_remote: str | None = None) -> None:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
    logger = logging.getLogger("iceberg-catalog-init")
    contract = load_contract()
    logger.info("Initializing %s contract v%s (%s tables)", contract.catalog_namespace, contract.version, len(contract.tables))
    spark_remote = spark_remote or os.getenv(
        "SPARK_REMOTE", "sc://spark-connect.ampere.svc.cluster.local:15002"
    )
    spark = SparkSession.builder.remote(spark_remote).appName(
        "ampere-iceberg-catalog-init"
    ).getOrCreate()
    try:
        logger.info("Using existing Spark Connect session at %s", spark_remote)
        for layer in ("bronze", "silver", "gold"):
            for table in contract.layer_tables(layer):
                _create_table(spark, contract, table)
        logger.info("Initialized and validated all %s contract tables", len(contract.tables))
    finally:
        spark.stop()


if __name__ == "__main__":
    initialize()
