"""Create all empty Ampere Iceberg tables from the current canonical contract."""

from __future__ import annotations

import logging
import os

from pyspark.sql import SparkSession

from tools.contracts.ampere_contract import AmpereContract, ResolvedTable, load_contract
from tools.contracts.spark_conformance import (
    quote_identifier,
    spark_type,
    validate_spark_table,
)


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _catalog_name(layer: str) -> str:
    return f"iceberg_{layer}"


def _partition_expression(spec: dict[str, str]) -> str:
    source = quote_identifier(spec["source"])
    transform = spec["transform"]
    return source if transform == "identity" else f"{transform}({source})"


def _evolve_bronze_lineage_columns(
    spark: SparkSession, target: str, table: ResolvedTable
) -> None:
    """Add the v4 lineage columns to existing v3 Bronze tables in place.

    Iceberg permits adding nullable columns without rewriting existing files.
    Limit this migration to the four known v4 lineage fields; all other schema
    drift remains a hard validation failure.
    """
    if table.layer != "bronze":
        return

    lineage_names = {
        "_dataset_id", "_source_row_id", "_source_batch_sequence", "_source_change_id"
    }
    expected = [
        column for column in table.columns if column["name"] in lineage_names
    ]
    if not expected:
        return

    observed_schema = spark.table(target).schema
    observed = {
        field.name: spark_type(field.dataType.simpleString())
        for field in observed_schema.fields
    }
    missing = []
    for column in expected:
        name = column["name"]
        expected_type = spark_type(column["type_text"])
        if name in observed:
            if observed[name] != expected_type:
                raise ValueError(
                    f"Cannot evolve {target}: lineage column {name} has type "
                    f"{observed[name]}, expected {expected_type}"
                )
        else:
            missing.append(column)

    if missing:
        additions = ", ".join(
            f"{quote_identifier(column['name'])} {column['type_text']}"
            for column in missing
        )
        spark.sql(f"ALTER TABLE {target} ADD COLUMNS ({additions})")
        logging.info(
            "Added nullable v4 lineage columns to %s: %s",
            target,
            ", ".join(column["name"] for column in missing),
        )


def _evolve_order_zone_columns(
    spark: SparkSession, target: str, table: ResolvedTable
) -> None:
    """Add the nullable v7 order-time zone fields to existing tables."""
    if (table.layer, table.name) not in {
        ("raw", "orders"),
        ("bronze", "orders"),
        ("silver", "fact_orders"),
    }:
        return

    names = {"order_zone_id", "preferred_zone_id_at_order"}
    expected = [column for column in table.columns if column["name"] in names]
    observed_schema = spark.table(target).schema
    observed = {
        field.name: spark_type(field.dataType.simpleString())
        for field in observed_schema.fields
    }
    missing = []
    for column in expected:
        name = column["name"]
        expected_type = spark_type(column["type_text"])
        if name in observed:
            if observed[name] != expected_type:
                raise ValueError(
                    f"Cannot evolve {target}: order-time column {name} has type "
                    f"{observed[name]}, expected {expected_type}"
                )
        else:
            missing.append(column)
    if missing:
        additions = ", ".join(
            f"{quote_identifier(column['name'])} {column['type_text']}"
            for column in missing
        )
        spark.sql(f"ALTER TABLE {target} ADD COLUMNS ({additions})")
        logging.info(
            "Added nullable v7 order-time columns to %s: %s",
            target,
            ", ".join(column["name"] for column in missing),
        )


def _create_table(spark: SparkSession, contract: AmpereContract, table: ResolvedTable) -> None:
    catalog = _catalog_name(table.layer)
    namespace = table.namespace
    target = f"{quote_identifier(catalog)}.{quote_identifier(namespace)}.{quote_identifier(table.name)}"
    spark.sql(
        f"CREATE NAMESPACE IF NOT EXISTS "
        f"{quote_identifier(catalog)}.{quote_identifier(namespace)}"
    )
    columns = ", ".join(
        f"{quote_identifier(column['name'])} {column['type_text']}"
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
    _evolve_bronze_lineage_columns(spark, target, table)
    _evolve_order_zone_columns(spark, target, table)
    if table.sort_order:
        ordering = ", ".join(
            f"{quote_identifier(item['source'])} {item.get('direction', 'asc').upper()}"
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


def initialize(
    spark_remote: str | None = None,
    *,
    layers: tuple[str, ...] = ("raw", "bronze", "silver", "gold"),
) -> None:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
    logger = logging.getLogger("iceberg-catalog-init")
    contract = load_contract()
    allowed_layers = {"raw", "bronze", "silver", "gold"}
    if not layers or set(layers) - allowed_layers:
        raise ValueError(f"Unsupported catalog layers: {layers}")
    logger.info("Initializing %s contract v%s (%s tables)", contract.catalog_namespace, contract.version, len(contract.tables))
    spark_remote = spark_remote or os.getenv(
        "SPARK_REMOTE", "sc://spark-connect.ampere.svc.cluster.local:15002"
    )
    spark = SparkSession.builder.remote(spark_remote).appName(
        "ampere-iceberg-catalog-init"
    ).getOrCreate()
    try:
        logger.info("Using existing Spark Connect session at %s", spark_remote)
        for layer in layers:
            for table in contract.layer_tables(layer):
                _create_table(spark, contract, table)
        initialized_count = sum(len(contract.layer_tables(layer)) for layer in layers)
        logger.info("Initialized and validated %s selected contract tables", initialized_count)
    finally:
        spark.stop()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--layers",
        nargs="+",
        choices=("raw", "bronze", "silver", "gold"),
        default=("raw", "bronze", "silver", "gold"),
    )
    args = parser.parse_args()
    initialize(layers=tuple(args.layers))
