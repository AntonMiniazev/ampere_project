"""Unity Catalog helpers for raw-to-bronze Spark jobs.

This module keeps UC-specific logic isolated so path-based ETL can stay
unchanged and UC enablement can be toggled with runtime flags.
"""

from __future__ import annotations

import logging

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType


def parse_bool_flag(value: str | bool | None, default: bool = False) -> bool:
    """Parse a boolean-like CLI/env value.

    Examples:
        parse_bool_flag("true") -> True
        parse_bool_flag("0") -> False
        parse_bool_flag(None, default=True) -> True
    """
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False
    return default


def quote_ident(value: str) -> str:
    """Quote Spark SQL identifier safely."""
    return "`" + value.replace("`", "``") + "`"


def uc_table_name(catalog: str, schema: str, table: str) -> str:
    """Build a fully-qualified UC table name."""
    return ".".join((quote_ident(catalog), quote_ident(schema), quote_ident(table)))


def ensure_uc_schema(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
) -> None:
    """Create UC schema if missing.

    This is idempotent and safe to run on every batch job.
    """
    spark.sql(
        f"CREATE SCHEMA IF NOT EXISTS {quote_ident(catalog)}.{quote_ident(schema)}"
    )
    logger.info("Ensured UC schema exists: %s.%s", catalog, schema)


def _column_type_sql(data_type) -> str:
    """Return Spark SQL type text for a PySpark data type."""
    # `simpleString()` is accepted by Spark SQL DDL for common/complex types.
    return data_type.simpleString()


def _columns_ddl(struct: StructType) -> str:
    """Build column DDL for CREATE TABLE from a Spark StructType."""
    columns = []
    for field in struct.fields:
        nullable_suffix = "" if field.nullable else " NOT NULL"
        columns.append(
            f"{quote_ident(field.name)} {_column_type_sql(field.dataType)}{nullable_suffix}"
        )
    return ", ".join(columns)


def _table_has_columns(spark: SparkSession, fq_table: str) -> bool:
    """Best-effort check whether a registered table has visible columns."""
    try:
        described = spark.sql(f"DESCRIBE TABLE {fq_table}").collect()
    except Exception:
        return False
    for row in described:
        col_name = (row[0] or "").strip() if len(row) > 0 else ""
        if not col_name or col_name.startswith("#"):
            continue
        if col_name.lower() in {"col_name"}:
            continue
        return True
    return False


def sync_external_delta_table(
    spark: SparkSession,
    catalog: str,
    schema: str,
    table: str,
    location: str,
    logger: logging.Logger,
) -> None:
    """Register or keep UC external table pointing to Delta path.

    The `CREATE TABLE IF NOT EXISTS ... USING DELTA LOCATION ...` command is
    metadata-only for existing Delta tables, so it is cheap and idempotent.
    """
    # UC server storage mappings in this setup are configured for s3:// URLs.
    # ETL paths are built as s3a:// for Spark/Hadoop IO, so normalize here.
    uc_location = location.replace("s3a://", "s3://", 1)
    fq_table = uc_table_name(catalog, schema, table)
    table_exists = spark.catalog.tableExists(f"{catalog}.{schema}.{table}")
    if not table_exists:
        delta_schema = spark.read.format("delta").load(location).schema
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {fq_table} ({_columns_ddl(delta_schema)}) "
            f"USING DELTA LOCATION '{uc_location}'"
        )
    else:
        # Keep existing metadata when table is already registered.
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {fq_table} USING DELTA LOCATION '{uc_location}'"
        )
        if not _table_has_columns(spark, fq_table):
            logger.warning(
                "UC table %s exists but has no visible columns; consider drop/recreate "
                "to repopulate metadata.",
                f"{catalog}.{schema}.{table}",
            )
    logger.info(
        "Synced UC table %s -> %s",
        f"{catalog}.{schema}.{table}",
        uc_location,
    )
