"""Unity Catalog helpers for raw-to-bronze Spark jobs.

This module keeps UC-specific logic isolated so path-based ETL can stay
unchanged and UC enablement can be toggled with runtime flags.
"""

from __future__ import annotations

import logging

from pyspark.sql import SparkSession


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
    fq_table = uc_table_name(catalog, schema, table)
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {fq_table} USING DELTA LOCATION '{location}'"
    )
    logger.info("Synced UC table %s -> %s", f"{catalog}.{schema}.{table}", location)
