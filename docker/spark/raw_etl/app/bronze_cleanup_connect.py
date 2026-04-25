"""Run Bronze Delta cleanup through the cluster Spark Connect service."""

from __future__ import annotations

import argparse
import logging
from datetime import date, timedelta

from pyspark.sql import SparkSession

from etl_utils import parse_date, parse_table_list, setup_logging

APP_NAME = "bronze-cleanup-connect"
SNAPSHOT_PARTITION_COLUMN = "snapshot_date"


def _quote_ident(value: str) -> str:
    """Return a backtick-quoted Spark SQL identifier."""
    return "`" + value.replace("`", "``") + "`"


def _table_name(catalog: str, schema: str, table: str) -> str:
    """Build a fully qualified UC table name for Spark SQL."""
    return ".".join((_quote_ident(catalog), _quote_ident(schema), _quote_ident(table)))


def _parse_args() -> argparse.Namespace:
    """Parse Bronze cleanup CLI arguments passed by the Airflow pod."""
    parser = argparse.ArgumentParser(
        description="Clean Bronze Delta tables through Spark Connect."
    )
    parser.add_argument(
        "--spark-remote",
        required=True,
        help="Spark Connect remote URI, for example sc://spark-connect:15002.",
    )
    parser.add_argument("--uc-catalog", required=True, help="UC catalog name.")
    parser.add_argument("--uc-bronze-schema", required=True, help="UC Bronze schema.")
    parser.add_argument(
        "--run-date",
        type=parse_date,
        required=True,
        help="Business date in YYYY-MM-DD form.",
    )
    parser.add_argument(
        "--retention-days",
        type=int,
        default=7,
        help="Days to retain before DELETE/VACUUM. Default 7.",
    )
    parser.add_argument(
        "--snapshot-tables",
        default="",
        help="Comma-separated snapshot table names.",
    )
    parser.add_argument(
        "--maintenance-tables",
        default="",
        help="Comma-separated mutable-dim/fact/event table names.",
    )
    return parser.parse_args()


def _describe_columns(spark: SparkSession, fqtn: str) -> set[str]:
    """Read Spark table columns and return the visible column names."""
    rows = spark.sql(f"DESCRIBE TABLE {fqtn}").collect()
    return {
        str(row.col_name)
        for row in rows
        if row.col_name and not str(row.col_name).startswith("#")
    }


def _registered_uc_tables(
    spark: SparkSession,
    *,
    catalog: str,
    schema: str,
) -> set[str]:
    """Return table names registered in the configured UC schema."""
    rows = spark.sql(
        f"SHOW TABLES IN {_quote_ident(catalog)}.{_quote_ident(schema)}"
    ).collect()
    return {str(row.tableName) for row in rows if row.tableName}


def _validate_uc_tables(
    spark: SparkSession,
    *,
    catalog: str,
    schema: str,
    tables: list[str],
    logger: logging.Logger,
) -> None:
    """Fail before cleanup if expected Bronze tables are absent from UC."""
    registered = _registered_uc_tables(spark, catalog=catalog, schema=schema)
    missing = [table for table in tables if table not in registered]
    if missing:
        raise ValueError(
            f"Bronze cleanup expected UC tables missing from "
            f"{catalog}.{schema}: {missing}"
        )
    logger.info(
        "Validated %s cleanup tables in UC schema %s.%s",
        len(tables),
        catalog,
        schema,
    )


def _delete_old_snapshots(
    spark: SparkSession,
    *,
    catalog: str,
    schema: str,
    tables: list[str],
    cutoff: date,
    retention_hours: int,
    logger: logging.Logger,
) -> None:
    """Delete old snapshot partitions and vacuum removed Delta files."""
    for table in tables:
        fqtn = _table_name(catalog, schema, table)
        columns = _describe_columns(spark, fqtn)
        if SNAPSHOT_PARTITION_COLUMN not in columns:
            raise ValueError(
                f"{fqtn} does not contain required {SNAPSHOT_PARTITION_COLUMN!r} column."
            )
        logger.info("Deleting %s rows where snapshot_date < %s", fqtn, cutoff)
        spark.sql(
            f"DELETE FROM {fqtn} "
            f"WHERE {SNAPSHOT_PARTITION_COLUMN} < DATE '{cutoff.isoformat()}'"
        ).collect()
        logger.info("Vacuuming deleted snapshot files for %s", fqtn)
        spark.sql(f"VACUUM {fqtn} RETAIN {retention_hours} HOURS").collect()


def _optimize_and_vacuum(
    spark: SparkSession,
    *,
    catalog: str,
    schema: str,
    tables: list[str],
    retention_hours: int,
    logger: logging.Logger,
) -> None:
    """Run OPTIMIZE followed by VACUUM for non-snapshot Bronze tables."""
    for table in tables:
        fqtn = _table_name(catalog, schema, table)
        _describe_columns(spark, fqtn)
        logger.info("Optimizing %s", fqtn)
        spark.sql(f"OPTIMIZE {fqtn}").collect()
        logger.info("Vacuuming %s with RETAIN %s HOURS", fqtn, retention_hours)
        spark.sql(f"VACUUM {fqtn} RETAIN {retention_hours} HOURS").collect()


def main() -> None:
    """Connect to Spark Connect and execute the requested cleanup table chunk."""
    setup_logging()
    logger = logging.getLogger(APP_NAME)
    args = _parse_args()
    run_date = date.fromisoformat(args.run_date)
    retention_days = max(int(args.retention_days), 1)
    cutoff = run_date - timedelta(days=retention_days)
    retention_hours = retention_days * 24
    snapshot_tables = parse_table_list(args.snapshot_tables)
    maintenance_tables = parse_table_list(args.maintenance_tables)

    if not snapshot_tables and not maintenance_tables:
        raise ValueError(
            "At least one of --snapshot-tables or --maintenance-tables is required."
        )

    logger.info(
        "Starting Bronze cleanup via %s for run_date=%s cutoff=%s",
        args.spark_remote,
        run_date,
        cutoff,
    )
    spark = (
        SparkSession.builder.remote(args.spark_remote)
        .appName(APP_NAME)
        .getOrCreate()
    )
    try:
        _validate_uc_tables(
            spark,
            catalog=args.uc_catalog,
            schema=args.uc_bronze_schema,
            tables=snapshot_tables + maintenance_tables,
            logger=logger,
        )
        if snapshot_tables:
            _delete_old_snapshots(
                spark,
                catalog=args.uc_catalog,
                schema=args.uc_bronze_schema,
                tables=snapshot_tables,
                cutoff=cutoff,
                retention_hours=retention_hours,
                logger=logger,
            )
        if maintenance_tables:
            _optimize_and_vacuum(
                spark,
                catalog=args.uc_catalog,
                schema=args.uc_bronze_schema,
                tables=maintenance_tables,
                retention_hours=retention_hours,
                logger=logger,
            )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
