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
        default=None,
        help=(
            "Deprecated compatibility fallback. Days to retain before "
            "maintenance VACUUM when --maintenance-vacuum-retention-hours "
            "is not supplied."
        ),
    )
    parser.add_argument(
        "--maintenance-vacuum-retention-hours",
        type=int,
        default=None,
        help="VACUUM retention for non-snapshot Bronze tables. Default 24.",
    )
    parser.add_argument(
        "--snapshot-vacuum-retention-hours",
        type=int,
        default=0,
        help="VACUUM retention for snapshot tables after DELETE. Default 0.",
    )
    parser.add_argument(
        "--optimize-min-files",
        type=int,
        default=32,
        help="Run OPTIMIZE only when a maintenance table has at least this many active files.",
    )
    parser.add_argument(
        "--optimize-target-min-file-mb",
        type=float,
        default=64.0,
        help="Run OPTIMIZE only when average active file size is below this threshold.",
    )
    parser.add_argument(
        "--skip-optimize",
        action="store_true",
        help="Run VACUUM only and skip maintenance-table OPTIMIZE.",
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


def _table_metrics(spark: SparkSession, fqtn: str) -> dict[str, float | int]:
    """Return compact active Delta metrics from DESCRIBE DETAIL."""
    row = spark.sql(f"DESCRIBE DETAIL {fqtn}").collect()[0].asDict()
    active_files = int(row.get("numFiles") or 0)
    active_size_mb = round(int(row.get("sizeInBytes") or 0) / 1024 / 1024, 3)
    avg_active_file_mb = (
        round(active_size_mb / active_files, 3) if active_files else 0.0
    )
    return {
        "active_files": active_files,
        "active_size_mb": active_size_mb,
        "avg_active_file_mb": avg_active_file_mb,
    }


def _log_table_metrics(
    spark: SparkSession,
    fqtn: str,
    label: str,
    logger: logging.Logger,
) -> dict[str, float | int]:
    """Log and return current active Delta metrics for one table."""
    metrics = _table_metrics(spark, fqtn)
    logger.info(
        "%s metrics for %s: active_files=%s active_size_mb=%s avg_active_file_mb=%s",
        label,
        fqtn,
        metrics["active_files"],
        metrics["active_size_mb"],
        metrics["avg_active_file_mb"],
    )
    return metrics


def _vacuum_table(
    spark: SparkSession,
    fqtn: str,
    retention_hours: int,
    logger: logging.Logger,
) -> None:
    """Run VACUUM with consistent logging."""
    logger.info("Vacuuming %s with RETAIN %s HOURS", fqtn, retention_hours)
    rows = spark.sql(f"VACUUM {fqtn} RETAIN {retention_hours} HOURS").collect()
    logger.info("VACUUM completed for %s; returned_rows=%s", fqtn, len(rows))


def _should_optimize(
    metrics: dict[str, float | int],
    *,
    optimize_min_files: int,
    optimize_target_min_file_mb: float,
) -> bool:
    """Decide whether OPTIMIZE is useful for current active file layout."""
    active_files = int(metrics["active_files"])
    avg_file_mb = float(metrics["avg_active_file_mb"])
    if optimize_min_files <= 0:
        return active_files > 1
    return (
        active_files >= optimize_min_files and avg_file_mb < optimize_target_min_file_mb
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
        _log_table_metrics(spark, fqtn, "before snapshot vacuum", logger)
        _vacuum_table(spark, fqtn, retention_hours, logger)
        _log_table_metrics(spark, fqtn, "after snapshot vacuum", logger)


def _maintain_tables(
    spark: SparkSession,
    *,
    catalog: str,
    schema: str,
    tables: list[str],
    retention_hours: int,
    optimize_min_files: int,
    optimize_target_min_file_mb: float,
    skip_optimize: bool,
    logger: logging.Logger,
) -> None:
    """Vacuum and conditionally optimize non-snapshot Bronze tables."""
    for table in tables:
        fqtn = _table_name(catalog, schema, table)
        _describe_columns(spark, fqtn)
        before_metrics = _log_table_metrics(
            spark, fqtn, "before maintenance vacuum", logger
        )
        _vacuum_table(spark, fqtn, retention_hours, logger)
        after_vacuum_metrics = _log_table_metrics(
            spark, fqtn, "after maintenance vacuum", logger
        )
        if skip_optimize:
            logger.info("Skipping OPTIMIZE for %s because --skip-optimize is set", fqtn)
            continue
        if not _should_optimize(
            after_vacuum_metrics,
            optimize_min_files=optimize_min_files,
            optimize_target_min_file_mb=optimize_target_min_file_mb,
        ):
            logger.info(
                "Skipping OPTIMIZE for %s; active_files=%s avg_active_file_mb=%s "
                "thresholds min_files=%s target_min_file_mb=%s",
                fqtn,
                after_vacuum_metrics["active_files"],
                after_vacuum_metrics["avg_active_file_mb"],
                optimize_min_files,
                optimize_target_min_file_mb,
            )
            continue

        logger.info(
            "Optimizing %s; before_active_files=%s before_active_size_mb=%s",
            fqtn,
            before_metrics["active_files"],
            before_metrics["active_size_mb"],
        )
        spark.sql(f"OPTIMIZE {fqtn}").collect()
        _log_table_metrics(spark, fqtn, "after optimize", logger)
        _vacuum_table(spark, fqtn, retention_hours, logger)
        _log_table_metrics(spark, fqtn, "after post-optimize vacuum", logger)


def main() -> None:
    """Connect to Spark Connect and execute the requested cleanup table chunk."""
    setup_logging()
    logger = logging.getLogger(APP_NAME)
    args = _parse_args()
    run_date = date.fromisoformat(args.run_date)
    if args.maintenance_vacuum_retention_hours is None:
        retention_days = max(int(args.retention_days or 1), 1)
        maintenance_vacuum_retention_hours = retention_days * 24
    else:
        maintenance_vacuum_retention_hours = max(
            int(args.maintenance_vacuum_retention_hours),
            0,
        )
        retention_days = max(maintenance_vacuum_retention_hours // 24, 1)
    cutoff = run_date - timedelta(days=retention_days)
    snapshot_vacuum_retention_hours = max(
        int(args.snapshot_vacuum_retention_hours),
        0,
    )
    snapshot_tables = parse_table_list(args.snapshot_tables)
    maintenance_tables = parse_table_list(args.maintenance_tables)

    if not snapshot_tables and not maintenance_tables:
        raise ValueError(
            "At least one of --snapshot-tables or --maintenance-tables is required."
        )

    logger.info(
        "Starting Bronze cleanup via %s for run_date=%s cutoff=%s "
        "maintenance_vacuum_retention_hours=%s snapshot_vacuum_retention_hours=%s",
        args.spark_remote,
        run_date,
        cutoff,
        maintenance_vacuum_retention_hours,
        snapshot_vacuum_retention_hours,
    )
    spark = (
        SparkSession.builder.remote(args.spark_remote).appName(APP_NAME).getOrCreate()
    )
    try:
        low_retention_values = [
            value
            for value in (
                snapshot_vacuum_retention_hours if snapshot_tables else None,
                maintenance_vacuum_retention_hours if maintenance_tables else None,
            )
            if value is not None and value < 168
        ]
        if low_retention_values:
            spark.conf.set(
                "spark.databricks.delta.retentionDurationCheck.enabled",
                "false",
            )
            logger.warning(
                "Disabled Delta retention duration check for low-retention "
                "VACUUM values=%s. This can remove files needed for time "
                "travel or concurrent readers.",
                low_retention_values,
            )
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
                retention_hours=snapshot_vacuum_retention_hours,
                logger=logger,
            )
        if maintenance_tables:
            _maintain_tables(
                spark,
                catalog=args.uc_catalog,
                schema=args.uc_bronze_schema,
                tables=maintenance_tables,
                retention_hours=maintenance_vacuum_retention_hours,
                optimize_min_files=max(int(args.optimize_min_files), 0),
                optimize_target_min_file_mb=max(
                    float(args.optimize_target_min_file_mb),
                    0.0,
                ),
                skip_optimize=bool(args.skip_optimize),
                logger=logger,
            )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
