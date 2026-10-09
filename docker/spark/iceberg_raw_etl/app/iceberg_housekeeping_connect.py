"""Compact small Iceberg files and expire old history through Spark Connect."""

from __future__ import annotations

import argparse
import logging
import re
import time
from datetime import datetime, timedelta, timezone
from threading import Event, Thread

from pyspark.sql import SparkSession


LOGGER = logging.getLogger("iceberg-housekeeping")
NAMESPACES = (
    ("iceberg_bronze", "bronze"),
    ("iceberg_bronze", "ops"),
    ("iceberg_silver", "silver"),
    ("iceberg_gold", "gold"),
)
# Limit destructive maintenance to known pipeline tables. A repair backup exists
# in Bronze and must remain untouched unless deliberately added here.
EXPECTED_TABLES = {
    ("iceberg_bronze", "bronze"): frozenset(
        {
            "assortment",
            "clients",
            "costing",
            "delivery_costing",
            "delivery_resource",
            "delivery_tracking",
            "delivery_type",
            "order_product",
            "order_status_history",
            "order_statuses",
            "orders",
            "payments",
            "product_categories",
            "products",
            "stores",
            "zones",
        }
    ),
    ("iceberg_bronze", "ops"): frozenset({"bronze_apply_registry"}),
    ("iceberg_silver", "silver"): frozenset(
        {
            "budget_orders_sales",
            "dim_assortment",
            "dim_clients",
            "dim_costing",
            "dim_delivery_costing",
            "dim_delivery_resource",
            "dim_delivery_type",
            "dim_order_statuses",
            "dim_product_categories",
            "dim_products",
            "dim_stores",
            "dim_zones",
            "fact_orders",
            "fact_order_product",
            "fact_payments",
            "fact_order_status_history",
            "fact_delivery_tracking",
        }
    ),
    ("iceberg_gold", "gold"): frozenset(
        {
            "curie_marketing_sales_budget_monthly_store",
            "curie_marketing_product_sales_monthly_store",
            "curie_marketing_category_sales_monthly_store",
            "curie_marketing_client_metrics_monthly_store",
            "curie_marketing_active_client_month",
            "curie_financial_performance_monthly_store",
            "curie_financial_product_margin_monthly_store",
            "curie_delivery_courier_performance_monthly_store",
        }
    ),
}
SAFE_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
METADATA_PROPERTIES = {
    "write.metadata.delete-after-commit.enabled": "true",
    "write.metadata.previous-versions-max": "14",
}
MAX_COMPACTION_TABLE_BYTES = 4 * 1024**3
COMPACTION_OPTIONS = {
    "target-file-size-bytes": str(128 * 1024**2),
    "min-file-size-bytes": str(32 * 1024**2),
    "max-file-size-bytes": str(MAX_COMPACTION_TABLE_BYTES),
    "max-file-group-size-bytes": str(512 * 1024**2),
    "max-concurrent-file-group-rewrites": "2",
    "partial-progress.enabled": "true",
    "partial-progress.max-commits": "10",
    # Small tables may have only two or three active files even when older
    # snapshots still reference hundreds of physical files.
    "min-input-files": "2",
}


def _identifier(value: str) -> str:
    if not SAFE_IDENTIFIER.fullmatch(value):
        raise ValueError(f"Unexpected Iceberg identifier: {value!r}")
    return f"`{value}`"


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _cutoff_sql(cutoff: datetime) -> str:
    if cutoff.tzinfo is None or cutoff.utcoffset() != timedelta(0):
        raise ValueError("Housekeeping cutoff must be in UTC")
    return f"TIMESTAMP {_literal(cutoff.strftime('%Y-%m-%d %H:%M:%S'))}"


def _procedure_sql(
    catalog: str, schema: str, table: str, cutoff: datetime, *, dry_run: bool
) -> tuple[str | None, str]:
    catalog_sql = _identifier(catalog)
    table_sql = _literal(f"{schema}.{table}")
    cutoff_sql = _cutoff_sql(cutoff)
    expire_sql = None
    if not dry_run:
        expire_sql = (
            f"CALL {catalog_sql}.system.expire_snapshots("
            f"table => {table_sql}, older_than => {cutoff_sql}, "
            "retain_last => 1, stream_results => true)"
        )
    orphan_sql = (
        f"CALL {catalog_sql}.system.remove_orphan_files("
        f"table => {table_sql}, older_than => {cutoff_sql}, "
        f"dry_run => {'true' if dry_run else 'false'}, stream_results => true)"
    )
    return expire_sql, orphan_sql


def _compaction_sql(catalog: str, schema: str, table: str) -> str:
    options = ", ".join(
        f"{_literal(key)}, {_literal(value)}"
        for key, value in COMPACTION_OPTIONS.items()
    )
    return (
        f"CALL {_identifier(catalog)}.system.rewrite_data_files("
        f"table => {_literal(f'{schema}.{table}')}, "
        f"strategy => 'binpack', options => map({options}))"
    )


def _data_file_stats(
    spark: SparkSession, catalog: str, schema: str, table: str
) -> tuple[int, int]:
    name = ".".join(map(_identifier, (catalog, schema, table, "data_files")))
    row = spark.sql(
        f"SELECT COUNT(*) AS file_count, "
        f"COALESCE(SUM(file_size_in_bytes), 0) AS total_bytes FROM {name}"
    ).collect()[0]
    return int(row.file_count), int(row.total_bytes)


def _compact_table(
    spark: SparkSession, catalog: str, schema: str, table: str, *, dry_run: bool
) -> bool:
    name = f"{catalog}.{schema}.{table}"
    file_count, total_bytes = _data_file_stats(spark, catalog, schema, table)
    if file_count < 2:
        LOGGER.info("Skipping compaction for %s: data_files=%s", name, file_count)
        return False
    if total_bytes > MAX_COMPACTION_TABLE_BYTES:
        LOGGER.info(
            "Skipping compaction for %s: data_bytes=%s exceeds limit=%s",
            name,
            total_bytes,
            MAX_COMPACTION_TABLE_BYTES,
        )
        return False
    if dry_run:
        LOGGER.info(
            "Would compact %s: data_files=%s data_bytes=%s",
            name,
            file_count,
            total_bytes,
        )
        return False
    LOGGER.info(
        "Checking compaction for %s: active_data_files=%s active_bytes=%s",
        name,
        file_count,
        total_bytes,
    )
    finished = Event()
    started = time.monotonic()

    def report_wait() -> None:
        while not finished.wait(120):
            LOGGER.info(
                "Still waiting for Iceberg compaction of %s: elapsed_seconds=%d; "
                "see Spark Connect task logs for file-group progress",
                name, int(time.monotonic() - started),
            )

    reporter = Thread(target=report_wait, daemon=True)
    reporter.start()
    try:
        result = spark.sql(_compaction_sql(catalog, schema, table)).collect()
    finally:
        finished.set()
        reporter.join(timeout=1)
    metrics = result[0].asDict() if result else {}
    LOGGER.info("Compaction for %s: %s", name, metrics)
    return bool(metrics.get("rewritten_data_files_count", 0))


def _tables(spark: SparkSession, catalog: str, schema: str) -> list[str]:
    namespace = f"{_identifier(catalog)}.{_identifier(schema)}"
    rows = spark.sql(f"SHOW TABLES IN {namespace}").collect()
    names = {str(row.tableName) for row in rows if not row.isTemporary}
    for name in names:
        _identifier(name)
    expected = EXPECTED_TABLES[catalog, schema]
    missing = expected - names
    if missing:
        raise RuntimeError(
            f"Missing pipeline tables in {catalog}.{schema}: {sorted(missing)}"
        )
    extra = names - expected
    if extra:
        LOGGER.info(
            "Leaving non-pipeline tables in %s.%s untouched: %s",
            catalog,
            schema,
            sorted(extra),
        )
    return sorted(expected)


def _ensure_metadata_policy(
    spark: SparkSession, catalog: str, schema: str, table: str, *, dry_run: bool
) -> None:
    name = ".".join(map(_identifier, (catalog, schema, table)))
    properties = {
        str(row.key): str(row.value)
        for row in spark.sql(f"SHOW TBLPROPERTIES {name}").collect()
    }
    if all(properties.get(key) == value for key, value in METADATA_PROPERTIES.items()):
        return
    if dry_run:
        LOGGER.info("Would set metadata retention properties on %s", name)
        return
    assignments = ", ".join(
        f"{_literal(key)}={_literal(value)}"
        for key, value in METADATA_PROPERTIES.items()
    )
    spark.sql(f"ALTER TABLE {name} SET TBLPROPERTIES ({assignments})").collect()
    LOGGER.info("Set metadata retention properties on %s", name)


def run_housekeeping(
    spark: SparkSession, *, cutoff: datetime, dry_run: bool = False
) -> int:
    """Compact small files, expire history, then remove aged orphan files."""
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    completed = 0
    compacted = 0
    for catalog, schema in NAMESPACES:
        tables = _tables(spark, catalog, schema)
        LOGGER.info("Discovered %s tables in %s.%s", len(tables), catalog, schema)
        for table in tables:
            name = f"{catalog}.{schema}.{table}"
            expire_sql, orphan_sql = _procedure_sql(
                catalog, schema, table, cutoff, dry_run=dry_run
            )
            try:
                _ensure_metadata_policy(spark, catalog, schema, table, dry_run=dry_run)
                compacted += _compact_table(
                    spark, catalog, schema, table, dry_run=dry_run
                )
                if expire_sql is not None:
                    result = spark.sql(expire_sql).collect()
                    LOGGER.info(
                        "Expired snapshots for %s: %s",
                        name,
                        result[0].asDict() if result else {},
                    )
                orphan_count = sum(1 for _ in spark.sql(orphan_sql).toLocalIterator())
            except Exception:
                LOGGER.exception("Iceberg housekeeping failed for %s", name)
                raise
            LOGGER.info(
                "%s orphan cleanup for %s: returned_paths=%s",
                "Previewed" if dry_run else "Completed",
                name,
                orphan_count,
            )
            completed += 1
    LOGGER.info(
        "Iceberg housekeeping completed: tables=%s compacted_tables=%s dry_run=%s",
        completed,
        compacted,
        dry_run,
    )
    return completed


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spark-remote", required=True)
    parser.add_argument("--retention-days", type=int, default=14)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    if args.retention_days < 14:
        parser.error("retention must be at least 14 days")
    cutoff = datetime.now(timezone.utc) - timedelta(days=args.retention_days)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    LOGGER.info(
        "Starting Iceberg housekeeping: cutoff=%s dry_run=%s",
        cutoff.isoformat(),
        args.dry_run,
    )
    spark = (
        SparkSession.builder.remote(args.spark_remote)
        .appName("ampere-iceberg-housekeeping")
        .getOrCreate()
    )
    try:
        run_housekeeping(spark, cutoff=cutoff, dry_run=args.dry_run)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
