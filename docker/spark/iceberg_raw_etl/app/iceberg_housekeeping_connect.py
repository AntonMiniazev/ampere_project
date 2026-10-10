"""Maintain contract v3 Iceberg tables through the shared Spark Connect service."""

from __future__ import annotations

import argparse
import logging
import re
import time
from datetime import datetime, timedelta, timezone
from threading import Event, Thread

from pyspark.sql import SparkSession
from tools.contracts.ampere_contract import AmpereContract, ResolvedTable, load_contract
from tools.contracts.spark_conformance import quote_identifier, validate_spark_table


LOGGER = logging.getLogger("iceberg-housekeeping")
SAFE_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
CATALOGS = {"bronze": "iceberg_bronze", "silver": "iceberg_silver", "gold": "iceberg_gold"}
MIN_MANIFESTS_FOR_REWRITE = 100
MAX_COMPACTION_TABLE_BYTES = 4 * 1024**3
CONTRACT = load_contract()


def _identifier(value: str) -> str:
    if not SAFE_IDENTIFIER.fullmatch(value):
        raise ValueError(f"Unexpected Iceberg identifier: {value!r}")
    return quote_identifier(value)


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _cutoff_sql(cutoff: datetime) -> str:
    if cutoff.tzinfo is None or cutoff.utcoffset() != timedelta(0):
        raise ValueError("Housekeeping cutoff must be in UTC")
    return f"TIMESTAMP {_literal(cutoff.strftime('%Y-%m-%d %H:%M:%S'))}"


def _contract_table(catalog: str, schema: str, name: str) -> ResolvedTable:
    layer = {value: key for key, value in CATALOGS.items()}.get(catalog)
    if layer is None:
        raise ValueError(f"Unknown contract catalog {catalog!r}")
    table = CONTRACT.table(layer, name)
    if table.namespace != schema:
        raise ValueError(f"Contract namespace mismatch for {catalog}.{schema}.{name}")
    return table


def _procedure_sql(
    catalog: str,
    schema: str,
    table: str,
    as_of: datetime,
    *,
    dry_run: bool,
    spec: ResolvedTable | None = None,
) -> tuple[str | None, str]:
    spec = spec or _contract_table(catalog, schema, table)
    target = _literal(f"{schema}.{table}")
    snapshots_days = int(spec.maintenance.get("snapshot_retention_days", 14))
    orphan_days = int(spec.maintenance.get("orphan_retention_days", 14))
    expire_sql = None
    if not dry_run:
        expire_sql = (
            f"CALL {_identifier(catalog)}.system.expire_snapshots("
            f"table => {target}, older_than => {_cutoff_sql(as_of - timedelta(days=snapshots_days))}, "
            "retain_last => 1, stream_results => true)"
        )
    orphan_sql = (
        f"CALL {_identifier(catalog)}.system.remove_orphan_files("
        f"table => {target}, older_than => {_cutoff_sql(as_of - timedelta(days=orphan_days))}, "
        f"dry_run => {'true' if dry_run else 'false'}, stream_results => true)"
    )
    return expire_sql, orphan_sql


def _data_file_stats(spark: SparkSession, catalog: str, schema: str, table: str) -> tuple[int, int]:
    metadata = ".".join(map(_identifier, (catalog, schema, table, "data_files")))
    row = spark.sql(
        f"SELECT COUNT(*) AS file_count, COALESCE(SUM(file_size_in_bytes), 0) AS total_bytes FROM {metadata}"
    ).collect()[0]
    return int(row.file_count), int(row.total_bytes)


def _delete_file_count(spark: SparkSession, catalog: str, schema: str, table: str) -> int:
    metadata = ".".join(map(_identifier, (catalog, schema, table, "delete_files")))
    try:
        return int(spark.sql(f"SELECT COUNT(*) FROM {metadata}").collect()[0][0])
    except Exception:
        # Iceberg metadata tables may reject the query when the table has no
        # delete-file support. Treat that as a hard error only for profiles
        # that request delete-file rewriting (handled by the caller).
        raise


def _compaction_sql(catalog: str, schema: str, table: str, spec: ResolvedTable) -> str:
    maintenance = spec.maintenance
    write = spec.write
    options = {
        "target-file-size-bytes": str(write.get("target_file_size_bytes", 64 * 1024**2)),
        "min-input-files": str(maintenance.get("min_input_files", 5)),
        "max-file-group-size-bytes": str(512 * 1024**2),
        "partial-progress.enabled": "true",
        "partial-progress.max-commits": "10",
    }
    if maintenance.get("delete_files") == "rewrite_on_threshold":
        options["delete-file-threshold"] = "1"
    options_sql = ", ".join(f"{_literal(k)}, {_literal(v)}" for k, v in options.items())
    return (
        f"CALL {_identifier(catalog)}.system.rewrite_data_files("
        f"table => {_literal(f'{schema}.{table}')}, strategy => 'binpack', "
        f"options => map({options_sql}))"
    )


def _compact_table(
    spark: SparkSession,
    catalog: str,
    schema: str,
    table: str,
    *,
    dry_run: bool,
    spec: ResolvedTable | None = None,
) -> bool:
    name = f"{catalog}.{schema}.{table}"
    spec = spec or _contract_table(catalog, schema, table)
    policy = spec.maintenance.get("data_compaction", "none")
    if policy == "none":
        LOGGER.info("Skipping compaction for %s: disabled by profile %s", name, spec.profile_name)
        return False
    file_count, total_bytes = _data_file_stats(spark, catalog, schema, table)
    minimum = int(spec.maintenance.get("min_input_files", 5))
    if file_count < minimum:
        LOGGER.info("Skipping compaction for %s: data_files=%s below contract min_input_files=%s", name, file_count, minimum)
        return False
    if total_bytes > MAX_COMPACTION_TABLE_BYTES:
        LOGGER.info("Skipping compaction for %s: active_bytes=%s exceeds safety limit=%s", name, total_bytes, MAX_COMPACTION_TABLE_BYTES)
        return False
    if dry_run:
        LOGGER.info("Would compact %s: data_files=%s active_bytes=%s", name, file_count, total_bytes)
        return False

    LOGGER.info("Checking compaction for %s: active_data_files=%s active_bytes=%s", name, file_count, total_bytes)
    finished = Event()
    started = time.monotonic()

    def report_wait() -> None:
        while not finished.wait(120):
            LOGGER.info("Still waiting for Iceberg compaction of %s: elapsed_seconds=%d; check Spark Connect task logs for file-group progress", name, int(time.monotonic() - started))

    reporter = Thread(target=report_wait, daemon=True)
    reporter.start()
    try:
        result = spark.sql(_compaction_sql(catalog, schema, table, spec)).collect()
    finally:
        finished.set()
        reporter.join(timeout=1)
    metrics = result[0].asDict() if result else {}
    LOGGER.info("Compaction for %s: %s", name, metrics)
    return bool(metrics.get("rewritten_data_files_count", 0))


def _tables(spark: SparkSession, catalog: str, schema: str) -> list[str]:
    namespace = f"{_identifier(catalog)}.{_identifier(schema)}"
    actual = {
        str(row.tableName)
        for row in spark.sql(f"SHOW TABLES IN {namespace}").collect()
        if not row.isTemporary
    }
    for name in actual:
        _identifier(name)
    layer = {value: key for key, value in CATALOGS.items()}[catalog]
    expected = {
        table.name for table in CONTRACT.layer_tables(layer)
        if table.namespace == schema
    }
    missing = expected - actual
    if missing:
        raise RuntimeError(f"Missing contract tables in {catalog}.{schema}: {sorted(missing)}")
    extras = actual - expected
    if extras:
        LOGGER.info("Leaving tables outside contract v%s untouched in %s.%s: %s", CONTRACT.version, catalog, schema, sorted(extras))
    return sorted(expected)


def _maybe_rewrite_manifests(
    spark: SparkSession,
    catalog: str,
    schema: str,
    table: str,
    spec: ResolvedTable,
    *,
    dry_run: bool,
) -> None:
    if spec.maintenance.get("rewrite_manifests", "none") == "none":
        return
    metadata = ".".join(map(_identifier, (catalog, schema, table, "manifests")))
    count = int(spark.sql(f"SELECT COUNT(*) FROM {metadata}").collect()[0][0])
    name = f"{catalog}.{schema}.{table}"
    if count < MIN_MANIFESTS_FOR_REWRITE:
        LOGGER.info("Skipping manifest rewrite for %s: manifests=%s threshold=%s", name, count, MIN_MANIFESTS_FOR_REWRITE)
        return
    if dry_run:
        LOGGER.info("Would rewrite manifests for %s: manifests=%s", name, count)
        return
    result = spark.sql(
        f"CALL {_identifier(catalog)}.system.rewrite_manifests(table => {_literal(f'{schema}.{table}')})"
    ).collect()
    LOGGER.info("Rewrote manifests for %s: %s", name, result[0].asDict() if result else {})


def run_housekeeping(spark: SparkSession, *, as_of: datetime, dry_run: bool = False) -> int:
    """Check contract conformance, compact by policy, and clean table history."""
    if as_of.tzinfo is None or as_of.utcoffset() != timedelta(0):
        raise ValueError("Housekeeping reference time must be in UTC")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    completed = 0
    compacted = 0
    namespaces = sorted({(CATALOGS[table.layer], table.namespace) for table in CONTRACT.tables.values()})
    for catalog, schema in namespaces:
        tables = _tables(spark, catalog, schema)
        LOGGER.info("Discovered %s contract tables in %s.%s", len(tables), catalog, schema)
        for table_name in tables:
            name = f"{catalog}.{schema}.{table_name}"
            spec = _contract_table(catalog, schema, table_name)
            try:
                validate_spark_table(spark, catalog, spec)
                LOGGER.info("Table conformance passed for %s", name)
                compacted += _compact_table(spark, catalog, schema, table_name, dry_run=dry_run, spec=spec)
                _maybe_rewrite_manifests(spark, catalog, schema, table_name, spec, dry_run=dry_run)
                expire_sql, orphan_sql = _procedure_sql(catalog, schema, table_name, as_of, dry_run=dry_run, spec=spec)
                if expire_sql:
                    result = spark.sql(expire_sql).collect()
                    LOGGER.info("Expired snapshots for %s: %s", name, result[0].asDict() if result else {})
                orphan_count = sum(1 for _ in spark.sql(orphan_sql).toLocalIterator())
                LOGGER.info("%s orphan cleanup for %s: returned_paths=%s", "Previewed" if dry_run else "Completed", name, orphan_count)
            except Exception:
                LOGGER.exception("Iceberg housekeeping failed for %s", name)
                raise
            completed += 1
    LOGGER.info("Iceberg housekeeping completed: tables=%s compacted_tables=%s dry_run=%s", completed, compacted, dry_run)
    return completed


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spark-remote", required=True)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    as_of = datetime.now(timezone.utc)
    LOGGER.info("Starting Iceberg housekeeping: contract_version=%s as_of=%s dry_run=%s", CONTRACT.version, as_of.isoformat(), args.dry_run)
    spark = SparkSession.builder.remote(args.spark_remote).appName("ampere-iceberg-housekeeping").getOrCreate()
    try:
        run_housekeeping(spark, as_of=as_of, dry_run=args.dry_run)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
