"""Transfer compact change metadata plus source after-images to Raw Iceberg."""

from __future__ import annotations

import argparse
import logging
import uuid

import psycopg
from pyspark import StorageLevel
from pyspark.sql import SparkSession

from etl_utils import configure_s3, get_env, get_minio_connection_settings, get_postgres_connection_settings, set_spark_log_level, setup_logging
from iceberg_bronze.catalog import configure_lakekeeper_catalog
from tools.contracts.ampere_contract import load_contract
from tools.contracts.spark_conformance import quote_identifier, validate_spark_table

LOGGER = logging.getLogger("journal-to-raw")
CONTRACT = load_contract()
SOURCE_TABLES = tuple(spec.name for spec in CONTRACT.layer_tables("raw"))
TRANSFER_LOCK_KEY = 724614902632
MAX_TRANSFER_BATCHES = 96
DEFAULT_RUN_BATCHES = 32


def _pg_quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _connect_postgres():
    settings = get_postgres_connection_settings()
    return psycopg.connect(
        host=settings.host,
        port=settings.port,
        dbname=settings.database,
        user=settings.user,
        password=settings.password,
        application_name="ampere-journal-to-raw",
    )


def _last_acknowledged_batch(cursor, dataset_id: str) -> int:
    """Read the indexed transfer high-water mark without joining old batches."""
    cursor.execute(
        "SELECT COALESCE(MAX(batch_sequence), 0) "
        "FROM pipeline.raw_transfer_batches WHERE dataset_id=%s",
        (dataset_id,),
    )
    return int(cursor.fetchone()[0])


def reserve_transfer(max_batches: int = 32) -> dict | None:
    """Freeze/resume one ordered, dataset-scoped transfer in PostgreSQL."""
    if not 1 <= max_batches <= MAX_TRANSFER_BATCHES:
        raise ValueError(
            f"Each Raw transfer group must contain 1..{MAX_TRANSFER_BATCHES} batches"
        )
    with _connect_postgres() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT pg_advisory_xact_lock(%s)", (TRANSFER_LOCK_KEY,))
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS pipeline.raw_transfers (
                transfer_id uuid PRIMARY KEY,
                dataset_id uuid NOT NULL REFERENCES pipeline.datasets(dataset_id),
                first_batch_sequence bigint NOT NULL,
                last_batch_sequence bigint NOT NULL,
                status text NOT NULL CHECK (status IN ('started', 'completed')),
                started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
                completed_at timestamptz
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS pipeline.raw_transfer_batches (
                transfer_id uuid NOT NULL REFERENCES pipeline.raw_transfers(transfer_id),
                dataset_id uuid NOT NULL,
                batch_id uuid NOT NULL REFERENCES pipeline.batches(batch_id),
                batch_sequence bigint NOT NULL,
                PRIMARY KEY (transfer_id, batch_id),
                UNIQUE (dataset_id, batch_id)
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS pipeline.raw_transfer_tables (
                transfer_id uuid NOT NULL REFERENCES pipeline.raw_transfers(transfer_id),
                source_table text NOT NULL,
                row_count bigint NOT NULL,
                snapshot_id bigint,
                status text NOT NULL CHECK (status IN ('completed')),
                completed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
                PRIMARY KEY (transfer_id, source_table)
            )
            """
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS ix_pipeline_raw_transfer_batches_dataset_sequence "
            "ON pipeline.raw_transfer_batches (dataset_id, batch_sequence DESC)"
        )
        cursor.execute(
            "SELECT t.transfer_id, t.dataset_id, t.first_batch_sequence, t.last_batch_sequence "
            "FROM pipeline.raw_transfers t "
            "JOIN pipeline.datasets d ON d.dataset_id=t.dataset_id "
            "WHERE t.status='started' AND d.status='ready' ORDER BY t.started_at LIMIT 1"
        )
        existing = cursor.fetchone()
        if existing:
            transfer_id, dataset_id, first_sequence, last_sequence = existing
        else:
            cursor.execute("SELECT dataset_id FROM pipeline.datasets WHERE status='ready'")
            dataset_row = cursor.fetchone()
            if dataset_row is None:
                raise RuntimeError("No ready source dataset exists for Raw transfer")
            dataset_id = dataset_row[0]
            last_acknowledged = _last_acknowledged_batch(cursor, dataset_id)
            cursor.execute(
                """
                SELECT batch_id, batch_sequence FROM pipeline.batches
                WHERE dataset_id=%s AND status='completed' AND batch_sequence>%s
                ORDER BY batch_sequence LIMIT %s
                """,
                (dataset_id, last_acknowledged, max_batches),
            )
            batches = cursor.fetchall()
            if not batches:
                return None
            first_sequence = int(batches[0][1])
            last_sequence = int(batches[-1][1])
            transfer_id = uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"ampere-raw:{dataset_id}:{first_sequence}:{last_sequence}:contract-v5",
            )
            cursor.execute(
                "INSERT INTO pipeline.raw_transfers "
                "(transfer_id,dataset_id,first_batch_sequence,last_batch_sequence,status) "
                "VALUES (%s,%s,%s,%s,'started')",
                (transfer_id, dataset_id, first_sequence, last_sequence),
            )
            cursor.executemany(
                "INSERT INTO pipeline.raw_transfer_batches "
                "(transfer_id,dataset_id,batch_id,batch_sequence) VALUES (%s,%s,%s,%s)",
                [(transfer_id, dataset_id, batch_id, sequence) for batch_id, sequence in batches],
            )

        cursor.execute(
            "SELECT batch_id, batch_sequence FROM pipeline.raw_transfer_batches "
            "WHERE transfer_id=%s ORDER BY batch_sequence",
            (transfer_id,),
        )
        batches = cursor.fetchall()
        cursor.execute(
            """
            SELECT c.source_table, SUM(c.change_count)::bigint
            FROM pipeline.batch_table_counts c
            JOIN pipeline.raw_transfer_batches b
              ON b.dataset_id=c.dataset_id AND b.batch_id=c.batch_id
            WHERE b.transfer_id=%s GROUP BY c.source_table
            """,
            (transfer_id,),
        )
        counts = {table: int(count) for table, count in cursor.fetchall()}
        return {
            "transfer_id": str(transfer_id),
            "dataset_id": str(dataset_id),
            "first_sequence": int(first_sequence),
            "last_sequence": int(last_sequence),
            "batches": [(str(batch_id), int(sequence)) for batch_id, sequence in batches],
            "counts": {table: counts.get(table, 0) for table in SOURCE_TABLES},
        }


def _raw_table_name(table: str) -> str:
    name = f"iceberg_raw.raw.{table}"
    spec = CONTRACT.table("raw", table)
    validate_spark_table(_SPARK, "iceberg_raw", spec)
    return ".".join(quote_identifier(part) for part in name.split("."))


def _change_slice_query(spec, transfer_id: str) -> str:
    """Build a JDBC slice from compact keys and source after-images."""
    columns = [column["name"] for column in spec.columns]
    business_columns = set(spec.ingestion["business_columns"])
    table = _pg_quote(spec.ingestion["source_table"])
    source_schema = _pg_quote(spec.ingestion["source_schema"])
    metadata = {
        "_dataset_id": "pb.dataset_id::text",
        "_batch_id": "j.batch_id::text",
        "_batch_sequence": "pb.batch_sequence",
        "_change_id": "j.change_id",
        "_source_row_id": "j.source_row_id::text",
        "_op": "j.op",
        "_business_date": "pb.business_date",
        "_captured_at": "pb.completed_at",
        "_transfer_id": f"'{transfer_id}'::text",
        "_ingest_run_id": "'ampere-raw-transfer'::text",
        "_ingest_ts": "clock_timestamp()",
        "_source_system": "'postgres-pre-raw'::text",
        "_source_table": f"'{spec.ingestion['source_table']}'::text",
        "_contract_version": "5::integer",
    }
    projection = []
    for column in columns:
        if column in business_columns:
            expression = f"s.{_pg_quote(column)}"
        else:
            expression = metadata[column]
        projection.append(f"{expression} AS {_pg_quote(column)}")
    return (
        "(SELECT " + ", ".join(projection) +
        f" FROM source_changes.{table} j "
        "JOIN pipeline.raw_transfer_batches rtb ON rtb.batch_id=j.batch_id "
        "JOIN pipeline.batches pb "
        "ON pb.dataset_id=rtb.dataset_id AND pb.batch_id=rtb.batch_id "
        f"LEFT JOIN {source_schema}.{table} s "
        "ON s.source_row_id=j.source_row_id "
        f"WHERE rtb.transfer_id='{transfer_id}') AS change_slice"
    )


_SPARK: SparkSession


def _merge_source_changes(spark: SparkSession, transfer: dict, table: str) -> int:
    """Join key-only change metadata to source after-images, then merge into Raw."""
    expected_count = transfer["counts"][table]
    if expected_count == 0:
        return 0

    spec = CONTRACT.table("raw", table)
    columns = [column["name"] for column in spec.columns]
    query = _change_slice_query(spec, transfer["transfer_id"])
    pg = get_postgres_connection_settings()
    source = (
        spark.read.format("jdbc")
        .option("url", pg.jdbc_url)
        .option("dbtable", query)
        .option("user", pg.user)
        .option("password", pg.password)
        .option("driver", "org.postgresql.Driver")
        .option("fetchsize", "5000")
        .load()
    )
    source = source.persist(StorageLevel.DISK_ONLY)
    try:
        actual_count = source.count()
        if actual_count != expected_count:
            raise RuntimeError(
                f"Journal count mismatch for {table}: control={expected_count}, JDBC={actual_count}"
            )
        source.createOrReplaceTempView("raw_transfer_source")
        target = _raw_table_name(table)
        keys = spec.ingestion["change_keys"]
        condition = " AND ".join(
            f"t.{quote_identifier(key)} = s.{quote_identifier(key)}" for key in keys
        )
        names = ", ".join(quote_identifier(column) for column in columns)
        values = ", ".join(f"s.{quote_identifier(column)}" for column in columns)
        spark.sql(
            f"MERGE INTO {target} t USING raw_transfer_source s ON {condition} "
            f"WHEN NOT MATCHED THEN INSERT ({names}) VALUES ({values})"
        )
    finally:
        source.unpersist()
    return expected_count


def _record_table_complete(transfer_id: str, table: str, count: int, snapshot: int | None) -> None:
    with _connect_postgres() as connection, connection.cursor() as cursor:
        cursor.execute(
            "INSERT INTO pipeline.raw_transfer_tables "
            "(transfer_id,source_table,row_count,snapshot_id,status) "
            "VALUES (%s,%s,%s,%s,'completed') "
            "ON CONFLICT (transfer_id,source_table) DO UPDATE SET "
            "row_count=EXCLUDED.row_count,snapshot_id=EXCLUDED.snapshot_id,status='completed', "
            "completed_at=clock_timestamp()",
            (transfer_id, table, count, snapshot),
        )


def _table_already_complete(transfer_id: str, table: str) -> bool:
    with _connect_postgres() as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT 1 FROM pipeline.raw_transfer_tables "
            "WHERE transfer_id=%s AND source_table=%s AND status='completed'",
            (transfer_id, table),
        )
        return cursor.fetchone() is not None


def _latest_snapshot(spark: SparkSession, table: str) -> int | None:
    row = spark.sql(
        f"SELECT snapshot_id FROM {_raw_table_name(table)}.snapshots "
        "ORDER BY committed_at DESC LIMIT 1"
    ).first()
    return int(row[0]) if row else None


def _complete_transfer(spark: SparkSession, transfer: dict) -> None:
    """Merge one bounded batch group and acknowledge its completed tables."""
    LOGGER.info(
        "Raw transfer %s dataset=%s batches=%s..%s (%s source batches)",
        transfer["transfer_id"], transfer["dataset_id"], transfer["first_sequence"],
        transfer["last_sequence"], len(transfer["batches"]),
    )
    for table in SOURCE_TABLES:
        if _table_already_complete(transfer["transfer_id"], table):
            LOGGER.info("RAW_TABLE_COMPLETE table=%s resumed=checkpoint", table)
            continue
        count = _merge_source_changes(spark, transfer, table)
        _record_table_complete(
            transfer["transfer_id"],
            table,
            count,
            _latest_snapshot(spark, table) if count else None,
        )
        LOGGER.info("RAW_TABLE_COMPLETE table=%s rows=%s", table, count)
    with _connect_postgres() as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT count(*) FROM pipeline.raw_transfer_tables "
            "WHERE transfer_id=%s AND status='completed'",
            (transfer["transfer_id"],),
        )
        if cursor.fetchone()[0] != len(SOURCE_TABLES):
            raise RuntimeError("Raw transfer is missing per-table completion records")
        cursor.execute(
            "UPDATE pipeline.raw_transfers SET status='completed',completed_at=clock_timestamp() "
            "WHERE transfer_id=%s AND status='started'",
            (transfer["transfer_id"],),
        )
    LOGGER.info("RAW_TRANSFER_COMPLETE transfer_id=%s", transfer["transfer_id"])


def transfer_backlog(
    spark: SparkSession,
    first_transfer: dict,
    *,
    max_batches: int,
    batch_size: int,
) -> int:
    """Process up to max_batches in sequential transfer groups of at most 96."""
    if max_batches < 1:
        raise ValueError("max_batches must be positive")
    if not 1 <= batch_size <= MAX_TRANSFER_BATCHES:
        raise ValueError(f"batch_size must be between 1 and {MAX_TRANSFER_BATCHES}")

    completed = 0
    transfer = first_transfer
    while transfer is not None and completed < max_batches:
        batch_count = len(transfer["batches"])
        if not batch_count:
            raise RuntimeError("Reserved Raw transfer contains no batches")
        if batch_count > MAX_TRANSFER_BATCHES:
            raise RuntimeError(
                f"Reserved transfer has {batch_count} batches; maximum safe group is "
                f"{MAX_TRANSFER_BATCHES}. Resolve the existing started transfer before retrying."
            )
        _complete_transfer(spark, transfer)
        completed += batch_count
        LOGGER.info(
            "RAW_TRANSFER_GROUP_COMPLETE batches=%s total=%s requested=%s",
            batch_count, completed, max_batches,
        )
        if completed >= max_batches:
            break
        next_size = min(batch_size, max_batches - completed)
        transfer = reserve_transfer(next_size)
    LOGGER.info("RAW_BACKLOG_COMPLETE transferred=%s requested=%s", completed, max_batches)
    return completed


def main() -> None:
    global _SPARK
    setup_logging()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--max-batches",
        type=int,
        default=DEFAULT_RUN_BATCHES,
        help="Maximum total completed batches to transfer in this Spark run",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=MAX_TRANSFER_BATCHES,
        help=f"Maximum batches per transfer group (1..{MAX_TRANSFER_BATCHES})",
    )
    parser.add_argument("--warehouse", default="raw")
    parser.add_argument("--catalog-uri", required=True)
    parser.add_argument("--oauth-uri", required=True)
    parser.add_argument("--scope", required=True)
    parser.add_argument("--app-name", default="ampere-journal-to-raw")
    args = parser.parse_args()
    if args.max_batches < 1:
        raise ValueError("--max-batches must be positive")
    if not 1 <= args.batch_size <= MAX_TRANSFER_BATCHES:
        raise ValueError(f"--batch-size must be between 1 and {MAX_TRANSFER_BATCHES}")
    transfer = reserve_transfer(min(args.max_batches, args.batch_size))
    if transfer is None:
        LOGGER.info("No completed untransferred source batches")
        return

    minio = get_minio_connection_settings()
    spark = (
        SparkSession.builder.appName(args.app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .getOrCreate()
    )
    _SPARK = spark
    try:
        configure_s3(spark, minio.endpoint, minio.access_key, minio.secret_key)
        set_spark_log_level(spark)
        configure_lakekeeper_catalog(
            spark,
            catalog="iceberg_raw",
            warehouse=args.warehouse,
            uri=args.catalog_uri,
            oauth_uri=args.oauth_uri,
            scope=args.scope,
            client_id=get_env("LAKEKEEPER_CLIENT_ID"),
            client_secret=get_env("LAKEKEEPER_CLIENT_SECRET"),
            minio_endpoint=minio.endpoint,
        )
        transfer_backlog(
            spark,
            transfer,
            max_batches=args.max_batches,
            batch_size=args.batch_size,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
