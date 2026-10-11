"""Apply accepted Raw change batches to Bronze as versioned current state."""

from __future__ import annotations

import logging
import os
import uuid

import psycopg
from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.window import Window

from etl_utils import configure_s3, get_env, get_minio_connection_settings, get_postgres_connection_settings
from iceberg_bronze.catalog import CONTRACT, configure_lakekeeper_catalog
from tools.contracts.spark_conformance import quote_identifier, validate_spark_table

LOGGER = logging.getLogger("raw-journal-to-bronze")
TABLES = tuple(spec.name for spec in CONTRACT.layer_tables("raw"))
BRONZE_REBUILD_GENERATION = "lineage-v1"


def _pg_connect():
    settings = get_postgres_connection_settings()
    return psycopg.connect(
        host=settings.host,
        port=settings.port,
        dbname=settings.database,
        user=settings.user,
        password=settings.password,
        application_name="ampere-raw-to-bronze",
    )


def _control_tables(cursor) -> None:
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS pipeline.bronze_apply_state (
            state_key boolean PRIMARY KEY DEFAULT true CHECK (state_key),
            dataset_id uuid NOT NULL,
            last_batch_sequence bigint NOT NULL,
            last_transfer_id uuid NOT NULL,
            completed_at timestamptz NOT NULL DEFAULT clock_timestamp()
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS pipeline.bronze_transfers (
            transfer_id uuid PRIMARY KEY,
            dataset_id uuid NOT NULL,
            first_batch_sequence bigint NOT NULL,
            last_batch_sequence bigint NOT NULL,
            status text NOT NULL CHECK (status IN ('started','completed')),
            completed_at timestamptz
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS pipeline.bronze_transfer_tables (
            transfer_id uuid NOT NULL REFERENCES pipeline.bronze_transfers(transfer_id),
            source_table text NOT NULL,
            status text NOT NULL CHECK (status='completed'),
            completed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
            PRIMARY KEY (transfer_id, source_table)
        )
        """
    )


def reserve_incremental_transfer() -> dict | None:
    """Return/resume the next complete Raw transfer; reject a dataset cutover."""
    with _pg_connect() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT pg_advisory_xact_lock(724614902633)")
        _control_tables(cursor)
        cursor.execute("SELECT state_key,dataset_id,last_batch_sequence FROM pipeline.bronze_apply_state WHERE state_key")
        state = cursor.fetchone()
        if state is None:
            raise RuntimeError("Bronze has no source checkpoint; run the explicit full-rebuild DAG first")
        cursor.execute("SELECT dataset_id FROM pipeline.datasets WHERE status='ready'")
        active_dataset = cursor.fetchone()
        if active_dataset and active_dataset[0] != state[1]:
            raise RuntimeError("Source dataset changed; run the explicit Bronze full-rebuild DAG")
        cursor.execute(
            "SELECT transfer_id,dataset_id,first_batch_sequence,last_batch_sequence "
            "FROM pipeline.bronze_transfers WHERE status='started' AND dataset_id=%s "
            "ORDER BY first_batch_sequence LIMIT 1",
            (state[1],),
        )
        transfer = cursor.fetchone()
        if transfer is None:
            cursor.execute(
                "SELECT transfer_id,dataset_id,first_batch_sequence,last_batch_sequence "
                "FROM pipeline.raw_transfers WHERE status='completed' AND dataset_id=%s "
                "AND first_batch_sequence>%s ORDER BY first_batch_sequence LIMIT 1",
                (state[1], state[2]),
            )
            transfer = cursor.fetchone()
            if transfer is None:
                return None
            transfer_id, dataset_id, first_sequence, last_sequence = transfer
            cursor.execute(
                "INSERT INTO pipeline.bronze_transfers "
                "(transfer_id,dataset_id,first_batch_sequence,last_batch_sequence,status) "
                "VALUES (%s,%s,%s,%s,'started') ON CONFLICT (transfer_id) DO NOTHING",
                (transfer_id, dataset_id, first_sequence, last_sequence),
            )
        cursor.execute(
            "SELECT count(*) FROM pipeline.raw_transfer_tables "
            "WHERE transfer_id=%s AND status='completed'",
            (transfer_id,),
        )
        if cursor.fetchone()[0] != len(TABLES):
            raise RuntimeError(f"Raw transfer {transfer_id} is not complete for all source tables")
        cursor.execute(
            "SELECT source_table,row_count FROM pipeline.raw_transfer_tables WHERE transfer_id=%s",
            (transfer_id,),
        )
        expected_counts = {table: int(count) for table, count in cursor.fetchall()}
        return {
            "transfer_id": str(transfer_id),
            "dataset_id": str(dataset_id),
            "first_sequence": int(first_sequence),
            "last_sequence": int(last_sequence),
            "transfer_ids": [str(transfer_id)],
            "expected_counts": expected_counts,
            "rebuild": False,
        }


def reserve_full_rebuild() -> dict:
    """Freeze all accepted Raw transfers for the currently ready dataset."""
    with _pg_connect() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT pg_advisory_xact_lock(724614902633)")
        _control_tables(cursor)
        cursor.execute("SELECT dataset_id FROM pipeline.datasets WHERE status='ready'")
        dataset_row = cursor.fetchone()
        if dataset_row is None:
            raise RuntimeError("No ready source dataset exists")
        dataset_id = dataset_row[0]
        cursor.execute(
            "SELECT transfer_id,first_batch_sequence,last_batch_sequence "
            "FROM pipeline.raw_transfers WHERE dataset_id=%s AND status='completed' "
            "ORDER BY first_batch_sequence",
            (dataset_id,),
        )
        transfers = cursor.fetchall()
        if not transfers:
            raise RuntimeError("No accepted Raw history exists to rebuild Bronze")
        ids = [row[0] for row in transfers]
        first_sequence = int(transfers[0][1])
        last_sequence = int(transfers[-1][2])
        cursor.execute(
            "SELECT source_table,SUM(row_count)::bigint FROM pipeline.raw_transfer_tables "
            "WHERE transfer_id = ANY(%s) GROUP BY source_table",
            (ids,),
        )
        expected_counts = {table: int(count) for table, count in cursor.fetchall()}
        rebuild_id = _full_rebuild_transfer_id(str(dataset_id), last_sequence)
        cursor.execute(
            "INSERT INTO pipeline.bronze_transfers "
            "(transfer_id,dataset_id,first_batch_sequence,last_batch_sequence,status) "
            "VALUES (%s,%s,%s,%s,'started') ON CONFLICT (transfer_id) DO NOTHING",
            (rebuild_id, dataset_id, first_sequence, last_sequence),
        )
        return {
            "transfer_id": str(rebuild_id),
            "dataset_id": str(dataset_id),
            "first_sequence": first_sequence,
            "last_sequence": last_sequence,
            "transfer_ids": [str(value) for value in ids],
            "expected_counts": expected_counts,
            "rebuild": True,
        }


def _latest_changes(raw: DataFrame) -> DataFrame:
    window = Window.partitionBy("_source_row_id").orderBy(
        F.col("_batch_sequence").desc(), F.col("_change_id").desc()
    )
    return (
        raw.withColumn("_latest_rank", F.row_number().over(window))
        .filter(F.col("_latest_rank") == 1)
        .drop("_latest_rank")
    )


def _raw_source_column_for_bronze(column: str) -> str:
    """Map Bronze lineage names to their corresponding Raw journal columns."""
    return {
        "_source_batch_sequence": "_batch_sequence",
        "_source_change_id": "_change_id",
    }.get(column, column)


def _full_rebuild_transfer_id(dataset_id: str, last_sequence: int) -> uuid.UUID:
    """Give corrected rebuilds a fresh checkpoint identity while keeping retries stable."""
    return uuid.uuid5(
        uuid.NAMESPACE_URL,
        f"ampere-bronze-rebuild:{dataset_id}:{last_sequence}:contract-v4:{BRONZE_REBUILD_GENERATION}",
    )


def _bronze_rows(table: str, changes: DataFrame, transfer: dict) -> DataFrame:
    spec = CONTRACT.table("bronze", table)
    validate_spark_table(_SESSION, "iceberg_bronze", spec)
    raw_spec = CONTRACT.table("raw", table)
    business = raw_spec.ingestion["business_columns"]
    expressions = []
    for column in spec.columns:
        name = column["name"]
        if name in business or name in {
            "_dataset_id", "_source_row_id", "_source_batch_sequence", "_source_change_id",
            "_ingest_run_id", "_ingest_ts", "_source_system", "_source_table", "_op",
        }:
            source_name = _raw_source_column_for_bronze(name)
            expr = F.col(quote_identifier(source_name)) if source_name in changes.columns else F.lit(None)
        elif name == "_row_hash":
            expr = F.sha2(F.to_json(F.struct(*[F.col(quote_identifier(c)) for c in business])), 256)
        elif name == "_bronze_last_run_id":
            expr = F.lit(transfer["transfer_id"])
        elif name == "_bronze_last_apply_ts":
            expr = F.current_timestamp().cast("string")
        elif name == "_bronze_last_manifest_path":
            expr = F.lit(transfer["transfer_id"])
        else:
            expr = F.lit(None)
        expressions.append(expr.cast(column["type_text"]).alias(name))
    return changes.select(*expressions)


def _merge_incremental(
    spark: SparkSession, target: str, table: str, changes: DataFrame, transfer: dict
) -> None:
    changes = changes.persist(StorageLevel.DISK_ONLY)
    try:
        latest = _latest_changes(changes).persist(StorageLevel.DISK_ONLY)
        try:
            keys = ["_dataset_id", "_source_row_id"]
            condition = " AND ".join(
                f"t.{quote_identifier(key)}=s.{quote_identifier(key)}" for key in keys
            )
            version = (
                "s._source_batch_sequence > t._source_batch_sequence OR "
                "(s._source_batch_sequence=t._source_batch_sequence AND "
                "s._source_change_id > t._source_change_id)"
            )
            deletes = latest.filter(F.col("_op") == "D").select(
                "_dataset_id", "_source_row_id", "_source_batch_sequence", "_source_change_id"
            )
            if not deletes.rdd.isEmpty():
                deletes.createOrReplaceTempView("bronze_delete_source")
                spark.sql(
                    f"MERGE INTO {target} t USING bronze_delete_source s ON {condition} "
                    f"WHEN MATCHED AND ({version}) THEN DELETE"
                )
            upserts = _bronze_rows(table, latest.filter(F.col("_op") != "D"), transfer)
            if not upserts.rdd.isEmpty():
                upserts.createOrReplaceTempView("bronze_upsert_source")
                names = [column["name"] for column in CONTRACT.table("bronze", table).columns]
                assignments = ", ".join(f"{quote_identifier(name)}=s.{quote_identifier(name)}" for name in names)
                insert_names = ", ".join(quote_identifier(name) for name in names)
                insert_values = ", ".join(f"s.{quote_identifier(name)}" for name in names)
                spark.sql(
                    f"MERGE INTO {target} t USING bronze_upsert_source s ON {condition} "
                    f"WHEN MATCHED AND ({version}) THEN UPDATE SET {assignments} "
                    f"WHEN NOT MATCHED THEN INSERT ({insert_names}) VALUES ({insert_values})"
                )
        finally:
            latest.unpersist()
    finally:
        changes.unpersist()


def _apply_full_rebuild(spark: SparkSession, target: str, table: str, changes: DataFrame, transfer: dict) -> None:
    latest = _latest_changes(changes).filter(F.col("_op") != "D")
    final_rows = _bronze_rows(table, latest, transfer)
    # One Iceberg overwrite commit atomically replaces the table contents.
    final_rows.writeTo(target).overwrite(F.lit(True))


def _mark_table_complete(transfer_id: str, table: str) -> None:
    with _pg_connect() as connection, connection.cursor() as cursor:
        cursor.execute(
            "INSERT INTO pipeline.bronze_transfer_tables(transfer_id,source_table,status) "
            "VALUES (%s,%s,'completed') ON CONFLICT (transfer_id,source_table) DO NOTHING",
            (transfer_id, table),
        )


def _complete_transfer(transfer: dict) -> None:
    with _pg_connect() as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT count(*) FROM pipeline.bronze_transfer_tables "
            "WHERE transfer_id=%s AND status='completed'",
            (transfer["transfer_id"],),
        )
        if cursor.fetchone()[0] != len(TABLES):
            raise RuntimeError("Bronze transfer is missing one or more completed tables")
        cursor.execute(
            "UPDATE pipeline.bronze_transfers SET status='completed',completed_at=clock_timestamp() "
            "WHERE transfer_id=%s",
            (transfer["transfer_id"],),
        )
        cursor.execute(
            "INSERT INTO pipeline.bronze_apply_state "
            "(state_key,dataset_id,last_batch_sequence,last_transfer_id,completed_at) "
            "VALUES (true,%s,%s,%s,clock_timestamp()) "
            "ON CONFLICT (state_key) DO UPDATE SET dataset_id=EXCLUDED.dataset_id, "
            "last_batch_sequence=EXCLUDED.last_batch_sequence, "
            "last_transfer_id=EXCLUDED.last_transfer_id,completed_at=EXCLUDED.completed_at",
            (transfer["dataset_id"], transfer["last_sequence"], transfer["transfer_id"]),
        )


_SESSION: SparkSession


def run(
    *,
    rebuild_all: bool,
    warehouse: str,
    bronze_warehouse: str,
    uri: str,
    oauth_uri: str,
    scope: str,
) -> None:
    global _SESSION
    transfer = reserve_full_rebuild() if rebuild_all else reserve_incremental_transfer()
    if transfer is None:
        LOGGER.info("No complete Raw transfer is waiting for Bronze")
        return
    spark = _SESSION
    minio = get_minio_connection_settings()
    configure_s3(spark, minio.endpoint, minio.access_key, minio.secret_key)
    configure_lakekeeper_catalog(
        spark,
        catalog="iceberg_raw",
        warehouse=warehouse,
        uri=uri,
        oauth_uri=oauth_uri,
        scope=scope,
        client_id=get_env("LAKEKEEPER_CLIENT_ID"),
        client_secret=get_env("LAKEKEEPER_CLIENT_SECRET"),
        minio_endpoint=os.environ["MINIO_S3_ENDPOINT"],
    )
    configure_lakekeeper_catalog(
        spark,
        catalog="iceberg_bronze",
        warehouse=bronze_warehouse,
        uri=uri,
        oauth_uri=oauth_uri,
        scope=scope,
        client_id=get_env("LAKEKEEPER_CLIENT_ID"),
        client_secret=get_env("LAKEKEEPER_CLIENT_SECRET"),
        minio_endpoint=os.environ["MINIO_S3_ENDPOINT"],
    )
    for table in TABLES:
        with _pg_connect() as connection, connection.cursor() as cursor:
            cursor.execute(
                "SELECT 1 FROM pipeline.bronze_transfer_tables "
                "WHERE transfer_id=%s AND source_table=%s AND status='completed'",
                (transfer["transfer_id"], table),
            )
            if cursor.fetchone():
                continue
        raw = spark.table(f"iceberg_raw.raw.{quote_identifier(table)}").filter(
            (F.col("_dataset_id") == transfer["dataset_id"])
            & F.col("_transfer_id").isin(transfer["transfer_ids"])
        ).persist(StorageLevel.DISK_ONLY)
        target = f"iceberg_bronze.bronze.{quote_identifier(table)}"
        try:
            actual_count = raw.count()
            expected_count = transfer["expected_counts"].get(table, 0)
            if actual_count != expected_count:
                raise RuntimeError(
                    f"Raw table count mismatch for {table}: "
                    f"registry={expected_count}, Iceberg={actual_count}"
                )
            if transfer["rebuild"]:
                _apply_full_rebuild(spark, target, table, raw, transfer)
            else:
                _merge_incremental(spark, target, table, raw, transfer)
            _mark_table_complete(transfer["transfer_id"], table)
        finally:
            raw.unpersist()
        LOGGER.info("BRONZE_TABLE_COMPLETE table=%s rebuild=%s", table, transfer["rebuild"])
    _complete_transfer(transfer)
    LOGGER.info(
        "BRONZE_TRANSFER_COMPLETE transfer_id=%s dataset=%s boundary=%s",
        transfer["transfer_id"], transfer["dataset_id"], transfer["last_sequence"],
    )


def run_cli() -> None:
    import argparse

    from pyspark.sql import SparkSession

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rebuild-all", action="store_true")
    parser.add_argument("--warehouse", default="raw")
    parser.add_argument("--bronze-warehouse", default="bronze")
    parser.add_argument("--lakekeeper-uri", required=True)
    parser.add_argument("--oauth-uri", required=True)
    parser.add_argument("--scope", required=True)
    parser.add_argument("--app-name", default="ampere-raw-to-bronze")
    args = parser.parse_args()
    spark = (
        SparkSession.builder.appName(args.app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .getOrCreate()
    )
    global _SESSION
    _SESSION = spark
    try:
        run(
            rebuild_all=args.rebuild_all,
            warehouse=args.warehouse,
            bronze_warehouse=args.bronze_warehouse,
            uri=args.lakekeeper_uri,
            oauth_uri=args.oauth_uri,
            scope=args.scope,
        )
    finally:
        spark.stop()
