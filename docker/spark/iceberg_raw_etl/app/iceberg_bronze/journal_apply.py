"""Apply accepted Raw change batches to Bronze as versioned current state."""

from __future__ import annotations

import logging
import os
import time
import uuid

import psycopg
from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.window import Window

from etl_utils import (
    configure_s3,
    get_env,
    get_minio_connection_settings,
    get_postgres_connection_settings,
    set_spark_log_level,
    setup_logging,
)
from iceberg_bronze.catalog import CONTRACT, configure_lakekeeper_catalog
from tools.contracts.spark_conformance import quote_identifier, validate_spark_table

LOGGER = logging.getLogger("raw-journal-to-bronze")
TABLES = tuple(spec.name for spec in CONTRACT.layer_tables("raw"))
BRONZE_REBUILD_GENERATION = "lineage-v1"
# Keep large replay batches bounded while avoiding repeated Iceberg MERGEs for
# small batches. The row target is approximate because hash buckets can skew.
SOURCE_ROW_BUCKETS = 8
SOURCE_ROWS_PER_BUCKET = 16_000


def _pg_connect():
    settings = get_postgres_connection_settings()
    return psycopg.connect(
        host=settings.host,
        port=settings.port,
        dbname=settings.database,
        user=settings.user,
        password=settings.password,
        application_name="ampere-iceberg-raw-to-bronze",
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
            "SELECT 1 FROM pipeline.bronze_transfers bt "
            "WHERE bt.status='started' AND bt.dataset_id=%s "
            "AND NOT EXISTS (SELECT 1 FROM pipeline.raw_transfers rt "
            "WHERE rt.transfer_id=bt.transfer_id) LIMIT 1",
            (state[1],),
        )
        if cursor.fetchone():
            raise RuntimeError(
                "A Bronze full rebuild is in progress; finish or retry it before daily apply"
            )
        cursor.execute(
            "SELECT bt.transfer_id,bt.dataset_id,bt.first_batch_sequence,bt.last_batch_sequence "
            "FROM pipeline.bronze_transfers bt WHERE bt.status='started' AND bt.dataset_id=%s "
            "AND EXISTS (SELECT 1 FROM pipeline.raw_transfers rt "
            "WHERE rt.transfer_id=bt.transfer_id) "
            "ORDER BY bt.first_batch_sequence LIMIT 1",
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
        cursor.execute(
            "SELECT batch_id,batch_sequence FROM pipeline.raw_transfer_batches "
            "WHERE transfer_id=%s ORDER BY batch_sequence",
            (transfer_id,),
        )
        batches = [{"batch_id": str(batch_id), "batch_sequence": int(sequence)} for batch_id, sequence in cursor.fetchall()]
        if not batches:
            raise RuntimeError(f"Completed Raw transfer {transfer_id} has no source batches")
        cursor.execute(
            "SELECT c.batch_id,c.source_table,c.change_count "
            "FROM pipeline.batch_table_counts c JOIN pipeline.raw_transfer_batches b "
            "ON b.dataset_id=c.dataset_id AND b.batch_id=c.batch_id WHERE b.transfer_id=%s",
            (transfer_id,),
        )
        batch_counts = {item["batch_id"]: {} for item in batches}
        for batch_id, table, count in cursor.fetchall():
            batch_counts[str(batch_id)][str(table)] = int(count)
        return {
            "transfer_id": str(transfer_id),
            "dataset_id": str(dataset_id),
            "first_sequence": int(first_sequence),
            "last_sequence": int(last_sequence),
            "transfer_ids": [str(transfer_id)],
            "expected_counts": expected_counts,
            "batches": batches,
            "batch_counts": batch_counts,
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
            "SELECT 1 FROM pipeline.bronze_transfers bt "
            "WHERE bt.status='started' AND bt.dataset_id=%s "
            "AND EXISTS (SELECT 1 FROM pipeline.raw_transfers rt "
            "WHERE rt.transfer_id=bt.transfer_id) LIMIT 1",
            (dataset_id,),
        )
        if cursor.fetchone():
            raise RuntimeError(
                "A daily Bronze transfer is in progress; finish or retry it before full rebuild"
            )
        cursor.execute(
            "SELECT transfer_id,first_batch_sequence,last_batch_sequence "
            "FROM pipeline.bronze_transfers bt "
            "WHERE bt.status='started' AND bt.dataset_id=%s "
            "AND NOT EXISTS (SELECT 1 FROM pipeline.raw_transfers rt "
            "WHERE rt.transfer_id=bt.transfer_id) "
            "ORDER BY last_batch_sequence DESC LIMIT 1",
            (dataset_id,),
        )
        in_progress = cursor.fetchone()
        if in_progress is not None:
            rebuild_id, first_sequence, last_sequence = in_progress
            cursor.execute(
                "SELECT transfer_id FROM pipeline.raw_transfers "
                "WHERE dataset_id=%s AND status='completed' "
                "AND first_batch_sequence >= %s AND last_batch_sequence <= %s "
                "ORDER BY first_batch_sequence",
                (dataset_id, first_sequence, last_sequence),
            )
            ids = [row[0] for row in cursor.fetchall()]
            if not ids:
                raise RuntimeError(
                    f"Started Bronze rebuild {rebuild_id} has no accepted Raw transfers"
                )
            cursor.execute(
                "SELECT source_table,SUM(row_count)::bigint FROM pipeline.raw_transfer_tables "
                "WHERE transfer_id = ANY(%s) GROUP BY source_table",
                (ids,),
            )
            expected_counts = {table: int(count) for table, count in cursor.fetchall()}
            cursor.execute(
                "SELECT rtb.transfer_id,rtb.batch_id,rtb.batch_sequence "
                "FROM pipeline.raw_transfer_batches rtb WHERE rtb.transfer_id=ANY(%s) "
                "ORDER BY rtb.batch_sequence",
                (ids,),
            )
            batches = [
                {"raw_transfer_id": str(raw_id), "batch_id": str(batch_id), "batch_sequence": int(sequence)}
                for raw_id, batch_id, sequence in cursor.fetchall()
            ]
            cursor.execute(
                "SELECT c.batch_id,c.source_table,c.change_count "
                "FROM pipeline.batch_table_counts c JOIN pipeline.raw_transfer_batches b "
                "ON b.dataset_id=c.dataset_id AND b.batch_id=c.batch_id "
                "WHERE b.transfer_id=ANY(%s)",
                (ids,),
            )
            batch_counts = {item["batch_id"]: {} for item in batches}
            for batch_id, table, count in cursor.fetchall():
                batch_counts[str(batch_id)][str(table)] = int(count)
            return {
                "transfer_id": str(rebuild_id),
                "dataset_id": str(dataset_id),
                "first_sequence": int(first_sequence),
                "last_sequence": int(last_sequence),
                "transfer_ids": [str(value) for value in ids],
                "expected_counts": expected_counts,
                "batches": batches,
                "batch_counts": batch_counts,
                "rebuild": True,
            }
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
        cursor.execute(
            "SELECT rtb.transfer_id,rtb.batch_id,rtb.batch_sequence "
            "FROM pipeline.raw_transfer_batches rtb WHERE rtb.transfer_id=ANY(%s) "
            "ORDER BY rtb.batch_sequence",
            (ids,),
        )
        batches = [
            {"raw_transfer_id": str(raw_id), "batch_id": str(batch_id), "batch_sequence": int(sequence)}
            for raw_id, batch_id, sequence in cursor.fetchall()
        ]
        cursor.execute(
            "SELECT c.batch_id,c.source_table,c.change_count "
            "FROM pipeline.batch_table_counts c JOIN pipeline.raw_transfer_batches b "
            "ON b.dataset_id=c.dataset_id AND b.batch_id=c.batch_id "
            "WHERE b.transfer_id=ANY(%s)",
            (ids,),
        )
        batch_counts = {item["batch_id"]: {} for item in batches}
        for batch_id, table, count in cursor.fetchall():
            batch_counts[str(batch_id)][str(table)] = int(count)
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
            "batches": batches,
            "batch_counts": batch_counts,
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


def _bronze_delete_rows(latest: DataFrame) -> DataFrame:
    """Project Raw ordering metadata under the names used by Bronze."""
    return latest.filter(F.col("_op") == "D").select(
        "_dataset_id",
        "_source_row_id",
        F.col("_batch_sequence").alias("_source_batch_sequence"),
        F.col("_change_id").alias("_source_change_id"),
    )


def _materialize_bounded_merge_source(changes: DataFrame, staging_path: str) -> DataFrame:
    """Materialize one bounded hash slice in shared storage for Iceberg MERGE."""
    try:
        changes.write.mode("errorifexists").parquet(staging_path)
        return changes.sparkSession.read.schema(changes.schema).parquet(staging_path)
    except Exception:
        _delete_merge_source_path(changes.sparkSession, staging_path)
        raise


def _merge_scratch_root(transfer: dict) -> str:
    bucket = os.getenv("MINIO_STAGING_BUCKET", "ampere-iceberg-raw")
    return f"s3a://{bucket}/_spark_scratch/bronze_merge/{transfer['transfer_id']}"


def _merge_source_path(transfer: dict, table: str, source_slice: str, operation: str) -> str:
    """Create a unique temporary MinIO path for one bounded merge source."""
    return (
        f"{_merge_scratch_root(transfer)}/{table}/{source_slice}/"
        f"{operation}-{uuid.uuid4().hex}"
    )


def _delete_merge_source_path(spark: SparkSession, staging_path: str) -> None:
    """Remove a temporary merge source from MinIO, logging cleanup failures."""
    try:
        path = spark._jvm.org.apache.hadoop.fs.Path(staging_path)
        filesystem = path.getFileSystem(spark._jsc.hadoopConfiguration())
        if filesystem.exists(path) and not filesystem.delete(path, True):
            LOGGER.warning("BRONZE_MERGE_SOURCE_CLEANUP_FAILED path=%s", staging_path)
    except Exception:
        LOGGER.warning(
            "BRONZE_MERGE_SOURCE_CLEANUP_FAILED path=%s",
            staging_path,
            exc_info=True,
        )


def _source_bucket_count(source_rows: int) -> int:
    """Use fewer slices for small batches and cap hot batches at eight slices."""
    return min(
        SOURCE_ROW_BUCKETS,
        max(1, (source_rows + SOURCE_ROWS_PER_BUCKET - 1) // SOURCE_ROWS_PER_BUCKET),
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
        f"ampere-bronze-rebuild:{dataset_id}:{last_sequence}:contract-v6:{BRONZE_REBUILD_GENERATION}",
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
    spark: SparkSession,
    target: str,
    table: str,
    changes: DataFrame,
    transfer: dict,
    source_slice: str,
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
            deletes = _bronze_delete_rows(latest)
            if not deletes.rdd.isEmpty():
                delete_path = _merge_source_path(transfer, table, source_slice, "delete")
                deletes = _materialize_bounded_merge_source(deletes, delete_path)
                try:
                    deletes.createOrReplaceTempView("bronze_delete_source")
                    spark.sql(
                        f"MERGE INTO {target} t USING bronze_delete_source s ON {condition} "
                        f"WHEN MATCHED AND ({version}) THEN DELETE"
                    )
                finally:
                    try:
                        spark.catalog.dropTempView("bronze_delete_source")
                    finally:
                        _delete_merge_source_path(spark, delete_path)
            upserts = _bronze_rows(table, latest.filter(F.col("_op") != "D"), transfer)
            if not upserts.rdd.isEmpty():
                upsert_path = _merge_source_path(transfer, table, source_slice, "upsert")
                upserts = _materialize_bounded_merge_source(upserts, upsert_path)
                try:
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
                    try:
                        spark.catalog.dropTempView("bronze_upsert_source")
                    finally:
                        _delete_merge_source_path(spark, upsert_path)
        finally:
            latest.unpersist(blocking=True)
    finally:
        changes.unpersist(blocking=True)


def _apply_batch_slices(
    spark: SparkSession,
    target: str,
    table: str,
    transfer: dict,
    batch: dict,
) -> int:
    """Apply one source batch in disjoint source-row buckets."""
    expected = transfer["batch_counts"].get(batch["batch_id"], {}).get(table)
    if expected is None:
        raise RuntimeError(f"Missing {table} count for completed source batch {batch['batch_id']}")
    if expected == 0:
        return 0
    raw_batch = spark.table(f"iceberg_raw.raw.{quote_identifier(table)}").filter(
        (F.col("_dataset_id") == transfer["dataset_id"])
        & (F.col("_batch_id") == batch["batch_id"])
        & (F.col("_batch_sequence") == batch["batch_sequence"])
    ).persist(StorageLevel.DISK_ONLY)
    try:
        actual = raw_batch.count()
        if actual != expected:
            raise RuntimeError(
                f"Raw batch count mismatch table={table} batch={batch['batch_id']} "
                f"control={expected} Iceberg={actual}"
            )
        bucket_count = _source_bucket_count(expected)
        bucket_expr = F.pmod(F.xxhash64(F.col("_source_row_id")), F.lit(bucket_count))
        buckets = [int(row[0]) for row in raw_batch.select(bucket_expr.alias("bucket")).distinct().collect()]
        for bucket in sorted(buckets):
            changes = raw_batch.filter(bucket_expr == F.lit(bucket))
            source_slice = f"batch-{batch['batch_sequence']}-bucket-{bucket}"
            _merge_incremental(spark, target, table, changes, transfer, source_slice)
            LOGGER.info(
                "BRONZE_BATCH_CHUNK_COMPLETE table=%s batch=%s bucket=%s/%s rows_in_batch=%s",
                table, batch["batch_sequence"], bucket, bucket_count, actual,
            )
        return actual
    finally:
        raw_batch.unpersist(blocking=True)


def _apply_full_rebuild(spark: SparkSession, target: str, table: str, transfer: dict) -> int:
    """Clear one target table, then replay accepted batches chronologically."""
    # If this table has no completion marker, a retry starts it cleanly and
    # replays the complete frozen batch list. Other completed tables stay put.
    spark.sql(f"DELETE FROM {target}")
    applied = 0
    previous_sequence = None
    for batch in transfer["batches"]:
        sequence = int(batch["batch_sequence"])
        if previous_sequence is not None and sequence <= previous_sequence:
            raise RuntimeError("Frozen Raw batch boundary is not strictly ascending")
        previous_sequence = sequence
        rows = _apply_batch_slices(spark, target, table, transfer, batch)
        applied += rows
    if applied != transfer["expected_counts"].get(table, 0):
        raise RuntimeError(
            f"Bronze rebuild input count mismatch for {table}: "
            f"batch ledger={transfer['expected_counts'].get(table, 0)} applied={applied}"
        )
    return applied


def _apply_incremental_table(spark: SparkSession, target: str, table: str, transfer: dict) -> int:
    applied = 0
    for batch in transfer["batches"]:
        applied += _apply_batch_slices(spark, target, table, transfer, batch)
    if applied != transfer["expected_counts"].get(table, 0):
        raise RuntimeError(
            f"Bronze incremental input count mismatch for {table}: "
            f"transfer ledger={transfer['expected_counts'].get(table, 0)} applied={applied}"
        )
    return applied


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
    tables: list[str],
    complete_transfer: bool,
    warehouse: str,
    bronze_warehouse: str,
    uri: str,
    oauth_uri: str,
    scope: str,
) -> None:
    global _SESSION
    if not tables:
        raise ValueError("At least one Bronze table must be selected")
    unknown_tables = sorted(set(tables) - set(TABLES))
    if unknown_tables:
        raise ValueError(f"Unknown Raw-to-Bronze tables: {unknown_tables}")
    if len(tables) != len(set(tables)):
        raise ValueError("Raw-to-Bronze table selection contains duplicates")
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
    _delete_merge_source_path(spark, _merge_scratch_root(transfer))
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
    LOGGER.info(
        "BRONZE_GROUP_START transfer_id=%s dataset=%s boundary=%s batches=%s "
        "tables=%s rebuild=%s finalize=%s",
        transfer["transfer_id"],
        transfer["dataset_id"],
        transfer["last_sequence"],
        len(transfer["transfer_ids"]),
        ",".join(tables),
        transfer["rebuild"],
        complete_transfer,
    )
    for table in tables:
        with _pg_connect() as connection, connection.cursor() as cursor:
            cursor.execute(
                "SELECT 1 FROM pipeline.bronze_transfer_tables "
                "WHERE transfer_id=%s AND source_table=%s AND status='completed'",
                (transfer["transfer_id"], table),
            )
            if cursor.fetchone():
                LOGGER.info(
                    "BRONZE_TABLE_SKIP table=%s reason=already_completed transfer_id=%s",
                    table,
                    transfer["transfer_id"],
                )
                continue
        table_started = time.monotonic()
        expected_count = transfer["expected_counts"].get(table, 0)
        LOGGER.info(
            "BRONZE_TABLE_START table=%s mode=%s expected_raw_rows=%s",
            table,
            "full_rebuild" if transfer["rebuild"] else "incremental",
            expected_count,
        )
        target = f"iceberg_bronze.bronze.{quote_identifier(table)}"
        try:
            if transfer["rebuild"]:
                actual_count = _apply_full_rebuild(spark, target, table, transfer)
            else:
                actual_count = _apply_incremental_table(spark, target, table, transfer)
            LOGGER.info(
                "BRONZE_TABLE_SOURCE_VALIDATED table=%s raw_rows=%s expected_rows=%s",
                table, actual_count, expected_count,
            )
            _mark_table_complete(transfer["transfer_id"], table)
        except Exception:
            LOGGER.exception("BRONZE_TABLE_FAILED table=%s", table)
            raise
        LOGGER.info(
            "BRONZE_TABLE_COMPLETE table=%s rebuild=%s raw_rows=%s elapsed_seconds=%.1f",
            table,
            transfer["rebuild"],
            actual_count,
            time.monotonic() - table_started,
        )
    if complete_transfer:
        _complete_transfer(transfer)
        LOGGER.info(
            "BRONZE_TRANSFER_COMPLETE transfer_id=%s dataset=%s boundary=%s tables=%s",
            transfer["transfer_id"],
            transfer["dataset_id"],
            transfer["last_sequence"],
            len(TABLES),
        )
    LOGGER.info(
        "BRONZE_GROUP_COMPLETE transfer_id=%s tables=%s finalized=%s",
        transfer["transfer_id"],
        len(tables),
        complete_transfer,
    )


def run_cli() -> None:
    import argparse

    from pyspark.sql import SparkSession

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rebuild-all", action="store_true")
    parser.add_argument("--tables", nargs="+", required=True)
    parser.add_argument("--complete-transfer", action="store_true")
    parser.add_argument("--warehouse", default="raw")
    parser.add_argument("--bronze-warehouse", default="bronze")
    parser.add_argument("--lakekeeper-uri", required=True)
    parser.add_argument("--oauth-uri", required=True)
    parser.add_argument("--scope", required=True)
    parser.add_argument("--app-name", default="ampere-iceberg-raw-to-bronze")
    setup_logging()
    args = parser.parse_args()
    LOGGER.info(
        "BRONZE_JOB_START mode=%s tables=%s complete_transfer=%s",
        "full_rebuild" if args.rebuild_all else "incremental",
        ",".join(args.tables),
        args.complete_transfer,
    )
    spark = (
        SparkSession.builder.appName(args.app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .getOrCreate()
    )
    set_spark_log_level(spark)
    global _SESSION
    _SESSION = spark
    try:
        run(
            rebuild_all=args.rebuild_all,
            tables=args.tables,
            complete_transfer=args.complete_transfer,
            warehouse=args.warehouse,
            bronze_warehouse=args.bronze_warehouse,
            uri=args.lakekeeper_uri,
            oauth_uri=args.oauth_uri,
            scope=args.scope,
        )
    finally:
        spark.stop()
