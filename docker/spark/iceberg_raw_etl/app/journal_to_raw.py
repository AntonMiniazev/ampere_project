"""Transfer compact change metadata plus source after-images to Raw Iceberg."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import uuid
from datetime import datetime, timedelta, timezone

import boto3
import psycopg
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

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
CONTRACT_VERSION = 6
DEFAULT_STAGING_PREFIX = "ampere/source-batches/v1"
TARGET_INPUT_BYTES = 512 * 1024 * 1024
MAX_FILES_PER_CHUNK = 128


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
                f"ampere-iceberg-raw:{dataset_id}:{first_sequence}:{last_sequence}:contract-v6",
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
            "SELECT rtb.batch_id,c.source_table,c.change_count "
            "FROM pipeline.raw_transfer_batches rtb "
            "JOIN pipeline.batch_table_counts c "
            "ON c.dataset_id=rtb.dataset_id AND c.batch_id=rtb.batch_id "
            "WHERE rtb.transfer_id=%s ORDER BY rtb.batch_sequence,c.source_table",
            (transfer_id,),
        )
        batch_counts = {}
        for batch_id, table, count in cursor.fetchall():
            batch_counts.setdefault(str(batch_id), {})[str(table)] = int(count)
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
            "batch_counts": batch_counts,
            "counts": {table: counts.get(table, 0) for table in SOURCE_TABLES},
        }


def _raw_table_name(table: str) -> str:
    name = f"iceberg_raw.raw.{table}"
    spec = CONTRACT.table("raw", table)
    validate_spark_table(_SPARK, "iceberg_raw", spec)
    return ".".join(quote_identifier(part) for part in name.split("."))


def _staging_client():
    minio = get_minio_connection_settings()
    return boto3.client(
        "s3",
        endpoint_url=minio.endpoint,
        aws_access_key_id=minio.access_key,
        aws_secret_access_key=minio.secret_key,
        region_name="us-east-1",
    )


def _load_eligible_manifests(transfer: dict) -> dict:
    """Validate DB-completed batches and their exact immutable manifests."""
    client = _staging_client()
    bucket = os.getenv("MINIO_STAGING_BUCKET", "ampere-iceberg-raw")
    prefix = os.getenv("MINIO_STAGING_PREFIX", DEFAULT_STAGING_PREFIX).strip("/")
    manifests = {}
    previous_sequence = 0
    for batch_id, sequence in transfer["batches"]:
        if int(sequence) <= previous_sequence:
            raise RuntimeError("Raw transfer batch order is not strictly ascending")
        previous_sequence = int(sequence)
        if set(transfer["batch_counts"].get(batch_id, {})) != set(SOURCE_TABLES):
            raise RuntimeError(f"PostgreSQL batch ledger does not cover every source table for {batch_id}")
        batch_root = f"{prefix}/datasets/{transfer['dataset_id']}/batches/{sequence:020d}-{batch_id}"
        marker = json.loads(client.get_object(
            Bucket=bucket, Key=f"{batch_root}/COMMITTED.json"
        )["Body"].read())
        manifest_key = f"{batch_root}/manifest.json"
        if marker.get("manifest_key") != manifest_key or marker.get("postgres_committed") is not True:
            raise RuntimeError(f"Batch {batch_id} has no valid PostgreSQL commit marker in staging")
        manifest_bytes = client.get_object(Bucket=bucket, Key=manifest_key)["Body"].read()
        if marker.get("manifest_sha256") != hashlib.sha256(manifest_bytes).hexdigest():
            raise RuntimeError(f"Manifest checksum mismatch for completed batch {batch_id}")
        manifest = json.loads(manifest_bytes)
        expected = {
            "manifest_version": 1,
            "contract_version": CONTRACT_VERSION,
            "dataset_id": transfer["dataset_id"],
            "batch_id": batch_id,
            "batch_sequence": sequence,
        }
        if any(manifest.get(name) != value for name, value in expected.items()):
            raise RuntimeError(f"Staging manifest identity mismatch for batch {batch_id}")
        if set(manifest.get("tables", {})) != set(SOURCE_TABLES):
            raise RuntimeError(f"Staging manifest {batch_id} does not cover all 16 source tables")
        for table in SOURCE_TABLES:
            table_info = manifest["tables"][table]
            files = table_info.get("files", [])
            manifest_count = int(table_info.get("row_count", -1))
            if manifest_count != transfer["batch_counts"][batch_id][table]:
                raise RuntimeError(
                    f"Manifest/control count mismatch batch={batch_id} table={table} "
                    f"manifest={manifest_count} ledger={transfer['batch_counts'][batch_id][table]}"
                )
            if sum(int(item.get("rows", -1)) for item in files) != manifest_count:
                raise RuntimeError(f"Manifest file row counts do not match batch={batch_id} table={table}")
            operations = table_info.get("operation_counts", {})
            if set(operations) != {"B", "I", "U", "D"} or sum(map(int, operations.values())) != manifest_count:
                raise RuntimeError(f"Manifest operation counts do not match batch={batch_id} table={table}")
            previous_change_id = 0
            for item in files:
                if not item.get("key", "").startswith(f"{batch_root}/{table}/"):
                    raise RuntimeError(f"Manifest file escaped its batch/table prefix: {item.get('key')}")
                if len(str(item.get("sha256", ""))) != 64 or int(item.get("bytes", 0)) <= 0:
                    raise RuntimeError(f"Manifest file checksum/size is invalid for batch={batch_id} table={table}")
                stored = client.head_object(Bucket=bucket, Key=item["key"])
                metadata = {str(key).lower(): value for key, value in stored.get("Metadata", {}).items()}
                if int(stored.get("ContentLength", -1)) != int(item["bytes"]):
                    raise RuntimeError(f"Staged Parquet object size mismatch: {item['key']}")
                if metadata.get("sha256") != item["sha256"]:
                    raise RuntimeError(f"Staged Parquet object checksum metadata mismatch: {item['key']}")
                if int(item.get("first_change_id", 0)) != previous_change_id + 1:
                    raise RuntimeError(f"Manifest change ordering has a gap for batch={batch_id} table={table}")
                if int(item.get("last_change_id", 0)) - int(item["first_change_id"]) + 1 != int(item["rows"]):
                    raise RuntimeError(f"Manifest change ID range is invalid for batch={batch_id} table={table}")
                previous_change_id = int(item["last_change_id"])
            if sum(int(item["bytes"]) for item in files) != int(table_info.get("byte_count", -1)):
                raise RuntimeError(f"Manifest file byte counts do not match batch={batch_id} table={table}")
        manifests[batch_id] = {"root": batch_root, **manifest}
    return manifests


_SPARK: SparkSession


def _merge_staged_changes(spark: SparkSession, transfer: dict, table: str, manifests: dict) -> int:
    """Read immutable mutation-time Parquet and merge it into Raw in bounded chunks."""
    expected_count = transfer["counts"][table]
    if expected_count == 0:
        return 0

    spec = CONTRACT.table("raw", table)
    batch_table = [
        (batch_id, manifests[batch_id]["tables"][table])
        for batch_id, _sequence in transfer["batches"]
    ]
    target_input_bytes = int(os.getenv("AMPERE_RAW_TARGET_INPUT_BYTES", str(TARGET_INPUT_BYTES)))
    max_files_per_chunk = int(os.getenv("AMPERE_RAW_MAX_FILES_PER_CHUNK", str(MAX_FILES_PER_CHUNK)))
    if target_input_bytes < 1 or max_files_per_chunk < 1:
        raise ValueError("Raw target input bytes and max files per chunk must be positive")
    file_groups: list[tuple[list[str], int]] = []
    active: list[str] = []
    active_bytes = 0
    bucket = os.getenv("MINIO_STAGING_BUCKET", "ampere-iceberg-raw")
    for _batch_id, table_info in batch_table:
        for item in table_info["files"]:
            active.append(f"s3a://{bucket}/{item['key']}")
            active_bytes += int(item["bytes"])
            if len(active) >= max_files_per_chunk or active_bytes >= target_input_bytes:
                file_groups.append((active, active_bytes))
                active = []
                active_bytes = 0
    if active:
        file_groups.append((active, active_bytes))
    columns = [column["name"] for column in spec.columns]
    keys = spec.ingestion["change_keys"]
    condition = " AND ".join(
        f"t.{quote_identifier(key)} = s.{quote_identifier(key)}" for key in keys
    )
    names = ", ".join(quote_identifier(column) for column in columns)
    values = ", ".join(f"s.{quote_identifier(column)}" for column in columns)
    actual_count = 0
    target = _raw_table_name(table)
    for chunk_number, (file_paths, chunk_bytes) in enumerate(file_groups, start=1):
        source = spark.read.parquet(*file_paths)
        # Parquet carries exact source physical types. Cast the business/event
        # fields to the catalog contract before MERGE (notably timestamp_ntz
        # source values to the Raw timestamp type) under the UTC Spark session.
        for column in spec.columns:
            name = column["name"]
            if name in source.columns:
                source = source.withColumn(
                    name,
                    F.col(quote_identifier(name)).cast(column["type_text"]),
                )
        source = (
            source
            .withColumn("_transfer_id", F.lit(transfer["transfer_id"]))
            .withColumn("_ingest_run_id", F.lit("ampere-iceberg-raw-transfer"))
            .withColumn("_ingest_ts", F.current_timestamp())
            .withColumn("_source_system", F.lit("postgres-pre-raw"))
            .withColumn("_source_table", F.lit(table))
        )
        source.createOrReplaceTempView("raw_transfer_source")
        chunk_count = source.count()
        actual_count += chunk_count
        spark.sql(
            f"MERGE INTO {target} t USING raw_transfer_source s ON {condition} "
            f"WHEN NOT MATCHED THEN INSERT ({names}) VALUES ({values})"
        )
        LOGGER.info(
            "RAW_TABLE_CHUNK_COMPLETE table=%s chunk=%s files=%s bytes=%s rows=%s",
            table, chunk_number, len(file_paths),
            chunk_bytes, chunk_count,
        )
    if actual_count != expected_count:
        raise RuntimeError(
            f"Staged Parquet count mismatch for {table}: control={expected_count}, Spark={actual_count}"
        )
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


def cleanup_acknowledged_staging(*, retention_days: int | None = None) -> int:
    """Remove expired Parquet payload only after the Raw transfer is fully durable."""
    retention = (
        retention_days
        if retention_days is not None
        else int(os.getenv("AMPERE_STAGING_RETENTION_DAYS", "14"))
    )
    if not 7 <= retention <= 14:
        raise ValueError("AMPERE_STAGING_RETENTION_DAYS must be between 7 and 14")
    bucket = os.getenv("MINIO_STAGING_BUCKET", "ampere-iceberg-raw")
    prefix = os.getenv("MINIO_STAGING_PREFIX", DEFAULT_STAGING_PREFIX).strip("/")
    client = _staging_client()
    with _connect_postgres() as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT rtb.dataset_id,rtb.batch_id,rtb.batch_sequence "
            "FROM pipeline.raw_transfers rt JOIN pipeline.raw_transfer_batches rtb "
            "ON rtb.transfer_id=rt.transfer_id WHERE rt.status='completed' "
            "AND rt.completed_at < clock_timestamp() - (%s * interval '1 day') "
            "AND (SELECT count(*) FROM pipeline.raw_transfer_tables t "
            "WHERE t.transfer_id=rt.transfer_id AND t.status='completed')=%s "
            "ORDER BY rt.completed_at LIMIT 50",
            (retention, len(SOURCE_TABLES)),
        )
        expired = cursor.fetchall()
    removed = 0
    paginator = client.get_paginator("list_objects_v2")
    for dataset_id, batch_id, sequence in expired:
        batch_root = (
            f"{prefix}/datasets/{dataset_id}/batches/"
            f"{int(sequence):020d}-{batch_id}"
        )
        keys = [
            item["Key"]
            for page in paginator.paginate(Bucket=bucket, Prefix=f"{batch_root}/")
            for item in page.get("Contents", [])
            if item["Key"].endswith(".parquet")
        ]
        for offset in range(0, len(keys), 1000):
            chunk = keys[offset : offset + 1000]
            if chunk:
                client.delete_objects(
                    Bucket=bucket,
                    Delete={"Objects": [{"Key": key} for key in chunk], "Quiet": True},
                )
                removed += len(chunk)
        LOGGER.info("STAGING_RETENTION_COMPLETE batch=%s parquet_objects=%s", batch_id, len(keys))
    if expired:
        LOGGER.info("STAGING_RETENTION_SUMMARY batches=%s parquet_objects=%s days=%s", len(expired), removed, retention)
    return removed


def cleanup_orphaned_staging(*, retention_days: int = 14, limit_batches: int = 50) -> int:
    """Remove old staging prefixes with no committed PostgreSQL batch record.

    This is deliberately separate from acknowledged-payload retention. Active,
    pending, failed, or partially transferred batches remain protected by their
    compact PostgreSQL batch records.
    """
    if retention_days < 14 or limit_batches < 1:
        raise ValueError("Orphan cleanup requires at least 14 days retention and a positive batch limit")
    bucket = os.getenv("MINIO_STAGING_BUCKET", "ampere-iceberg-raw")
    prefix = os.getenv("MINIO_STAGING_PREFIX", DEFAULT_STAGING_PREFIX).strip("/")
    cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
    client = _staging_client()
    paginator = client.get_paginator("list_objects_v2")
    candidates: dict[tuple[str, str], dict[str, object]] = {}
    for page in paginator.paginate(Bucket=bucket, Prefix=f"{prefix}/datasets/"):
        for item in page.get("Contents", []):
            key = item["Key"]
            relative = key[len(prefix) + 1 :].split("/")
            if len(relative) < 5 or relative[0] != "datasets" or relative[2] != "batches":
                continue
            dataset_id = relative[1]
            batch_token = relative[3]
            _sequence, separator, batch_id = batch_token.partition("-")
            if not separator or not batch_id:
                continue
            candidate = candidates.setdefault(
                (dataset_id, batch_id), {"keys": [], "all_expired": True}
            )
            candidate["keys"].append(key)
            if item["LastModified"] > cutoff:
                candidate["all_expired"] = False

    removed = 0
    deleted_batches = 0
    for (dataset_id, batch_id), candidate in candidates.items():
        if deleted_batches >= limit_batches:
            break
        if not candidate["all_expired"]:
            continue
        keys = candidate["keys"]
        with _connect_postgres() as connection, connection.cursor() as cursor:
            cursor.execute(
                "SELECT 1 FROM pipeline.batches WHERE dataset_id=%s AND batch_id=%s",
                (dataset_id, batch_id),
            )
            if cursor.fetchone() is not None:
                continue
        for offset in range(0, len(keys), 1000):
            chunk = keys[offset : offset + 1000]
            response = client.delete_objects(
                Bucket=bucket,
                Delete={"Objects": [{"Key": key} for key in chunk], "Quiet": True},
            )
            errors = response.get("Errors", [])
            if errors:
                raise RuntimeError(f"Failed to remove orphaned staging objects: {errors[:3]}")
            removed += len(chunk)
        deleted_batches += 1
        LOGGER.info("STAGING_ORPHAN_CLEANUP_COMPLETE dataset=%s batch=%s objects=%s", dataset_id, batch_id, len(keys))
    if removed:
        LOGGER.info("STAGING_ORPHAN_CLEANUP_SUMMARY batches=%s objects=%s", deleted_batches, removed)
    return removed


def _complete_transfer(spark: SparkSession, transfer: dict, manifests: dict) -> None:
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
        count = _merge_staged_changes(spark, transfer, table, manifests)
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
        if batch_count > max_batches - completed:
            raise RuntimeError(
                f"Started Raw transfer has {batch_count} batches but only "
                f"{max_batches - completed} remain in this run's max_batches limit. "
                "Retry with a max_batches value large enough for the frozen transfer."
            )
        if batch_count > MAX_TRANSFER_BATCHES:
            raise RuntimeError(
                f"Reserved transfer has {batch_count} batches; maximum safe group is "
                f"{MAX_TRANSFER_BATCHES}. Resolve the existing started transfer before retrying."
            )
        manifests = _load_eligible_manifests(transfer)
        _complete_transfer(spark, transfer, manifests)
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
        try:
            cleanup_acknowledged_staging()
        except Exception:
            LOGGER.exception("STAGING_RETENTION_FAILED")
        try:
            cleanup_orphaned_staging()
        except Exception:
            LOGGER.exception("STAGING_ORPHAN_CLEANUP_FAILED")
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
        try:
            cleanup_acknowledged_staging()
        except Exception:
            LOGGER.exception("STAGING_RETENTION_FAILED")
        try:
            cleanup_orphaned_staging()
        except Exception:
            LOGGER.exception("STAGING_ORPHAN_CLEANUP_FAILED")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
