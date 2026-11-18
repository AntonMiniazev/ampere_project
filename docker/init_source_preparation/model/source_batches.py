"""Initialize compact pipeline metadata and a staged Raw baseline batch."""

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import date

import polars as pl
from sqlalchemy import text

from init_source_preparation.db import quote
from init_source_preparation.ddl_init import table_queries
from tools.source_batch_staging import StagedChangeBatch

SOURCE_TABLES = tuple(name for name, entry in table_queries.items() if entry["type"] != "index")
CONTROL_TABLES = ("datasets", "batches", "source_tables", "batch_table_counts")
CONTRACT_VERSION = 7


def _ensure_source_row_identity(connection, source_schema: str, table: str) -> None:
    qualified = f"{quote(source_schema)}.{quote(table)}"
    connection.execute(text(
        f"ALTER TABLE {qualified} ADD COLUMN IF NOT EXISTS "
        "source_row_id uuid DEFAULT gen_random_uuid()"
    ))
    connection.execute(text(
        f"UPDATE {qualified} SET source_row_id=gen_random_uuid() WHERE source_row_id IS NULL"
    ))
    connection.execute(text(f"ALTER TABLE {qualified} ALTER COLUMN source_row_id SET NOT NULL"))
    connection.execute(text(
        f"CREATE UNIQUE INDEX IF NOT EXISTS {quote(f'uq_{table}_source_row_id')} "
        f"ON {qualified}(source_row_id)"
    ))


def _ensure_control_schema(connection, control: str) -> None:
    connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {control}"))
    connection.execute(text(f"""
        CREATE TABLE {control}.datasets (
            dataset_id uuid PRIMARY KEY,
            initialization_run_id text,
            source_schema text NOT NULL,
            project_start_date date NOT NULL,
            status text NOT NULL CHECK (status IN ('initializing','ready','retired')),
            created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
            retired_at timestamptz
        )
    """))
    connection.execute(text(f"""
        CREATE UNIQUE INDEX uq_pipeline_one_ready_dataset
        ON {control}.datasets ((status)) WHERE status='ready'
    """))
    connection.execute(text(f"""
        CREATE TABLE {control}.batches (
            batch_id uuid PRIMARY KEY,
            dataset_id uuid NOT NULL REFERENCES {control}.datasets(dataset_id),
            batch_sequence bigint GENERATED ALWAYS AS IDENTITY UNIQUE,
            business_date date NOT NULL,
            batch_kind text NOT NULL CHECK (batch_kind IN ('baseline','generation','manual')),
            status text NOT NULL CHECK (status IN ('started','completed')),
            scenario_seed bigint,
            config_fingerprint text NOT NULL,
            source_code_version text NOT NULL,
            started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
            completed_at timestamptz,
            UNIQUE (dataset_id,business_date,batch_kind)
        )
    """))
    connection.execute(text(
        "CREATE INDEX ix_pipeline_batches_completed "
        f"ON {control}.batches(dataset_id,status,batch_sequence)"
    ))
    connection.execute(text(f"""
        CREATE TABLE {control}.source_tables (
            source_table text PRIMARY KEY,
            enabled boolean NOT NULL DEFAULT true
        )
    """))
    connection.execute(text(f"""
        CREATE TABLE {control}.batch_table_counts (
            dataset_id uuid NOT NULL,
            batch_id uuid NOT NULL REFERENCES {control}.batches(batch_id),
            batch_sequence bigint NOT NULL,
            source_table text NOT NULL,
            change_count bigint NOT NULL DEFAULT 0,
            PRIMARY KEY(dataset_id,batch_id,source_table)
        )
    """))


def _new_batch(connection, control: str, dataset_id: uuid.UUID, business_date: date, kind: str,
               fingerprint: str, code_version: str) -> tuple[uuid.UUID, int]:
    batch_id = uuid.uuid4()
    sequence = connection.execute(text(f"""
        INSERT INTO {control}.batches
            (batch_id,dataset_id,business_date,batch_kind,status,
             config_fingerprint,source_code_version)
        VALUES (:batch,:dataset,:business_date,:kind,'started',:fingerprint,:code_version)
        RETURNING batch_sequence
    """), {
        "batch": batch_id,
        "dataset": dataset_id,
        "business_date": business_date,
        "kind": kind,
        "fingerprint": fingerprint,
        "code_version": code_version,
    }).scalar_one()
    connection.execute(text(f"""
        INSERT INTO {control}.batch_table_counts
            (dataset_id,batch_id,batch_sequence,source_table,change_count)
        SELECT :dataset,:batch,:sequence,source_table,0
        FROM {control}.source_tables WHERE enabled
    """), {"dataset": dataset_id, "batch": batch_id, "sequence": sequence})
    return batch_id, int(sequence)


def initialize_source_dataset(
    connection,
    source_schema: str,
    project_start_date: date,
    *,
    source_code_version: str = "unknown",
    initialization_run_id: str | None = None,
    control_schema: str = "pipeline",
    client=None,
    bucket: str | None = None,
    prefix: str | None = None,
    chunk_rows: int | None = None,
    target_file_bytes: int | None = None,
):
    """Create a new dataset and stage its exact current rows as baseline events.

    Caller owns the transaction. The returned writer must publish its commit
    marker only after that transaction succeeds.
    """
    control = quote(control_schema)
    _ensure_control_schema(connection, control)
    for table in SOURCE_TABLES:
        connection.execute(text(
            f"INSERT INTO {control}.source_tables(source_table,enabled) VALUES (:table,true)"
        ), {"table": table})
        _ensure_source_row_identity(connection, source_schema, table)

    dataset_id = uuid.uuid4()
    connection.execute(text(
        f"INSERT INTO {control}.datasets "
        "(dataset_id,initialization_run_id,source_schema,project_start_date,status) "
        "VALUES (:dataset,:run_id,:schema,:start_date,'initializing')"
    ), {
        "dataset": dataset_id,
        "run_id": initialization_run_id,
        "schema": source_schema,
        "start_date": project_start_date,
    })
    fingerprint = hashlib.sha256(json.dumps({
        "source_schema": source_schema,
        "project_start_date": project_start_date.isoformat(),
    }, sort_keys=True).encode("utf-8")).hexdigest()
    batch_id, sequence = _new_batch(
        connection, control, dataset_id, project_start_date, "baseline", fingerprint, source_code_version
    )
    stager = StagedChangeBatch(
        dataset_id=str(dataset_id),
        batch_id=str(batch_id),
        batch_sequence=sequence,
        business_date=project_start_date,
        contract_version=CONTRACT_VERSION,
        source_tables=SOURCE_TABLES,
        client=client,
        bucket=bucket,
        prefix=prefix,
        chunk_rows=chunk_rows,
        target_file_bytes=target_file_bytes,
    )
    for table in SOURCE_TABLES:
        # Apply streaming to this SELECT only. Setting it on the Connection
        # persists for subsequent statements and makes psycopg try to declare
        # a server cursor for the UPDATE below (which PostgreSQL disallows).
        statement = text(
            f"SELECT * FROM {quote(source_schema)}.{quote(table)} ORDER BY source_row_id"
        ).execution_options(stream_results=True)
        result = connection.execute(statement).mappings()
        row_count = 0
        while rows := result.fetchmany(stager.chunk_rows):
            row_count += stager.append(table, pl.DataFrame([dict(row) for row in rows]), "B")
        connection.execute(text(
            f"UPDATE {control}.batch_table_counts SET change_count=:count "
            "WHERE dataset_id=:dataset AND batch_id=:batch AND source_table=:table"
        ), {"count": row_count, "dataset": dataset_id, "batch": batch_id, "table": table})

    stager.prepare()
    connection.execute(text(
        f"UPDATE {control}.batches SET status='completed',completed_at=clock_timestamp() "
        "WHERE dataset_id=:dataset AND batch_id=:batch"
    ), {"dataset": dataset_id, "batch": batch_id})
    connection.execute(text(
        f"UPDATE {control}.datasets SET status='ready' WHERE dataset_id=:dataset"
    ), {"dataset": dataset_id})
    return str(dataset_id), stager


def drop_source_capture_triggers(connection, source_schema: str, table: str) -> None:
    """Remove legacy capture triggers from a table during clean-room reset."""
    qualified = f"{quote(source_schema)}.{quote(table)}"
    for trigger in (
        "capture_source_change", "capture_source_change_insert",
        "capture_source_change_update", "capture_source_change_delete",
    ):
        connection.execute(text(f"DROP TRIGGER IF EXISTS {quote(trigger)} ON {qualified}"))
