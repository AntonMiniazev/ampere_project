"""Persist successful Silver and Gold publication boundaries in PostgreSQL."""

from __future__ import annotations

import os
import shlex
import sys
import uuid
from datetime import date
from pathlib import Path

import psycopg


def connect():
    return psycopg.connect(
        host=os.getenv("PGHOST", "postgres-service"),
        port=int(os.getenv("PGPORT", "5432")),
        dbname=os.getenv("PGDATABASE", "ampere_db"),
        user=os.environ["PGUSER"],
        password=os.environ["PGPASSWORD"],
        application_name="ampere-iceberg-incremental-state",
    )


def ensure_tables(cursor) -> None:
    cursor.execute("CREATE SCHEMA IF NOT EXISTS pipeline")
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS pipeline.iceberg_layer_state (
            layer text PRIMARY KEY CHECK (layer IN ('silver','gold')),
            dataset_id uuid NOT NULL,
            last_batch_sequence bigint NOT NULL DEFAULT 0,
            last_silver_publication_sequence bigint NOT NULL DEFAULT 0,
            updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
        )"""
    )
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS pipeline.silver_publications (
            publication_sequence bigserial PRIMARY KEY,
            publication_id uuid NOT NULL UNIQUE,
            dataset_id uuid NOT NULL,
            from_batch_sequence bigint NOT NULL,
            to_batch_sequence bigint NOT NULL,
            affected_months date[] NOT NULL DEFAULT '{}',
            completed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
            CHECK (to_batch_sequence >= from_batch_sequence)
        )"""
    )
    cursor.execute(
        "ALTER TABLE pipeline.silver_publications ADD COLUMN IF NOT EXISTS "
        "status text NOT NULL DEFAULT 'completed'"
    )
    cursor.execute('ALTER TABLE pipeline.silver_publications ALTER COLUMN completed_at DROP NOT NULL')


def _state_input(layer: str, run_mode: str) -> dict[str, str]:
    with connect() as connection, connection.cursor() as cursor:
        if os.getenv('ICEBERG_VALIDATION_ONLY', 'false') == 'true':
            connection.read_only = True
        else:
            ensure_tables(cursor)
        cursor.execute(
            "SELECT dataset_id,last_batch_sequence FROM pipeline.bronze_apply_state "
            "WHERE state_key=true"
        )
        bronze = cursor.fetchone()
        if bronze is None:
            raise RuntimeError("Existing Bronze checkpoint is missing; stop downstream execution and inspect metadata without rewriting upstream data")
        dataset_id, bronze_sequence = bronze

        if layer == "silver":
            cursor.execute(
                "SELECT dataset_id,last_batch_sequence FROM pipeline.iceberg_layer_state "
                "WHERE layer='silver'"
            )
            state = cursor.fetchone()
            if run_mode == "daily_refresh" and state is None:
                raise RuntimeError("Silver has no checkpoint; complete a Silver full rebuild before daily processing")
            if state is not None and state[0] != dataset_id and run_mode == "daily_refresh":
                raise RuntimeError("Source dataset changed; run the explicit Silver full rebuild")
            start = 0 if run_mode == "full_history" or state is None else int(state[1])
            end = int(bronze_sequence)
            if end < start:
                raise RuntimeError(f"Bronze checkpoint {end} is behind Silver checkpoint {start}")
            values = {
                "SILVER_DELTA_FROM_SEQUENCE": str(start),
                "SILVER_DELTA_TO_SEQUENCE": str(end),
                "ICEBERG_PUBLICATION_ID": str(uuid.uuid4()),
            }
        else:
            cursor.execute(
                "SELECT dataset_id,last_batch_sequence FROM pipeline.iceberg_layer_state WHERE layer='silver'"
            )
            silver = cursor.fetchone()
            if silver is None or silver[0] != dataset_id:
                raise RuntimeError("Gold requires a successful Silver baseline for the active dataset")
            cursor.execute(
                "SELECT 1 FROM pipeline.silver_publications WHERE dataset_id=%s AND status='pending' LIMIT 1",
                (dataset_id,),
            )
            if cursor.fetchone() is not None:
                raise RuntimeError('Silver has an incomplete publication; retry Silver before reading it in Gold')
            cursor.execute(
                "SELECT dataset_id,last_silver_publication_sequence "
                "FROM pipeline.iceberg_layer_state WHERE layer='gold'"
            )
            state = cursor.fetchone()
            cursor.execute(
                "SELECT coalesce(max(publication_sequence),0) FROM pipeline.silver_publications "
                "WHERE dataset_id=%s AND status='completed'", (dataset_id,)
            )
            latest_publication = int(cursor.fetchone()[0])
            if run_mode == "daily_refresh" and state is None:
                raise RuntimeError("Gold has no checkpoint; complete a Gold full rebuild before daily processing")
            if state is not None and state[0] != dataset_id and run_mode == "daily_refresh":
                raise RuntimeError("Source dataset changed; run the explicit Gold full rebuild")
            consumed = 0 if state is None else int(state[1])
            if run_mode == 'daily_refresh' and consumed > latest_publication:
                raise RuntimeError('Gold checkpoint is ahead of completed Silver publications')
            if run_mode == "full_history":
                months = "*"
                consumed = 0
            else:
                cursor.execute(
                    "SELECT DISTINCT affected_month FROM pipeline.silver_publications p "
                    "CROSS JOIN LATERAL unnest(p.affected_months) AS affected(affected_month) "
                    "WHERE p.publication_sequence > %s AND p.publication_sequence <= %s "
                    "AND p.dataset_id=%s AND p.status='completed' ORDER BY affected_month",
                    (consumed, latest_publication, dataset_id),
                )
                months = sorted({row[0].replace(day=1) for row in cursor.fetchall()})
            values = {
                "GOLD_AFFECTED_MONTHS": "*" if months == "*" else ",".join(value.isoformat() for value in months),
                "GOLD_SILVER_PUBLICATION_FROM": str(consumed),
                "GOLD_SILVER_PUBLICATION_TO": str(latest_publication),
                "GOLD_SILVER_BATCH_SEQUENCE": str(silver[1]),
                "ICEBERG_PUBLICATION_ID": str(uuid.uuid4()),
            }
    values["ICEBERG_DATASET_ID"] = str(dataset_id)
    return values


def prepare() -> None:
    layer = os.environ["ICEBERG_LAYER"]
    mode = os.getenv("ICEBERG_RUN_MODE", "daily_refresh")
    if layer not in {"silver", "gold"} or mode not in {"daily_refresh", "full_history"}:
        raise ValueError("Unsupported layer or run mode")
    values = _state_input(layer, mode)
    path = Path(os.getenv("ICEBERG_RUNTIME_STATE_FILE", "/tmp/iceberg-runtime-state.sh"))
    path.write_text(
        "".join(f"export {key}={shlex.quote(value)}\n" for key, value in values.items()),
        encoding="utf-8",
    )
    path.chmod(0o600)
    print(
        f"Prepared {layer} checkpoint state: "
        + ", ".join(f"{key}={value}" for key, value in values.items()),
        flush=True,
    )


def stage_silver_publication(months: set[date]) -> set[date]:
    """Retain invalidations across retries that partially committed Iceberg tables."""
    dataset = uuid.UUID(os.environ['ICEBERG_DATASET_ID'])
    start = int(os.environ['SILVER_DELTA_FROM_SEQUENCE'])
    end = int(os.environ['SILVER_DELTA_TO_SEQUENCE'])
    with connect() as connection, connection.cursor() as cursor:
        ensure_tables(cursor)
        cursor.execute(
            "SELECT publication_id,affected_months FROM pipeline.silver_publications "
            "WHERE dataset_id=%s AND (from_batch_sequence=%s OR %s) AND to_batch_sequence<=%s "
            "AND status='pending' ORDER BY publication_sequence FOR UPDATE",
            (dataset, start, os.getenv('ICEBERG_RUN_MODE') == 'full_history', end),
        )
        pending = cursor.fetchall()
        if pending:
            publication_id = pending[0][0]
            months = months | {month for _, previous in pending for month in previous}
            if len(pending) > 1:
                cursor.execute(
                    "UPDATE pipeline.silver_publications SET status='superseded' WHERE publication_id=ANY(%s)",
                    ([row[0] for row in pending[1:]],),
                )
            cursor.execute(
                "UPDATE pipeline.silver_publications SET affected_months=%s,from_batch_sequence=%s,to_batch_sequence=%s WHERE publication_id=%s",
                (sorted(months), start, end, publication_id),
            )
        else:
            publication_id = uuid.UUID(os.environ['ICEBERG_PUBLICATION_ID'])
            cursor.execute(
                "INSERT INTO pipeline.silver_publications "
                "(publication_id,dataset_id,from_batch_sequence,to_batch_sequence,affected_months,status,completed_at) "
                "VALUES (%s,%s,%s,%s,%s,'pending',NULL)",
                (publication_id, dataset, start, end, sorted(months)),
            )
    os.environ['ICEBERG_PUBLICATION_ID'] = str(publication_id)
    return months


def record_silver_publication(months: set[date]) -> None:
    dataset_id = uuid.UUID(os.environ["ICEBERG_DATASET_ID"])
    publication_id = uuid.UUID(os.environ["ICEBERG_PUBLICATION_ID"])
    start = int(os.environ["SILVER_DELTA_FROM_SEQUENCE"])
    end = int(os.environ["SILVER_DELTA_TO_SEQUENCE"])
    sorted_months = sorted({value.replace(day=1) for value in months})
    with connect() as connection, connection.cursor() as cursor:
        ensure_tables(cursor)
        cursor.execute(
            "INSERT INTO pipeline.silver_publications "
            "(publication_id,dataset_id,from_batch_sequence,to_batch_sequence,affected_months) "
            "VALUES (%s,%s,%s,%s,%s) ON CONFLICT (publication_id) DO UPDATE "
            "SET status='completed',completed_at=clock_timestamp() "
            "RETURNING publication_sequence",
            (publication_id, dataset_id, start, end, sorted_months),
        )
        inserted = cursor.fetchone()
        if inserted is None:
            cursor.execute(
                "SELECT publication_sequence FROM pipeline.silver_publications WHERE publication_id=%s",
                (publication_id,),
            )
            sequence = int(cursor.fetchone()[0])
        else:
            sequence = int(inserted[0])
        cursor.execute(
            "INSERT INTO pipeline.iceberg_layer_state "
            "(layer,dataset_id,last_batch_sequence,last_silver_publication_sequence) "
            "VALUES ('silver',%s,%s,%s) ON CONFLICT (layer) DO UPDATE SET "
            "dataset_id=excluded.dataset_id,last_batch_sequence=excluded.last_batch_sequence,"
            "last_silver_publication_sequence=excluded.last_silver_publication_sequence,"
            "updated_at=clock_timestamp()",
            (dataset_id, end, sequence),
        )
    print(f"Committed Silver checkpoint sequence={end} publication={sequence} months={sorted_months}", flush=True)


def record_gold_publication() -> None:
    dataset_id = uuid.UUID(os.environ["ICEBERG_DATASET_ID"])
    publication_sequence = int(os.environ["GOLD_SILVER_PUBLICATION_TO"])
    with connect() as connection, connection.cursor() as cursor:
        ensure_tables(cursor)
        cursor.execute(
            "SELECT dataset_id,last_batch_sequence FROM pipeline.iceberg_layer_state "
            "WHERE layer='silver'"
        )
        silver_state = cursor.fetchone()
        if silver_state is None or silver_state[0] != dataset_id:
            raise RuntimeError("Cannot checkpoint Gold without Silver published for the active source dataset")
        bronze_sequence = int(os.environ['GOLD_SILVER_BATCH_SEQUENCE'])
        cursor.execute(
            "INSERT INTO pipeline.iceberg_layer_state "
            "(layer,dataset_id,last_batch_sequence,last_silver_publication_sequence) "
            "VALUES ('gold',%s,%s,%s) ON CONFLICT (layer) DO UPDATE SET "
            "dataset_id=excluded.dataset_id,last_batch_sequence=excluded.last_batch_sequence,"
            "last_silver_publication_sequence=excluded.last_silver_publication_sequence,"
            "updated_at=clock_timestamp()",
            (dataset_id, bronze_sequence, publication_sequence),
        )
    print(f"Committed Gold checkpoint publication_sequence={publication_sequence}", flush=True)


if __name__ == "__main__":
    action = sys.argv[1] if len(sys.argv) > 1 else "prepare"
    if action == "prepare":
        prepare()
    else:
        raise SystemExit(f"Unsupported action: {action}")
