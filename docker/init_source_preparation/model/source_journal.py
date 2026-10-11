"""PostgreSQL dataset, generation-batch, and typed source-change journal DDL."""

from __future__ import annotations

import uuid
from datetime import date, timedelta
import hashlib
import json

from sqlalchemy import text


SOURCE_TABLES = (
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
)

_METADATA_COLUMNS = {
    "_dataset_id": "text NOT NULL",
    "_batch_id": "text NOT NULL",
    "_batch_sequence": "bigint NOT NULL",
    "_change_id": "bigint NOT NULL",
    "_source_row_id": "text NOT NULL",
    "_op": "char(1) NOT NULL",
    "_business_date": "date NOT NULL",
    "_captured_at": "timestamptz NOT NULL",
    "_transfer_id": "text",
    "_ingest_run_id": "text NOT NULL",
    "_ingest_ts": "timestamptz NOT NULL",
    "_source_system": "text NOT NULL",
    "_source_table": "text NOT NULL",
    "_contract_version": "integer NOT NULL",
}


def _quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _source_columns(connection, source_schema: str, table: str) -> list[tuple[str, str]]:
    rows = connection.execute(
        text(
            """
            SELECT a.attname, format_type(a.atttypid, a.atttypmod)
            FROM pg_catalog.pg_attribute a
            JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = :schema AND c.relname = :table
              AND a.attnum > 0 AND NOT a.attisdropped
              AND a.attname <> 'source_row_id'
            ORDER BY a.attnum
            """
        ),
        {"schema": source_schema, "table": table},
    ).all()
    if not rows:
        raise RuntimeError(f"Source table {source_schema}.{table} is missing or empty-schema")
    return [(str(name), str(sql_type)) for name, sql_type in rows]


def _ensure_source_row_identity(connection, source_schema: str, table: str) -> None:
    qualified = f"{_quote(source_schema)}.{_quote(table)}"
    connection.execute(
        text(
            f"ALTER TABLE {qualified} ADD COLUMN IF NOT EXISTS "
            "source_row_id uuid DEFAULT gen_random_uuid()"
        )
    )
    connection.execute(
        text(f"UPDATE {qualified} SET source_row_id = gen_random_uuid() WHERE source_row_id IS NULL")
    )
    connection.execute(text(f"ALTER TABLE {qualified} ALTER COLUMN source_row_id SET NOT NULL"))
    index = _quote(f"uq_{table}_source_row_id")
    connection.execute(text(f"CREATE UNIQUE INDEX IF NOT EXISTS {index} ON {qualified}(source_row_id)"))


def _ensure_control_schema(connection) -> None:
    connection.execute(text("CREATE SCHEMA IF NOT EXISTS pipeline"))
    connection.execute(text("CREATE SCHEMA IF NOT EXISTS source_changes"))
    connection.execute(text("CREATE SEQUENCE IF NOT EXISTS source_changes.change_id_seq"))
    connection.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS pipeline.datasets (
                dataset_id uuid PRIMARY KEY,
                source_schema text NOT NULL,
                project_start_date date NOT NULL,
                status text NOT NULL CHECK (status IN ('initializing', 'ready', 'retired')),
                created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
                retired_at timestamptz
            )
            """
        )
    )
    connection.execute(
        text(
            "CREATE UNIQUE INDEX IF NOT EXISTS uq_pipeline_one_ready_dataset "
            "ON pipeline.datasets ((status)) WHERE status = 'ready'"
        )
    )
    connection.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS pipeline.batches (
                batch_id uuid PRIMARY KEY,
                dataset_id uuid NOT NULL REFERENCES pipeline.datasets(dataset_id),
                batch_sequence bigint GENERATED ALWAYS AS IDENTITY UNIQUE,
                business_date date NOT NULL,
                batch_kind text NOT NULL CHECK (batch_kind IN ('baseline', 'generation', 'manual')),
                status text NOT NULL CHECK (status IN ('started', 'completed')),
                scenario_seed bigint,
                config_fingerprint text NOT NULL,
                source_code_version text NOT NULL,
                started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
                completed_at timestamptz,
                UNIQUE (dataset_id, business_date, batch_kind)
            )
            """
        )
    )
    connection.execute(
        text(
            "CREATE INDEX IF NOT EXISTS ix_pipeline_batches_completed "
            "ON pipeline.batches (dataset_id, status, batch_sequence)"
        )
    )
    connection.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS pipeline.source_tables (
                source_table text PRIMARY KEY,
                enabled boolean NOT NULL DEFAULT true
            )
            """
        )
    )
    connection.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS pipeline.batch_table_counts (
                dataset_id uuid NOT NULL,
                batch_id uuid NOT NULL REFERENCES pipeline.batches(batch_id),
                batch_sequence bigint NOT NULL,
                source_table text NOT NULL,
                change_count bigint NOT NULL DEFAULT 0,
                PRIMARY KEY (dataset_id, batch_id, source_table)
            )
            """
        )
    )
    connection.execute(
        text(
            """
            CREATE OR REPLACE FUNCTION pipeline.require_completed_source_batch()
            RETURNS trigger LANGUAGE plpgsql AS $function$
            BEGIN
                IF EXISTS (
                    SELECT 1 FROM pipeline.batches
                    WHERE dataset_id=NEW.dataset_id AND batch_id=NEW.batch_id
                      AND status <> 'completed'
                ) THEN
                    RAISE EXCEPTION 'Source batch % must be completed before commit', NEW.batch_id;
                END IF;
                RETURN NEW;
            END;
            $function$
            """
        )
    )
    connection.execute(
        text(
            """
            DROP TRIGGER IF EXISTS require_completed_source_batch ON pipeline.batches
            """
        )
    )
    connection.execute(
        text(
            """
            CREATE CONSTRAINT TRIGGER require_completed_source_batch
            AFTER INSERT OR UPDATE ON pipeline.batches
            DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
            EXECUTE FUNCTION pipeline.require_completed_source_batch();
            """
        )
    )
    connection.execute(
        text(
            """
            CREATE OR REPLACE FUNCTION pipeline.begin_manual_source_batch(
                p_business_date date, p_config_fingerprint text, p_source_code_version text
            ) RETURNS uuid LANGUAGE plpgsql AS $function$
            DECLARE
                v_dataset uuid;
                v_batch uuid := gen_random_uuid();
                v_sequence bigint;
            BEGIN
                IF COALESCE(current_setting('ampere.batch_id', true), '') <> '' THEN
                    RAISE EXCEPTION 'A source batch is already active in this transaction';
                END IF;
                PERFORM pg_advisory_xact_lock(724614902631);
                SELECT dataset_id INTO STRICT v_dataset
                FROM pipeline.datasets WHERE status='ready' FOR UPDATE;
                INSERT INTO pipeline.batches
                    (batch_id,dataset_id,business_date,batch_kind,status,
                     config_fingerprint,source_code_version)
                VALUES
                    (v_batch,v_dataset,p_business_date,'manual','started',
                     p_config_fingerprint,p_source_code_version)
                RETURNING batch_sequence INTO v_sequence;
                INSERT INTO pipeline.batch_table_counts
                    (dataset_id,batch_id,batch_sequence,source_table,change_count)
                SELECT v_dataset,v_batch,v_sequence,source_table,0
                FROM pipeline.source_tables WHERE enabled;
                PERFORM set_config('ampere.dataset_id',v_dataset::text,true);
                PERFORM set_config('ampere.batch_id',v_batch::text,true);
                PERFORM set_config('ampere.batch_sequence',v_sequence::text,true);
                PERFORM set_config('ampere.business_date',p_business_date::text,true);
                RETURN v_batch;
            END;
            $function$
            """
        )
    )
    connection.execute(
        text(
            """
            CREATE OR REPLACE FUNCTION pipeline.complete_manual_source_batch()
            RETURNS void LANGUAGE plpgsql AS $function$
            DECLARE
                v_dataset uuid := NULLIF(current_setting('ampere.dataset_id',true),'')::uuid;
                v_batch uuid := NULLIF(current_setting('ampere.batch_id',true),'')::uuid;
            BEGIN
                IF v_dataset IS NULL OR v_batch IS NULL THEN
                    RAISE EXCEPTION 'No manual source batch is active in this transaction';
                END IF;
                UPDATE pipeline.batches SET status='completed',completed_at=clock_timestamp()
                WHERE dataset_id=v_dataset AND batch_id=v_batch
                  AND batch_kind='manual' AND status='started';
                IF NOT FOUND THEN
                    RAISE EXCEPTION 'Could not complete manual source batch %',v_batch;
                END IF;
            END;
            $function$
            """
        )
    )


def _create_typed_journal_table(connection, source_schema: str, table: str) -> list[str]:
    source_columns = _source_columns(connection, source_schema, table)
    before_columns = [(f"_before_{name}", sql_type) for name, sql_type in source_columns]
    declarations = [f"{_quote(name)} {sql_type}" for name, sql_type in source_columns]
    declarations += [f"{_quote(name)} {sql_type}" for name, sql_type in before_columns]
    declarations += [f"{_quote(name)} {sql_type}" for name, sql_type in _METADATA_COLUMNS.items()]
    declarations.append("PRIMARY KEY (_dataset_id, _batch_id, _change_id)")
    connection.execute(
        text(
            f"CREATE TABLE IF NOT EXISTS source_changes.{_quote(table)} "
            f"({', '.join(declarations)})"
        )
    )
    connection.execute(
        text(
            f"CREATE INDEX IF NOT EXISTS {_quote(f'ix_change_{table}_batch')} "
            f"ON source_changes.{_quote(table)} "
            "(_dataset_id, _batch_sequence, _change_id)"
        )
    )
    return [name for name, _ in source_columns]


def _install_capture_function(connection) -> None:
    connection.execute(
        text(
            """
            CREATE OR REPLACE FUNCTION source_changes.capture_source_change()
            RETURNS trigger
            LANGUAGE plpgsql
            AS $function$
            DECLARE
                v_dataset_id text;
                v_batch_id text;
                v_batch_sequence bigint;
                v_business_date date;
                v_old jsonb;
                v_new jsonb;
                v_payload jsonb;
                v_column record;
                v_columns text;
                v_select text;
                v_table text := TG_TABLE_NAME;
                v_row_id text;
                v_op char(1);
            BEGIN
                v_dataset_id := current_setting('ampere.dataset_id', true);
                v_batch_id := current_setting('ampere.batch_id', true);
                v_batch_sequence := NULLIF(current_setting('ampere.batch_sequence', true), '')::bigint;
                v_business_date := NULLIF(current_setting('ampere.business_date', true), '')::date;
                IF v_dataset_id IS NULL OR v_batch_id IS NULL OR v_batch_sequence IS NULL OR v_business_date IS NULL THEN
                    RAISE EXCEPTION 'Source write rejected: active pipeline batch metadata is required';
                END IF;

                IF TG_OP = 'INSERT' THEN
                    v_new := to_jsonb(NEW);
                    v_old := '{}'::jsonb;
                    v_op := 'I';
                    v_row_id := NEW.source_row_id::text;
                ELSIF TG_OP = 'UPDATE' THEN
                    v_new := to_jsonb(NEW);
                    v_old := to_jsonb(OLD);
                    v_op := 'U';
                    v_row_id := NEW.source_row_id::text;
                    IF NEW.source_row_id IS DISTINCT FROM OLD.source_row_id THEN
                        RAISE EXCEPTION 'source_row_id is immutable for %.%', TG_TABLE_SCHEMA, TG_TABLE_NAME;
                    END IF;
                ELSE
                    v_new := '{}'::jsonb;
                    v_old := to_jsonb(OLD);
                    v_op := 'D';
                    v_row_id := OLD.source_row_id::text;
                END IF;

                v_payload := v_new || jsonb_build_object(
                    '_dataset_id', v_dataset_id,
                    '_batch_id', v_batch_id,
                    '_batch_sequence', v_batch_sequence,
                    '_change_id', nextval('source_changes.change_id_seq'),
                    '_source_row_id', v_row_id,
                    '_op', v_op,
                    '_business_date', v_business_date,
                    '_captured_at', clock_timestamp(),
                    '_transfer_id', NULL,
                    '_ingest_run_id', current_setting('application_name', true),
                    '_ingest_ts', clock_timestamp(),
                    '_source_system', 'postgres-pre-raw',
                    '_source_table', v_table,
                    '_contract_version', 4
                );
                FOR v_column IN
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = TG_TABLE_SCHEMA AND table_name = TG_TABLE_NAME
                      AND column_name <> 'source_row_id'
                    ORDER BY ordinal_position
                LOOP
                    v_payload := v_payload || jsonb_build_object(
                        '_before_' || v_column.column_name,
                        v_old -> v_column.column_name
                    );
                END LOOP;

                SELECT string_agg(format('%I', column_name), ', ' ORDER BY ordinal_position),
                       string_agg(format('r.%I', column_name), ', ' ORDER BY ordinal_position)
                INTO v_columns, v_select
                FROM information_schema.columns
                WHERE table_schema = 'source_changes' AND table_name = v_table;
                EXECUTE format(
                    'INSERT INTO source_changes.%I (%s) '
                    'SELECT %s FROM jsonb_populate_record(NULL::source_changes.%I, $1) AS r',
                    v_table, v_columns, v_select, v_table
                ) USING v_payload;

                UPDATE pipeline.batch_table_counts
                SET change_count = change_count + 1
                WHERE dataset_id = v_dataset_id::uuid
                  AND batch_id = v_batch_id::uuid
                  AND source_table = v_table;
                IF NOT FOUND THEN
                    RAISE EXCEPTION 'Missing batch table-count row for %.%', TG_TABLE_SCHEMA, v_table;
                END IF;

                IF TG_OP = 'DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF;
            END;
            $function$
            """
        )
    )


def _install_triggers(connection, source_schema: str) -> None:
    _install_capture_function(connection)
    registered_tables = connection.execute(
        text("SELECT source_table FROM pipeline.source_tables WHERE enabled ORDER BY source_table")
    ).scalars().all()
    for table in registered_tables:
        connection.execute(
            text(
                f"DROP TRIGGER IF EXISTS capture_source_change ON "
                f"{_quote(source_schema)}.{_quote(table)}"
            )
        )
        connection.execute(
            text(
                f"CREATE TRIGGER capture_source_change "
                f"AFTER INSERT OR UPDATE OR DELETE ON {_quote(source_schema)}.{_quote(table)} "
                "FOR EACH ROW EXECUTE FUNCTION source_changes.capture_source_change()"
            )
        )


def _create_batch(connection, dataset_id: uuid.UUID, business_date: date, kind: str,
                  fingerprint: str, code_version: str, seed: int | None = None):
    batch_id = uuid.uuid4()
    sequence = connection.execute(
        text(
            """
            INSERT INTO pipeline.batches
                (batch_id, dataset_id, business_date, batch_kind, status,
                 scenario_seed, config_fingerprint, source_code_version)
            VALUES (:batch, :dataset, :business_date, :kind, 'started',
                    :seed, :fingerprint, :code_version)
            RETURNING batch_sequence
            """
        ),
        {
            "batch": batch_id,
            "dataset": dataset_id,
            "business_date": business_date,
            "kind": kind,
            "seed": seed,
            "fingerprint": fingerprint,
            "code_version": code_version,
        },
    ).scalar_one()
    for table in SOURCE_TABLES:
        connection.execute(
            text(
                "INSERT INTO pipeline.batch_table_counts "
                "(dataset_id, batch_id, batch_sequence, source_table, change_count) "
                "VALUES (:dataset, :batch, :sequence, :table, 0)"
            ),
            {"dataset": dataset_id, "batch": batch_id, "sequence": sequence, "table": table},
        )
    connection.execute(
        text("SELECT set_config('ampere.dataset_id', :value, true)"),
        {"value": str(dataset_id)},
    )
    connection.execute(
        text("SELECT set_config('ampere.batch_id', :value, true)"),
        {"value": str(batch_id)},
    )
    connection.execute(
        text("SELECT set_config('ampere.batch_sequence', :value, true)"),
        {"value": str(sequence)},
    )
    connection.execute(
        text("SELECT set_config('ampere.business_date', :value, true)"),
        {"value": business_date.isoformat()},
    )
    return batch_id, sequence


def _complete_batch(connection, dataset_id: uuid.UUID, batch_id: uuid.UUID) -> None:
    connection.execute(
        text(
            "UPDATE pipeline.batches SET status = 'completed', completed_at = clock_timestamp() "
            "WHERE dataset_id = :dataset AND batch_id = :batch AND status = 'started'"
        ),
        {"dataset": dataset_id, "batch": batch_id},
    )


def initialize_dataset_journal(
    connection,
    source_schema: str,
    project_start_date: date,
    *,
    source_code_version: str = "unknown",
) -> str:
    """Create a new dataset lineage and atomically seed its typed baseline batch.

    The caller owns one transaction that also creates/truncates/populates all
    source tables. Existing journal rows remain attached to retired dataset IDs.
    """
    _ensure_control_schema(connection)
    for table in SOURCE_TABLES:
        connection.execute(
            text(
                "INSERT INTO pipeline.source_tables(source_table) VALUES (:table) "
                "ON CONFLICT (source_table) DO UPDATE SET enabled = true"
            ),
            {"table": table},
        )
    for table in SOURCE_TABLES:
        _ensure_source_row_identity(connection, source_schema, table)
        _create_typed_journal_table(connection, source_schema, table)
    _install_triggers(connection, source_schema)

    connection.execute(text("UPDATE pipeline.datasets SET status='retired', retired_at=clock_timestamp() WHERE status='ready'"))
    dataset_id = uuid.uuid4()
    connection.execute(
        text(
            "INSERT INTO pipeline.datasets(dataset_id, source_schema, project_start_date, status) "
            "VALUES (:dataset, :schema, :start_date, 'initializing')"
        ),
        {"dataset": dataset_id, "schema": source_schema, "start_date": project_start_date},
    )
    batch_id, sequence = _create_batch(
        connection,
        dataset_id,
        project_start_date,
        "baseline",
        fingerprint=hashlib.sha256(
            json.dumps(
                {"source_schema": source_schema, "project_start_date": project_start_date.isoformat()},
                sort_keys=True,
            ).encode("utf-8")
        ).hexdigest(),
        code_version=source_code_version,
    )
    for table in SOURCE_TABLES:
        source_columns = _source_columns(connection, source_schema, table)
        source_names = [name for name, _ in source_columns]
        before_names = [f"_before_{name}" for name in source_names]
        metadata_names = list(_METADATA_COLUMNS)
        target_names = source_names + before_names + metadata_names
        projection = [f"s.{_quote(name)}" for name in source_names]
        projection += [f"NULL::{sql_type}" for _, sql_type in source_columns]
        projection += [
            "CAST(:dataset AS text)",
            "CAST(:batch AS text)",
            ":sequence",
            "nextval('source_changes.change_id_seq')",
            "s.source_row_id::text",
            "'B'",
            ":business_date",
            "clock_timestamp()",
            "NULL::text",
            "current_setting('application_name', true)",
            "clock_timestamp()",
            "'postgres-pre-raw'",
            f"'{table}'",
            "4",
        ]
        connection.execute(
            text(
                f"INSERT INTO source_changes.{_quote(table)} "
                f"({', '.join(_quote(name) for name in target_names)}) "
                f"SELECT {', '.join(projection)} FROM {_quote(source_schema)}.{_quote(table)} s"
            ),
            {
                "dataset": str(dataset_id),
                "batch": str(batch_id),
                "sequence": sequence,
                "business_date": project_start_date,
            },
        )
        connection.execute(
            text(
                "UPDATE pipeline.batch_table_counts SET change_count = "
                f"(SELECT count(*) FROM {_quote('source_changes')}.{_quote(table)} "
                "WHERE _dataset_id=:dataset AND _batch_id=:batch) "
                "WHERE dataset_id=CAST(:dataset AS uuid) AND batch_id=CAST(:batch AS uuid) "
                "AND source_table=:table"
            ),
            {"dataset": str(dataset_id), "batch": str(batch_id), "table": table},
        )

    _complete_batch(connection, dataset_id, batch_id)
    connection.execute(
        text("UPDATE pipeline.datasets SET status='ready' WHERE dataset_id=:dataset"),
        {"dataset": dataset_id},
    )
    connection.execute(text("SELECT set_config('ampere.dataset_id', '', true)"))
    connection.execute(text("SELECT set_config('ampere.batch_id', '', true)"))
    connection.execute(text("SELECT set_config('ampere.batch_sequence', '', true)"))
    connection.execute(text("SELECT set_config('ampere.business_date', '', true)"))
    return str(dataset_id)


def migrate_existing_source_journal(
    connection,
    source_schema: str,
    project_start_date: date,
    *,
    source_code_version: str = "unknown",
) -> str:
    """Install journal metadata without truncating or reseeding source rows.

    When no dataset exists, the current source contents become one baseline.
    Re-running the migration for an already-ready dataset only ensures the
    identity columns and capture triggers; it never replaces its dataset ID.
    """
    _ensure_control_schema(connection)
    for table in SOURCE_TABLES:
        # Validate the live table before changing anything. The caller owns a
        # transaction, so any later DDL failure rolls the migration back.
        _source_columns(connection, source_schema, table)
        connection.execute(
            text(
                "INSERT INTO pipeline.source_tables(source_table) VALUES (:table) "
                "ON CONFLICT (source_table) DO UPDATE SET enabled = true"
            ),
            {"table": table},
        )
        _ensure_source_row_identity(connection, source_schema, table)
        _create_typed_journal_table(connection, source_schema, table)

    ready = connection.execute(
        text(
            "SELECT dataset_id, source_schema FROM pipeline.datasets "
            "WHERE status='ready' FOR UPDATE"
        )
    ).first()
    if ready:
        if ready.source_schema != source_schema:
            raise RuntimeError(
                f"Ready dataset {ready.dataset_id} uses source schema "
                f"{ready.source_schema}, not {source_schema}"
            )
        _install_triggers(connection, source_schema)
        return str(ready.dataset_id)

    # Existing source rows may already cover a historical interval. Start new
    # generation after their latest order date so migration cannot encourage
    # replaying dates whose mutations were never captured.
    latest_order_date = connection.execute(
        text(
            f"SELECT max({_quote('order_date')}) "
            f"FROM {_quote(source_schema)}.{_quote('orders')}"
        )
    ).scalar_one_or_none()
    if latest_order_date is not None:
        project_start_date = max(project_start_date, latest_order_date + timedelta(days=1))

    return initialize_dataset_journal(
        connection,
        source_schema,
        project_start_date,
        source_code_version=source_code_version,
    )
