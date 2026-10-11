"""PostgreSQL dataset, generation-batch, and compact source-change metadata DDL."""

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
    "batch_id": "uuid NOT NULL",
    "change_id": "bigint NOT NULL",
    "source_row_id": "uuid NOT NULL",
    "op": "char(1) NOT NULL",
}


def _quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def drop_source_capture_triggers(connection, source_schema: str, table: str) -> None:
    """Disable all journal capture triggers while reset bootstrap data is loaded.

    ``capture_source_change`` is the legacy row-trigger name. The compact
    journal uses three statement-level triggers, one per write operation.
    Removing both generations prevents reset COPY statements from requiring
    batch metadata before the new dataset baseline exists.
    """
    qualified = f"{_quote(source_schema)}.{_quote(table)}"
    for trigger in (
        "capture_source_change",
        "capture_source_change_insert",
        "capture_source_change_update",
        "capture_source_change_delete",
    ):
        connection.execute(
            text(f"DROP TRIGGER IF EXISTS {_quote(trigger)} ON {qualified}")
        )


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


def _create_compact_journal_table(connection, source_schema: str, table: str) -> list[str]:
    """Create a key-only journal, migrating wide legacy rows when necessary.

    The source tables remain the sole owner of business values. Journal records
    retain only entity identity, operation, and batch ordering so Raw can fetch
    after-images by indexed source_row_id when it transfers a completed batch.
    """
    source_columns = _source_columns(connection, source_schema, table)
    journal = f"source_changes.{_quote(table)}"
    existing = connection.execute(
        text(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema='source_changes' AND table_name=:table"
        ),
        {"table": table},
    ).scalars().all()
    if not existing:
        declarations = [f"{_quote(name)} {sql_type}" for name, sql_type in _METADATA_COLUMNS.items()]
        declarations.append("PRIMARY KEY (batch_id, change_id)")
        connection.execute(text(f"CREATE TABLE {journal} ({', '.join(declarations)})"))
    elif set(existing) != set(_METADATA_COLUMNS):
        # Migrate v4 full-payload rows to four compact fields while preserving
        # operation order and keys for any accepted but untransferred batches.
        compact = f"source_changes.{_quote(f'compact_{table}')}"
        connection.execute(text(f"DROP TABLE IF EXISTS {compact}"))
        declarations = [f"{_quote(name)} {sql_type}" for name, sql_type in _METADATA_COLUMNS.items()]
        declarations.append("PRIMARY KEY (batch_id, change_id)")
        connection.execute(text(f"CREATE TABLE {compact} ({', '.join(declarations)})"))
        connection.execute(
            text(
                f"INSERT INTO {compact} (batch_id,change_id,source_row_id,op) "
                f"SELECT _batch_id::uuid,_change_id,_source_row_id::uuid,_op "
                f"FROM {journal}"
            )
        )
        connection.execute(text(f"DROP TABLE {journal}"))
        connection.execute(
            text(f"ALTER TABLE {compact} RENAME TO {_quote(table)}")
        )
    connection.execute(
        text(
            f"CREATE INDEX IF NOT EXISTS {_quote(f'ix_change_{table}_batch')} "
            f"ON source_changes.{_quote(table)} "
            "(batch_id, change_id)"
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
                v_from_clause text;
                v_table text := TG_TABLE_NAME;
                v_row_id text;
                v_op char(1);
                v_change_count bigint;
                v_expected_count bigint;
            BEGIN
                v_dataset_id := current_setting('ampere.dataset_id', true);
                v_batch_id := current_setting('ampere.batch_id', true);
                v_batch_sequence := NULLIF(current_setting('ampere.batch_sequence', true), '')::bigint;
                v_business_date := NULLIF(current_setting('ampere.business_date', true), '')::date;
                IF v_dataset_id IS NULL OR v_batch_id IS NULL OR v_batch_sequence IS NULL OR v_business_date IS NULL THEN
                    RAISE EXCEPTION 'Source write rejected: active pipeline batch metadata is required';
                END IF;

                IF TG_LEVEL <> 'STATEMENT' THEN
                    RAISE EXCEPTION 'Source journal capture requires statement-level triggers';
                END IF;

                IF TG_OP = 'INSERT' THEN
                    v_op := 'I';
                    v_row_id := 'n.source_row_id::text';
                    v_from_clause := 'FROM new_rows n';
                ELSIF TG_OP = 'UPDATE' THEN
                    v_op := 'U';
                    v_row_id := 'n.source_row_id::text';
                    v_from_clause := 'FROM new_rows n JOIN old_rows o USING (source_row_id)';
                ELSE
                    v_op := 'D';
                    v_row_id := 'o.source_row_id::text';
                    v_from_clause := 'FROM old_rows o';
                END IF;

                EXECUTE format(
                    'INSERT INTO source_changes.%I '
                    '(batch_id,change_id,source_row_id,op) ' ||
                    'SELECT $1::uuid,nextval(''source_changes.change_id_seq''),' ||
                    v_row_id || '::uuid,$2::char(1) ' || v_from_clause,
                    v_table
                ) USING v_batch_id, v_op;
                GET DIAGNOSTICS v_change_count = ROW_COUNT;

                IF TG_OP = 'UPDATE' THEN
                    EXECUTE 'SELECT count(*) FROM old_rows' INTO v_expected_count;
                    IF v_change_count <> v_expected_count THEN
                        RAISE EXCEPTION
                            'source_row_id is immutable for %.% (captured % of % updated rows)',
                            TG_TABLE_SCHEMA, v_table, v_change_count, v_expected_count;
                    END IF;
                END IF;

                UPDATE pipeline.batch_table_counts
                SET change_count = change_count + v_change_count
                WHERE dataset_id = v_dataset_id::uuid
                  AND batch_id = v_batch_id::uuid
                  AND source_table = v_table;
                IF NOT FOUND THEN
                    RAISE EXCEPTION 'Missing batch table-count row for %.%', TG_TABLE_SCHEMA, v_table;
                END IF;

                RETURN NULL;
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
        for operation in ("insert", "update", "delete"):
            connection.execute(
                text(
                    f"DROP TRIGGER IF EXISTS capture_source_change_{operation} ON "
                    f"{_quote(source_schema)}.{_quote(table)}"
                )
            )
        connection.execute(
            text(
                f"CREATE TRIGGER capture_source_change_insert "
                f"AFTER INSERT ON {_quote(source_schema)}.{_quote(table)} "
                "REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT "
                "EXECUTE FUNCTION source_changes.capture_source_change()"
            )
        )
        connection.execute(
            text(
                f"CREATE TRIGGER capture_source_change_update "
                f"AFTER UPDATE ON {_quote(source_schema)}.{_quote(table)} "
                "REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows "
                "FOR EACH STATEMENT EXECUTE FUNCTION source_changes.capture_source_change()"
            )
        )
        connection.execute(
            text(
                f"CREATE TRIGGER capture_source_change_delete "
                f"AFTER DELETE ON {_quote(source_schema)}.{_quote(table)} "
                "REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT "
                "EXECUTE FUNCTION source_changes.capture_source_change()"
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


def _truncate_source_change_metadata(connection) -> None:
    """Clear old compact keys before seeding a new dataset baseline."""
    tables = ", ".join(
        f"{_quote('source_changes')}.{_quote(table)}" for table in SOURCE_TABLES
    )
    connection.execute(text(f"TRUNCATE TABLE {tables}"))


def _delete_inactive_source_change_metadata(connection) -> None:
    """Keep only the active dataset's changes while retaining batch metadata."""
    for table in SOURCE_TABLES:
        connection.execute(
            text(
                f"DELETE FROM {_quote('source_changes')}.{_quote(table)} j "
                "WHERE NOT EXISTS (SELECT 1 FROM pipeline.batches b "
                "JOIN pipeline.datasets d ON d.dataset_id=b.dataset_id "
                "WHERE b.batch_id=j.batch_id AND d.status='ready')"
            )
        )


def initialize_dataset_journal(
    connection,
    source_schema: str,
    project_start_date: date,
    *,
    source_code_version: str = "unknown",
) -> str:
    """Create a new dataset lineage and atomically seed compact baseline keys.

    The caller owns one transaction that also creates/truncates/populates all
    source tables. Retired datasets' change payloads are cleared because the
    reset source state supersedes them; dataset and batch metadata remains for
    lineage, and the new dataset receives a fresh baseline.
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
        _create_compact_journal_table(connection, source_schema, table)
    _install_triggers(connection, source_schema)

    connection.execute(text("UPDATE pipeline.datasets SET status='retired', retired_at=clock_timestamp() WHERE status='ready'"))
    # The reset source contents supersede every previous journal payload. Keep
    # the small lineage metadata, then seed the new dataset's baseline below.
    _truncate_source_change_metadata(connection)
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
        connection.execute(
            text(
                f"INSERT INTO source_changes.{_quote(table)} "
                "(batch_id,change_id,source_row_id,op) "
                f"SELECT CAST(:batch AS uuid),nextval('source_changes.change_id_seq'),"
                f"s.source_row_id,'B' FROM {_quote(source_schema)}.{_quote(table)} s"
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
                "WHERE batch_id=CAST(:batch AS uuid)) "
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
    """Install journal metadata without resetting source rows or active history.

    When no dataset exists, the current source contents become one baseline.
    Re-running the migration for an already-ready dataset only ensures the
    identity columns and capture triggers, and removes payload for inactive
    dataset IDs; it never replaces its dataset ID or active journal rows.
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
        _create_compact_journal_table(connection, source_schema, table)

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
        _delete_inactive_source_change_metadata(connection)
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
