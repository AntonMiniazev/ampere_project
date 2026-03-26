import logging
import os
from functools import lru_cache

import polars as pl
from sqlalchemy import MetaData, Table, create_engine, inspect, text
from sqlalchemy.engine import make_url

from order_data_generator.config import load_config
from order_data_generator.logging_utils import APP_NAME

logger = logging.getLogger(APP_NAME)


def _get_env_any(*names: str) -> str | None:
    """Return the first non-empty environment variable from several aliases.

    The generator supports both standard `PG*` names and a few project-specific
    secret names for convenience. Resolving them in one helper keeps the rest of
    the database code focused on SQL behavior instead of environment juggling.
    """
    for name in names:
        value = os.getenv(name)
        if value:
            return value
    return None


def _get_db_params() -> tuple[str, str, str, int, str]:
    """Resolve the final Postgres connection parameters for generator writes.

    This function normalizes either `DATABASE_URL` or discrete `PG*` settings
    into one tuple that every DB helper can rely on. That matters because the
    daily generator touches many source tables and should fail fast if the
    database target is not configured correctly.
    """
    config = load_config()
    url = os.getenv("DATABASE_URL")
    if url:
        parsed = make_url(url)
        user = parsed.username or _get_env_any("PGUSER", "pguser")
        password = parsed.password or _get_env_any("PGPASSWORD", "pgpass")
        host = parsed.host or os.getenv("PGHOST", config.source_db_host)
        port = parsed.port or int(os.getenv("PGPORT", str(config.source_db_port)))
        database = parsed.database or os.getenv("PGDATABASE", config.source_db_name)
    else:
        user = _get_env_any("PGUSER", "pguser")
        password = _get_env_any("PGPASSWORD", "pgpass")
        host = os.getenv("PGHOST", config.source_db_host)
        port = int(os.getenv("PGPORT", str(config.source_db_port)))
        database = os.getenv("PGDATABASE", config.source_db_name)

    missing = [
        name
        for name, value in {
            "PGUSER": user,
            "PGPASSWORD": password,
            "PGDATABASE": database,
        }.items()
        if not value
    ]
    if missing:
        missing_list = ", ".join(missing)
        raise ValueError(
            f"Missing required database settings: {missing_list}. "
            "Set DATABASE_URL or the standard PG* environment variables."
        )

    return user, password, host, port, database


def _build_database_url() -> str:
    """Build the SQLAlchemy URL used by the generator database engine.

    SQLAlchemy prefers a single URL, while orchestration layers often provide
    separate environment variables. This helper bridges those two models so the
    rest of the generator DB helpers can consistently call `get_engine()`.
    """
    url = os.getenv("DATABASE_URL")
    if url:
        return url

    user, password, host, port, database = _get_db_params()
    return f"postgresql+psycopg://{user}:{password}@{host}:{port}/{database}"


@lru_cache
def get_engine():
    """Create and cache the SQLAlchemy engine for the source database.

    A single generation run performs many reads and writes, so reusing one
    engine improves performance and keeps connection logging understandable. The
    cache also makes helper calls feel lightweight from the generator modules.
    """
    url = make_url(_build_database_url())
    logger.info(
        "Connecting to Postgres via SQLAlchemy host=%s port=%s db=%s user=%s",
        url.host,
        url.port,
        url.database,
        url.username,
    )
    return create_engine(
        url,
        pool_pre_ping=True,
        connect_args={"application_name": APP_NAME},
    )


def read_sql(query: str, params: dict | None = None) -> pl.DataFrame:
    """Execute a SQL query and return the result as a Polars DataFrame.

    Generator modules read the current source state before deciding what to
    mutate next. Returning Polars keeps the in-memory transformation style
    consistent with the rest of the generator code.
    """
    engine = get_engine()
    with engine.begin() as conn:
        result = conn.execute(text(query), params or {})
        rows = result.fetchall()
        if not rows:
            return pl.DataFrame(schema=list(result.keys()))
        return pl.DataFrame(rows, schema=list(result.keys()))


def exec_sql(query: str, params: dict | None = None) -> None:
    """Execute a SQL statement that mutates source-side state.

    This helper is used for actions such as marking clients as churned or
    clearing temporary rows. Separating it from `read_sql` makes the intent of
    each call site easier to understand during code review.
    """
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text(query), params or {})


def table_exists(schema: str, table: str) -> bool:
    """Check whether a source table exists in the configured schema.

    The daily generator rarely needs this directly, but keeping the helper here
    mirrors the init container and makes the database toolkit complete. It is
    also useful when extending the pipeline with new generator steps.
    """
    engine = get_engine()
    inspector = inspect(engine)
    return inspector.has_table(table, schema=schema)


def upload_new_data(
    table: pl.DataFrame,
    target_table: str,
    schema: str,
    yesterday: str | None = None,
    delivered_status_id: int = 3,
) -> None:
    """Bulk-insert generated rows into a source table, with one special cleanup rule.

    Most target tables are pure append operations, but `delivery_tracking`
    requires deleting stale unpaid placeholder rows before writing fresh events.
    Centralizing that exception here keeps the higher-level generator functions
    readable while still documenting an important business rule.
    """
    if table.height == 0:
        logger.info("No data to upload to %s.", target_table)
        return

    if target_table == "delivery_tracking":
        if yesterday is None:
            raise ValueError("yesterday is required for delivery_tracking updates")
        exec_sql(
            f'''
            DELETE FROM "{schema}"."delivery_tracking"
            WHERE status IS NULL
              AND order_id IN (
                SELECT order_id
                FROM "{schema}"."order_status_history"
                WHERE order_status_id = :delivered_status_id AND status_datetime >= :yesterday
              )
            ''',
            {"yesterday": yesterday, "delivered_status_id": delivered_status_id},
        )

    columns = table.columns
    column_list = ", ".join(f'"{col}"' for col in columns)
    copy_sql = f'COPY "{schema}"."{target_table}" ({column_list}) FROM STDIN'

    logger.info(
        "Bulk insert starting: %s.%s (rows=%s)",
        schema,
        target_table,
        table.height,
    )
    engine = get_engine()
    logger.info("Opening COPY connection for %s.%s", schema, target_table)
    raw_conn = engine.raw_connection()
    try:
        cur = raw_conn.cursor()
        with cur.copy(copy_sql) as copy:
            for row in table.iter_rows():
                copy.write_row(row)
        raw_conn.commit()
    finally:
        raw_conn.close()
    logger.info(
        "Bulk insert completed: %s records into %s.%s",
        table.height,
        schema,
        target_table,
    )


def insert_orders_returning_ids(orders: list[dict], schema: str) -> list[int]:
    """Insert orders and return the database-generated order IDs.

    Orders are created first and then referenced by order lines, status history,
    payments, and delivery tracking. Returning the inserted IDs is what ties the
    rest of the generated operational rows back to the canonical `orders` table.
    """
    if not orders:
        return []

    metadata = MetaData(schema=schema)
    engine = get_engine()
    orders_table = Table("orders", metadata, autoload_with=engine)
    stmt = orders_table.insert().returning(orders_table.c.id)

    with engine.begin() as conn:
        result = conn.execute(stmt, orders)
        ids = [row[0] for row in result.fetchall()]

    return ids


def fetch_delivery_type_ids(schema: str) -> list[int]:
    """Load delivery type IDs from the source dictionary table.

    The generator uses these IDs when assigning a delivery mode to each order.
    Reading them from the database keeps the daily generator aligned with the
    actual dictionary data seeded during bootstrap.
    """
    df = read_sql(f'SELECT id FROM "{schema}"."delivery_type" ORDER BY id')
    return df.get_column("id").to_list() if df.height else []


def fetch_order_status_map(schema: str) -> dict[str, int]:
    """Load a status-name to status-ID mapping from the source dictionary table.

    Status IDs are referenced throughout order-status generation, so the mapping
    is resolved once up front instead of hardcoding integer assumptions in the
    generator. This makes the code easier to read and safer to extend.
    """
    df = read_sql(f'SELECT id, order_status FROM "{schema}"."order_statuses"')
    status_map = {}
    for row in df.iter_rows(named=True):
        status_map[str(row["order_status"]).strip().lower()] = int(row["id"])
    return status_map

