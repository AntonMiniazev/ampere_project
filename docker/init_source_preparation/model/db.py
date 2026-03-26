import logging
import os
from functools import lru_cache

import psycopg
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import make_url

from init_source_preparation.config import (
    source_db_host,
    source_db_name,
    source_db_port,
)
from init_source_preparation.logging_utils import APP_NAME

logger = logging.getLogger(APP_NAME)


def _get_env_any(*names: str) -> str | None:
    """Return the first non-empty environment variable from a list of aliases.

    The bootstrap image supports both standard libpq variables such as `PGUSER`
    and project-specific legacy names such as `pguser`. This helper keeps the
    compatibility mapping in one place so the rest of the module can talk in
    terms of resolved credentials instead of environment-variable trivia.
    """
    for name in names:
        value = os.getenv(name)
        if value:
            return value
    return None


def _get_db_params() -> tuple[str, str, str, int, str]:
    """Resolve the final Postgres connection parameters for bootstrap work.

    The function accepts either a single `DATABASE_URL` or the usual `PG*`
    variables and then validates that the minimum required fields exist. This
    makes it clear for a junior reader that all later database work in this
    module depends on one normalized source of truth.
    """
    url = os.getenv("DATABASE_URL")
    if url:
        parsed = make_url(url)
        user = parsed.username or _get_env_any("PGUSER", "pguser")
        password = parsed.password or _get_env_any("PGPASSWORD", "pgpass")
        host = parsed.host or os.getenv("PGHOST", source_db_host)
        port = parsed.port or int(os.getenv("PGPORT", str(source_db_port)))
        database = parsed.database or os.getenv("PGDATABASE", source_db_name)
    else:
        user = _get_env_any("PGUSER", "pguser")
        password = _get_env_any("PGPASSWORD", "pgpass")
        host = os.getenv("PGHOST", source_db_host)
        port = int(os.getenv("PGPORT", str(source_db_port)))
        database = os.getenv("PGDATABASE", source_db_name)

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
    """Build a SQLAlchemy connection URL from the resolved Postgres settings.

    SQLAlchemy expects a full database URL even when the rest of the project
    prefers separate `PG*` variables. This helper bridges those two worlds and
    keeps the URL-building details away from the higher-level table operations.
    """
    url = os.getenv("DATABASE_URL")
    if url:
        return url

    user, password, host, port, database = _get_db_params()
    return f"postgresql+psycopg://{user}:{password}@{host}:{port}/{database}"


@lru_cache
def get_engine():
    """Create and cache the SQLAlchemy engine used by bootstrap operations.

    The engine is cached because a single bootstrap run issues many table
    creation, truncate, and insert commands against the same database. Reusing
    one engine avoids reconnect noise and makes logs easier to follow.
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


def _copy_dataframe(table, target_table: str, schema: str) -> None:
    """Bulk-load a Polars DataFrame into Postgres using COPY.

    COPY is much faster than row-by-row INSERTs, which matters even during
    bootstrap because the initial client and courier populations are large. This
    function is the final "write rows into Postgres" step for all dictionary and
    generated bootstrap datasets.
    """
    columns = table.columns
    column_list = ", ".join(f'"{col}"' for col in columns)
    copy_sql = f'COPY "{schema}"."{target_table}" ({column_list}) FROM STDIN'

    user, password, host, port, database = _get_db_params()
    logger.info(
        "Opening COPY connection host=%s port=%s db=%s user=%s",
        host,
        port,
        database,
        user,
    )
    with psycopg.connect(
        user=user,
        password=password,
        host=host,
        port=port,
        dbname=database,
        application_name=APP_NAME,
    ) as conn:
        with conn.cursor() as cur:
            with cur.copy(copy_sql) as copy:
                for row in table.iter_rows():
                    copy.write_row(row)


def ensure_schema(schema: str) -> None:
    """Create the target Postgres schema when it does not exist yet.

    The bootstrap pipeline always starts by ensuring the source schema exists so
    every later CREATE TABLE or COPY target has a valid namespace. This is the
    first database-side action in the one-time initialization flow.
    """
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))


def exec_sql(query: str) -> None:
    """Execute a single SQL statement or SQL block inside a transaction.

    This is used for DDL tasks such as CREATE TABLE, TRUNCATE, and CREATE INDEX.
    Keeping it as a helper makes the orchestration code read more like a story
    of pipeline steps instead of a repeated database-connection ceremony.
    """
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text(query))


def table_exists(schema: str, table: str) -> bool:
    """Check whether a table already exists in the target schema.

    The bootstrap workflow uses this to decide between create-vs-truncate logic.
    That distinction is important because re-running the init DAG should reset
    data tables without failing on already-created structures.
    """
    engine = get_engine()
    inspector = inspect(engine)
    return inspector.has_table(table, schema=schema)


def truncate_table(schema: str, table: str) -> None:
    """Remove all rows from an existing bootstrap table.

    Truncation keeps the table structure intact while clearing old synthetic
    data. In practice this makes the init DAG idempotent enough for safe manual
    reruns during development.
    """
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text(f'TRUNCATE TABLE "{schema}"."{table}"'))


def upload_new_data(table, target_table: str, schema: str) -> None:
    """Write a DataFrame into a bootstrap target table when rows are present.

    This is the common write helper used by dictionary loaders and generator
    functions. It keeps logging, empty-data behavior, and COPY semantics in one
    place so the orchestration layer can focus on *what* data is being loaded.
    """
    if table.height == 0:
        logger.info("No data to upload to %s.", target_table)
        return

    logger.info(
        "Bulk insert starting: %s.%s (rows=%s)",
        schema,
        target_table,
        table.height,
    )
    _copy_dataframe(table, target_table, schema)
    logger.info(
        "Bulk insert completed: %s records into %s.%s",
        table.height,
        schema,
        target_table,
    )
