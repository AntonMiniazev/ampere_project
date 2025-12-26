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
    for name in names:
        value = os.getenv(name)
        if value:
            return value
    return None


def _get_db_params() -> tuple[str, str, str, int, str]:
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
    url = os.getenv("DATABASE_URL")
    if url:
        return url

    user, password, host, port, database = _get_db_params()
    return f"postgresql+psycopg://{user}:{password}@{host}:{port}/{database}"


@lru_cache
def get_engine():
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
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))


def exec_sql(query: str) -> None:
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text(query))


def table_exists(schema: str, table: str) -> bool:
    engine = get_engine()
    inspector = inspect(engine)
    return inspector.has_table(table, schema=schema)


def truncate_table(schema: str, table: str) -> None:
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text(f'TRUNCATE TABLE "{schema}"."{table}"'))


def upload_new_data(table, target_table: str, schema: str) -> None:
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
