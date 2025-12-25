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
    return create_engine(_build_database_url(), pool_pre_ping=True)


def _copy_dataframe(table, target_table: str, schema: str) -> None:
    columns = table.columns
    column_list = ", ".join(f'"{col}"' for col in columns)
    copy_sql = f'COPY "{schema}"."{target_table}" ({column_list}) FROM STDIN'

    user, password, host, port, database = _get_db_params()
    with psycopg.connect(
        user=user, password=password, host=host, port=port, dbname=database
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
        print(f"Truncated table: {schema}.{table}")


def upload_new_data(table, target_table: str, schema: str) -> None:
    if table.height == 0:
        print(f"No data to upload to {target_table}.")
        return

    _copy_dataframe(table, target_table, schema)
    print(f"{table.height} records inserted into {schema}.{target_table}")
