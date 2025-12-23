import os
from functools import lru_cache

from sqlalchemy import MetaData, Table, create_engine, inspect, text

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


def _build_database_url() -> str:
    url = os.getenv("DATABASE_URL")
    if url:
        return url

    user = _get_env_any("PGUSER", "pguser")
    password = _get_env_any("PGPASSWORD", "pgpass")
    host = os.getenv("PGHOST", source_db_host)
    port = os.getenv("PGPORT", str(source_db_port))
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

    return f"postgresql+psycopg://{user}:{password}@{host}:{port}/{database}"


@lru_cache
def get_engine():
    return create_engine(_build_database_url(), pool_pre_ping=True)


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

    records = table.to_dicts()
    metadata = MetaData(schema=schema)
    engine = get_engine()
    db_table = Table(target_table, metadata, autoload_with=engine)

    with engine.begin() as conn:
        conn.execute(db_table.insert(), records)
        print(f"{len(records)} records inserted into {schema}.{target_table}")
