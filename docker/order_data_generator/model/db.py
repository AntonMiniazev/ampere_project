import os
from functools import lru_cache

import polars as pl
import psycopg
from sqlalchemy import MetaData, Table, create_engine, inspect, text
from sqlalchemy.engine import make_url

from order_data_generator.config import load_config


def _get_env_any(*names: str) -> str | None:
    for name in names:
        value = os.getenv(name)
        if value:
            return value
    return None


def _get_db_params() -> tuple[str, str, str, int, str]:
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
    url = os.getenv("DATABASE_URL")
    if url:
        return url

    user, password, host, port, database = _get_db_params()
    return f"postgresql+psycopg://{user}:{password}@{host}:{port}/{database}"


@lru_cache
def get_engine():
    return create_engine(_build_database_url(), pool_pre_ping=True)


def read_sql(query: str, params: dict | None = None) -> pl.DataFrame:
    engine = get_engine()
    with engine.begin() as conn:
        result = conn.execute(text(query), params or {})
        rows = result.fetchall()
        if not rows:
            return pl.DataFrame(schema=list(result.keys()))
        return pl.DataFrame(rows, schema=list(result.keys()))


def exec_sql(query: str, params: dict | None = None) -> None:
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text(query), params or {})


def table_exists(schema: str, table: str) -> bool:
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
    if table.height == 0:
        print(f"No data to upload to {target_table}.")
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

    user, password, host, port, database = _get_db_params()
    with psycopg.connect(
        user=user, password=password, host=host, port=port, dbname=database
    ) as conn:
        with conn.cursor() as cur:
            with cur.copy(copy_sql) as copy:
                for row in table.iter_rows():
                    copy.write_row(row)
    print(f"{table.height} records inserted into {schema}.{target_table}")


def insert_orders_returning_ids(orders: list[dict], schema: str) -> list[int]:
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
    df = read_sql(f'SELECT id FROM "{schema}"."delivery_type" ORDER BY id')
    return df.get_column("id").to_list() if df.height else []


def fetch_order_status_map(schema: str) -> dict[str, int]:
    df = read_sql(f'SELECT id, order_status FROM "{schema}"."order_statuses"')
    status_map = {}
    for row in df.iter_rows(named=True):
        status_map[str(row["order_status"]).strip().lower()] = int(row["id"])
    return status_map

