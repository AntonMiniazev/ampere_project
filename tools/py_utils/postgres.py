"""PostgreSQL helpers for local source-layer checks."""

from __future__ import annotations

import os
from typing import Any

import polars as pl
import psycopg

from .env import load_project_env


def conninfo() -> str:
    """Build PostgreSQL connection info from root `.env` values."""
    load_project_env()
    host = os.environ.get("AMPERE_POSTGRES_HOST", "postgres.local")
    port = os.environ.get("AMPERE_POSTGRES_PORT", "5432")
    dbname = os.environ.get("AMPERE_POSTGRES_DB", "ampere_db")
    user = os.environ.get("AMPERE_POSTGRES_USER", "postgres")
    password = os.environ.get("AMPERE_POSTGRES_PASSWORD", "")
    return (
        f"host={host} port={port} dbname={dbname} "
        f"user={user} password={password}"
    )


def quote_ident(name: str) -> str:
    """Quote a PostgreSQL identifier."""
    return '"' + name.replace('"', '""') + '"'


def table_ident(schema: str, table: str) -> str:
    """Build a safely quoted PostgreSQL table identifier."""
    return f"{quote_ident(schema)}.{quote_ident(table)}"


def row_count(table_name: str, schema: str = "source") -> int:
    """Return row count for a source PostgreSQL table."""
    ident = table_ident(schema, table_name)
    with psycopg.connect(conninfo()) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT count(*)::bigint FROM {ident}")
            return int(cur.fetchone()[0])


def fetch_table(table_name: str, schema: str = "source") -> pl.DataFrame:
    """Fetch a PostgreSQL table into a Polars DataFrame."""
    ident = table_ident(schema, table_name)
    with psycopg.connect(conninfo()) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT * FROM {ident}")
            rows: list[tuple[Any, ...]] = cur.fetchall()
            return pl.DataFrame(rows, schema=[desc[0] for desc in cur.description])


def query_df(sql: str, params: tuple[Any, ...] | dict[str, Any] | None = None) -> pl.DataFrame:
    """Run an arbitrary PostgreSQL query and return rows as a Polars DataFrame."""
    with psycopg.connect(conninfo()) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            if cur.description is None:
                conn.commit()
                return pl.DataFrame()
            rows: list[tuple[Any, ...]] = cur.fetchall()
            return pl.DataFrame(rows, schema=[desc[0] for desc in cur.description])


def execute(sql: str, params: tuple[Any, ...] | dict[str, Any] | None = None) -> int:
    """Run a PostgreSQL statement and return the affected row count."""
    with psycopg.connect(conninfo()) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rowcount = int(cur.rowcount or 0)
        conn.commit()
        return rowcount
