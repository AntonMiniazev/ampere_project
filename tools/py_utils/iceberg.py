"""DuckDB access to Ampere's Lakekeeper Iceberg warehouses."""

from __future__ import annotations

import os
import re
from collections.abc import Mapping, Sequence
from datetime import date
from urllib.parse import urlparse

import duckdb
import polars as pl

from .env import load_project_env, require_env


LAYERS = ("bronze", "silver", "gold")
BUILD_COLUMNS = {"silver": "_silver_build_ts", "gold": "_gold_build_ts"}
IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def open_connection(
    layers: Sequence[str] = ("gold",), *, memory_limit: str = "4GB", threads: int = 2
) -> duckdb.DuckDBPyConnection:
    """Open an in-memory DuckDB connection with selected Lakekeeper catalogs."""
    if not layers or any(layer not in LAYERS for layer in layers):
        raise ValueError(f"layers must be selected from {LAYERS}")
    load_project_env()
    endpoint = os.getenv("AMPERE_MINIO_ENDPOINT") or os.getenv("MINIO_S3_ENDPOINT")
    if not endpoint:
        raise ValueError("Set AMPERE_MINIO_ENDPOINT in .env")
    parsed = urlparse(endpoint if "://" in endpoint else "https://" + endpoint)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError("AMPERE_MINIO_ENDPOINT must be an HTTP(S) URL")
    access_key = os.getenv("AMPERE_MINIO_ACCESS_KEY") or require_env("MINIO_ACCESS_KEY")
    secret_key = os.getenv("AMPERE_MINIO_SECRET_KEY") or require_env("MINIO_SECRET_KEY")
    con = duckdb.connect(":memory:", config={"memory_limit": memory_limit, "threads": threads})
    try:
        con.execute("LOAD httpfs")
        con.execute("LOAD iceberg")
        ca_file = os.getenv("DUCKDB_CA_CERT_FILE", "").strip()
        if ca_file:
            con.execute("SET ca_cert_file = ?", [ca_file])
        con.execute(
            "CREATE SECRET ampere_s3 (TYPE S3, PROVIDER CONFIG, "
            f"KEY_ID {_literal(access_key)}, SECRET {_literal(secret_key)}, "
            f"ENDPOINT {_literal(parsed.netloc)}, REGION 'us-east-1', "
            f"URL_STYLE 'path', USE_SSL {str(parsed.scheme == 'https').lower()})"
        )
        con.execute(
            "CREATE SECRET ampere_lakekeeper (TYPE ICEBERG, "
            f"CLIENT_ID {_literal(require_env('LAKEKEEPER_CLIENT_ID'))}, "
            f"CLIENT_SECRET {_literal(require_env('LAKEKEEPER_CLIENT_SECRET'))}, "
            f"OAUTH2_SERVER_URI {_literal(require_env('LAKEKEEPER_OAUTH_URI'))}, "
            f"OAUTH2_SCOPE {_literal(require_env('LAKEKEEPER_SCOPE'))}, "
            f"ENDPOINT {_literal(require_env('LAKEKEEPER_CATALOG_URI'))})"
        )
        for layer in dict.fromkeys(layers):
            warehouse = os.getenv(f"ICEBERG_{layer.upper()}_WAREHOUSE", layer)
            con.execute(f"ATTACH {_literal(warehouse)} AS iceberg_{layer} (TYPE ICEBERG)")
        return con
    except Exception:
        con.close()
        raise


def query_df(
    con: duckdb.DuckDBPyConnection, sql: str, params: Sequence[object] = ()
) -> pl.DataFrame:
    """Run DuckDB SQL and return a Polars DataFrame."""
    return pl.from_arrow(con.execute(sql, params).fetch_arrow_table())


def earliest_build_date(
    con: duckdb.DuckDBPyConnection, tables_by_layer: Mapping[str, Sequence[str]]
) -> date:
    """Return the oldest latest-build date among selected published tables."""
    dates: list[date] = []
    for layer, tables in tables_by_layer.items():
        if layer not in BUILD_COLUMNS:
            raise ValueError(f"Build timestamp is unavailable for layer: {layer}")
        for table in tables:
            if not IDENTIFIER.fullmatch(table):
                raise ValueError(f"Invalid table identifier: {table}")
            row = con.execute(
                f"SELECT max({BUILD_COLUMNS[layer]}) FROM iceberg_{layer}.{layer}.{table}"
            ).fetchone()
            if row is None or row[0] is None:
                raise ValueError(f"Table is empty or has no build timestamp: {layer}.{table}")
            dates.append(row[0].date())
    if not dates:
        raise ValueError("Select at least one table")
    return min(dates)
