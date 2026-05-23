"""Spark Connect helpers for local notebooks."""

from __future__ import annotations

import os
import socket
import urllib.parse
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

import polars as pl

from .env import load_project_env

DEFAULT_GRPC_ROOTS_FILE_PATH = Path.home() / ".cert" / "ampere-local-ca.crt"


def spark_remote(default: str = "sc://sparkconnect.local:443/;use_ssl=true") -> str:
    """Return the Spark Connect remote URI from `.env` or a local ingress default."""
    load_project_env()
    return os.environ.get("AMPERE_SPARK_REMOTE", default)


def configure_grpc_roots() -> None:
    """Apply optional custom gRPC root certificate path from `.env`."""
    load_project_env()
    cert_path = os.environ.get("AMPERE_GRPC_SSL_ROOTS_FILE_PATH")
    if not cert_path and DEFAULT_GRPC_ROOTS_FILE_PATH.exists():
        cert_path = str(DEFAULT_GRPC_ROOTS_FILE_PATH)
    if cert_path:
        os.environ.setdefault("GRPC_DEFAULT_SSL_ROOTS_FILE_PATH", cert_path)


@dataclass(frozen=True)
class SparkConnectEndpoint:
    """Parsed Spark Connect endpoint used for notebook preflight checks."""

    host: str
    port: int
    use_ssl: bool


def parse_spark_remote(remote: str | None = None) -> SparkConnectEndpoint:
    """Parse a Spark Connect remote URI into host, port, and SSL settings."""
    value = remote or spark_remote()
    parsed = urllib.parse.urlparse(value)
    if parsed.scheme != "sc":
        raise ValueError(f"Spark Connect remote must use sc:// scheme: {value}")
    if not parsed.hostname:
        raise ValueError(f"Spark Connect remote is missing host: {value}")
    options = "&".join(
        value.strip("/;")
        for value in (parsed.path, parsed.params, parsed.query)
        if value.strip("/;")
    )
    query = urllib.parse.parse_qs(options.replace(";", "&"))
    use_ssl = query.get("use_ssl", ["false"])[-1].lower() == "true"
    return SparkConnectEndpoint(
        host=parsed.hostname,
        port=parsed.port or (443 if use_ssl else 15002),
        use_ssl=use_ssl,
    )


def check_spark_connect_tcp(remote: str | None = None, timeout_seconds: float = 5.0) -> SparkConnectEndpoint:
    """Fail fast when the Spark Connect endpoint cannot be reached over TCP."""
    endpoint = parse_spark_remote(remote)
    with socket.create_connection(
        (endpoint.host, endpoint.port), timeout=timeout_seconds
    ):
        return endpoint


def create_spark_session(app_name: str, remote: str | None = None):
    """Create a Spark Connect session for local notebooks."""
    configure_grpc_roots()
    from pyspark.sql import SparkSession

    return SparkSession.builder.remote(remote or spark_remote()).appName(app_name).getOrCreate()


def quote_ident(value: str) -> str:
    """Return a backtick-quoted Spark SQL identifier."""
    return "`" + value.replace("`", "``") + "`"


def table_name(catalog: str, schema: str, table: str) -> str:
    """Build a fully qualified Unity Catalog table name for Spark SQL."""
    return ".".join((quote_ident(catalog), quote_ident(schema), quote_ident(table)))


def _to_polars(df: Any) -> pl.DataFrame:
    """Convert a Spark DataFrame to a Polars DataFrame."""
    return pl.from_pandas(df.toPandas())


def _exception_messages(exc: Exception) -> list[str]:
    """Collect useful messages from Spark Connect exceptions."""
    messages = [f"{type(exc).__name__}: {exc}"]
    current = exc.__cause__ or exc.__context__
    while current is not None:
        messages.append(f"{type(current).__name__}: {current}")
        current = current.__cause__ or current.__context__
    return [message for message in messages if message.strip()]


def _raise_sql_error(statement: str, exc: Exception) -> None:
    """Raise a compact notebook-friendly SQL error."""
    details = "\n".join(_exception_messages(exc))
    raise RuntimeError(f"Spark SQL failed:\n{statement}\n\n{details}") from exc


def apply_namespace(spark: Any, catalog: str, schema: str) -> None:
    """Set catalog/schema with fallbacks for parser/runtime differences."""
    catalog_ident = quote_ident(catalog)
    schema_ident = quote_ident(schema)
    full_ident = f"{catalog_ident}.{schema_ident}"

    errors: list[str] = []
    for command in (
        f"USE CATALOG {catalog_ident}",
        f"USE {schema_ident}",
    ):
        try:
            spark.sql(command)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{command}: {exc}")
            break
    else:
        return

    for command in (
        f"USE {full_ident}",
        f"USE {catalog}.{schema}",
    ):
        try:
            spark.sql(command)
            return
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{command}: {exc}")

    raise RuntimeError(
        "Could not set Spark SQL namespace. Tried: " + " | ".join(errors)
    )


def init_delta_check(
    *,
    catalog: str,
    schema: str,
    app_name: str,
    default_limit: int,
    spark_remote_override: str | None = None,
    connect_timeout_seconds: float = 5.0,
    auto_connect: bool = False,
) -> dict[str, Callable[..., Any]]:
    """Create lazy Spark Connect helpers for delta-check notebooks."""
    spark = None

    def remote_uri() -> str:
        return spark_remote_override or spark_remote()

    def connect(force: bool = False):
        nonlocal spark
        if spark is not None and not force:
            return spark

        remote = remote_uri()
        configure_grpc_roots()
        endpoint = check_spark_connect_tcp(remote, timeout_seconds=connect_timeout_seconds)
        print(
            "Spark Connect TCP preflight ok: "
            f"{endpoint.host}:{endpoint.port}, ssl={endpoint.use_ssl}"
        )
        print("Creating Spark Connect session...")
        spark = create_spark_session(app_name, remote=remote)
        apply_namespace(spark, catalog, schema)
        print(f"Connected to Spark Connect. Current namespace: {catalog}.{schema}")
        return spark

    def sql_spark(statement: str):
        try:
            return connect().sql(statement)
        except Exception as exc:  # noqa: BLE001
            _raise_sql_error(statement, exc)

    def sql(statement: str) -> pl.DataFrame:
        try:
            return _to_polars(sql_spark(statement))
        except RuntimeError:
            raise
        except Exception as exc:  # noqa: BLE001
            _raise_sql_error(statement, exc)

    def fqtn(table: str) -> str:
        if not table:
            raise ValueError("Set TABLE_NAME or pass a table name.")
        return table_name(catalog, schema, table)

    def show_table(table: str, limit: int = default_limit):
        return sql(f"SELECT * FROM {fqtn(table)} LIMIT {int(limit)}")

    def describe(table: str):
        return sql(f"DESCRIBE TABLE {fqtn(table)}")

    def optimize(table: str):
        return sql(f"OPTIMIZE {fqtn(table)}")

    def vacuum(table: str, retain_hours: int = 168):
        return sql(f"VACUUM {fqtn(table)} RETAIN {int(retain_hours)} HOURS")

    def warmup():
        session = connect()
        session.sql("SELECT 1 AS ok").collect()
        tables = session.sql(
            f"SHOW TABLES IN {quote_ident(catalog)}.{quote_ident(schema)}"
        ).collect()
        print(f"Visible tables: {len(tables)}")
        return tables

    def spark_sql(_: str, cell: str):
        return sql(cell)

    try:
        ip = get_ipython()
    except NameError:
        ip = None

    if ip is not None:
        ip.register_magic_function(spark_sql, "cell", "spark_sql")

    if auto_connect:
        connect()

    return {
        "connect": connect,
        "sql": sql,
        "sql_spark": sql_spark,
        "fqtn": fqtn,
        "show_table": show_table,
        "describe": describe,
        "optimize": optimize,
        "vacuum": vacuum,
        "warmup": warmup,
    }
