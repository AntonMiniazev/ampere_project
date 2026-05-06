"""Spark Connect helpers for local notebooks."""

from __future__ import annotations

import os

from .env import load_project_env


def spark_remote(default: str = "sc://sparkconnect.local:443/;use_ssl=true") -> str:
    """Return the Spark Connect remote URI from `.env` or a local ingress default."""
    load_project_env()
    return os.environ.get("AMPERE_SPARK_REMOTE", default)


def configure_grpc_roots() -> None:
    """Apply optional custom gRPC root certificate path from `.env`."""
    load_project_env()
    cert_path = os.environ.get("AMPERE_GRPC_SSL_ROOTS_FILE_PATH")
    if cert_path:
        os.environ.setdefault("GRPC_DEFAULT_SSL_ROOTS_FILE_PATH", cert_path)


def create_spark_session(app_name: str):
    """Create a Spark Connect session for local notebooks."""
    configure_grpc_roots()
    from pyspark.sql import SparkSession

    return SparkSession.builder.remote(spark_remote()).appName(app_name).getOrCreate()


def quote_ident(value: str) -> str:
    """Return a backtick-quoted Spark SQL identifier."""
    return "`" + value.replace("`", "``") + "`"


def table_name(catalog: str, schema: str, table: str) -> str:
    """Build a fully qualified Unity Catalog table name for Spark SQL."""
    return ".".join((quote_ident(catalog), quote_ident(schema), quote_ident(table)))
