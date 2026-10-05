"""Read-only DuckDB setup and contract helpers for Delta/Iceberg notebooks."""

from __future__ import annotations

import json
import os
from pathlib import Path
from urllib.parse import urlparse

import duckdb

from .env import load_project_env
from .paths import project_root


def _required(name: str, fallback: str | None = None) -> str:
    """Return a required local setting without printing secret values."""
    value = os.getenv(name) or (os.getenv(fallback) if fallback else None)
    if not value:
        raise RuntimeError(f"Set {name} in the root .env or process environment")
    if "<" in value or ">" in value:
        raise ValueError(f"Replace the placeholder for {name} in the root .env")
    return value.strip()


def _literal(value: str) -> str:
    """Quote a DuckDB string literal."""
    return "'" + value.replace("'", "''") + "'"


def _identifier(value: str) -> str:
    """Quote a DuckDB identifier from the canonical contract."""
    return '"' + value.replace('"', '""') + '"'


def load_parity_contract() -> dict:
    """Load the canonical table contract from the repository."""
    path = project_root() / "tools/uc/contracts/ampere_tables.json"
    return json.loads(path.read_text(encoding="utf-8"))


def contract_table(contract: dict, layer: str, name: str) -> dict:
    """Resolve a selected table; reject names absent from the contract."""
    if layer not in {"bronze", "silver", "gold"}:
        raise ValueError(f"Unknown layer: {layer}")
    for table in contract["catalog"]["layers"][layer]["tables"]:
        if table["table_name"] == name:
            return table
    raise ValueError(f"Table {layer}.{name} is absent from the contract")


def table_relations(contract: dict, layer: str, name: str) -> tuple[str, str]:
    """Return Iceberg and Delta SQL relations for one contract table."""
    table = contract_table(contract, layer, name)
    schema = table["schema_name"]
    iceberg = ".".join(_identifier(part) for part in (f"iceberg_{layer}", schema, name))
    delta = f"delta_scan({_literal(table['storage_location'])})"
    return iceberg, delta


def business_columns(contract: dict, layer: str, name: str) -> list[str]:
    """Exclude lineage fields and the Bronze snapshot marker from parity."""
    table = contract_table(contract, layer, name)
    return [
        column["name"]
        for column in table["columns"]
        if not column["name"].startswith("_")
        and not (layer == "bronze" and column["name"] == "snapshot_date")
    ]


def open_parity_connection(memory_limit: str = "4GB") -> duckdb.DuckDBPyConnection:
    """Attach all three Iceberg warehouses and Delta paths with local credentials."""
    # Notebook kernels retain environment values between cell runs. Reload the
    # edited .env so a previous placeholder cannot remain in this process.
    load_project_env(override=True)
    endpoint = _required("AMPERE_MINIO_ENDPOINT", "MINIO_S3_ENDPOINT")
    parsed = urlparse(endpoint)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError("AMPERE_MINIO_ENDPOINT must be an HTTP(S) URL")
    access_key = _required("AMPERE_MINIO_ACCESS_KEY", "MINIO_ACCESS_KEY")
    secret_key = _required("AMPERE_MINIO_SECRET_KEY", "MINIO_SECRET_KEY")
    client_id = _required("LAKEKEEPER_CLIENT_ID")
    client_secret = _required("LAKEKEEPER_CLIENT_SECRET")
    catalog_uri = _required("LAKEKEEPER_CATALOG_URI")
    oauth_uri = _required("LAKEKEEPER_OAUTH_URI")
    oauth_scope = _required("LAKEKEEPER_SCOPE")
    settings = {"memory_limit": memory_limit}
    ca_file = os.getenv("DUCKDB_CA_CERT_FILE", "").strip()
    if ca_file:
        if not Path(ca_file).is_file():
            raise FileNotFoundError(ca_file)
        settings["ca_cert_file"] = ca_file
        settings["enable_server_cert_verification"] = True
    con = duckdb.connect(":memory:", config=settings)
    try:
        for extension in ("httpfs", "iceberg", "delta"):
            con.execute(f"LOAD {extension}")
        con.execute(
            "CREATE SECRET ampere_parity_s3 (TYPE S3, PROVIDER CONFIG, "
            f"KEY_ID {_literal(access_key)}, SECRET {_literal(secret_key)}, "
            f"ENDPOINT {_literal(parsed.netloc)}, "
            f"REGION {_literal(os.getenv('AMPERE_MINIO_REGION', 'us-east-1'))}, "
            "URL_STYLE 'path', "
            f"USE_SSL {'true' if parsed.scheme == 'https' else 'false'})"
        )
        con.execute(
            "CREATE SECRET ampere_parity_lakekeeper (TYPE ICEBERG, "
            f"CLIENT_ID {_literal(client_id)}, CLIENT_SECRET {_literal(client_secret)}, "
            f"OAUTH2_SERVER_URI {_literal(oauth_uri)}, "
            f"OAUTH2_SCOPE {_literal(oauth_scope)}, ENDPOINT {_literal(catalog_uri)})"
        )
        for layer in ("bronze", "silver", "gold"):
            warehouse = os.getenv(f"ICEBERG_{layer.upper()}_WAREHOUSE", layer)
            con.execute(
                f"ATTACH {_literal(warehouse)} AS {_identifier(f'iceberg_{layer}')} "
                "(TYPE iceberg)"
            )
    except Exception:
        con.close()
        raise
    return con
