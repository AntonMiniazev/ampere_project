"""Prepare one ephemeral DuckDB workspace for Lakekeeper-backed dbt v2."""

from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import urlparse

import duckdb
import yaml


def required(name: str) -> str:
    """Read a required Kubernetes secret or deployment setting."""
    value = os.environ.get(name, "").strip()
    if not value:
        raise ValueError(f"Missing required setting: {name}")
    return value


def sql_string(value: str) -> str:
    """Escape a value for a DuckDB SQL string literal."""
    return "'" + value.replace("'", "''") + "'"


def prepare() -> None:
    """Persist pod-local secrets so every dbt ADBC connection can attach catalogs."""
    secret_dir = Path(os.getenv("DUCKDB_SECRET_DIRECTORY", "/app/secret_store"))
    profile_dir = Path(os.getenv("DBT_PROFILES_DIR", "/app/profiles"))
    # dbt_project.yml places transient views in the ampere_work catalog.
    workspace = Path(os.getenv("DUCKDB_PATH", "/app/artifacts/ampere_work.duckdb"))
    secret_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    secret_dir.chmod(0o700)
    profile_dir.mkdir(parents=True, exist_ok=True)
    workspace.parent.mkdir(parents=True, exist_ok=True)

    endpoint_url = required("MINIO_S3_ENDPOINT")
    if "://" not in endpoint_url:
        endpoint_url = "http://" + endpoint_url
    parsed = urlparse(endpoint_url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError("MINIO_S3_ENDPOINT must be a valid HTTP(S) endpoint")
    minio_host = parsed.netloc
    minio_ssl = parsed.scheme == "https"

    lakekeeper_uri = required("LAKEKEEPER_CATALOG_URI")
    oauth_uri = required("LAKEKEEPER_OAUTH_URI")
    oauth_scope = required("LAKEKEEPER_SCOPE")
    client_id = required("LAKEKEEPER_CLIENT_ID")
    client_secret = required("LAKEKEEPER_CLIENT_SECRET")
    access_key = required("MINIO_ACCESS_KEY")
    secret_key = required("MINIO_SECRET_KEY")
    ca_cert_file = os.getenv("DUCKDB_CA_CERT_FILE", "").strip()
    if ca_cert_file and not Path(ca_cert_file).is_file():
        raise ValueError(f"DUCKDB_CA_CERT_FILE does not exist: {ca_cert_file}")

    con = duckdb.connect(":memory:", config={"secret_directory": str(secret_dir)})
    try:
        con.execute("LOAD iceberg")
        con.execute("LOAD httpfs")
        con.execute(
            "CREATE OR REPLACE PERSISTENT SECRET ampere_iceberg_s3 ("
            "TYPE S3, PROVIDER CONFIG, "
            f"KEY_ID {sql_string(access_key)}, SECRET {sql_string(secret_key)}, "
            f"ENDPOINT {sql_string(minio_host)}, REGION 'us-east-1', "
            f"URL_STYLE 'path', USE_SSL {'true' if minio_ssl else 'false'})"
        )
        con.execute(
            "CREATE OR REPLACE PERSISTENT SECRET ampere_lakekeeper ("
            "TYPE ICEBERG, "
            f"CLIENT_ID {sql_string(client_id)}, "
            f"CLIENT_SECRET {sql_string(client_secret)}, "
            f"OAUTH2_SERVER_URI {sql_string(oauth_uri)}, "
            f"OAUTH2_SCOPE {sql_string(oauth_scope)}, "
            f"ENDPOINT {sql_string(lakekeeper_uri)})"
        )
    finally:
        con.close()

    publish_mode = os.getenv("ICEBERG_PUBLISH_MODE", "direct").strip().lower()
    if publish_mode not in {"direct", "staged"}:
        raise ValueError("ICEBERG_PUBLISH_MODE must be direct or staged")
    attach = []
    for layer in ("bronze", "silver", "gold"):
        if publish_mode == "staged" and layer != "bronze":
            # dbt builds the daily slice locally; the publisher merges it into
            # Lakekeeper only after every model and test has passed.
            attach.append(
                {"path": str(workspace.parent / f"staged_{layer}.duckdb"),
                 "alias": f"iceberg_{layer}"}
            )
        else:
            attach.append(
                {"path": os.getenv(f"ICEBERG_{layer.upper()}_WAREHOUSE", f"ampere-{layer}"),
                 "alias": f"iceberg_{layer}", "type": "iceberg"}
            )
    duckdb_settings = {
        "secret_directory": str(secret_dir),
        "memory_limit": os.getenv("DUCKDB_MEMORY_LIMIT", "6GB"),
    }
    worker_threads = os.getenv("DUCKDB_WORKER_THREADS", "").strip()
    if worker_threads:
        worker_count = int(worker_threads)
        if worker_count < 1:
            raise ValueError("DUCKDB_WORKER_THREADS must be positive")
        duckdb_settings["threads"] = worker_count
    preserve_order = os.getenv("DUCKDB_PRESERVE_INSERTION_ORDER", "").strip().lower()
    if preserve_order:
        if preserve_order not in {"true", "false"}:
            raise ValueError("DUCKDB_PRESERVE_INSERTION_ORDER must be true or false")
        duckdb_settings["preserve_insertion_order"] = preserve_order == "true"
    if ca_cert_file:
        duckdb_settings["ca_cert_file"] = ca_cert_file
        duckdb_settings["enable_server_cert_verification"] = True

    profile = {
        "ampere_iceberg_project": {
            "target": "prod",
            "outputs": {
                "prod": {
                    "type": "duckdb",
                    "path": str(workspace),
                    "schema": "main",
                    "threads": int(os.getenv("DBT_THREADS", "2")),
                    "extensions": ["iceberg", "httpfs"],
                    "settings": duckdb_settings,
                    "attach": attach,
                }
            },
        }
    }
    profile_path = profile_dir / "profiles.yml"
    profile_path.write_text(yaml.safe_dump(profile, sort_keys=False), encoding="utf-8")
    profile_path.chmod(0o600)
    print("Prepared DuckDB Iceberg catalogs: bronze, silver, gold")


if __name__ == "__main__":
    prepare()
