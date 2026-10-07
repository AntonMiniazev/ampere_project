"""Shared DuckDB settings for dbt preparation and Iceberg publication."""

from __future__ import annotations

import os
from pathlib import Path


def runtime_settings(workspace: Path) -> dict[str, object]:
    """Build the same bounded connection settings for every DuckDB process."""
    secret_dir = Path(os.getenv("DUCKDB_SECRET_DIRECTORY", "/app/secret_store"))
    temp_dir = Path(
        os.getenv("DUCKDB_TEMP_DIRECTORY", str(workspace.parent / "duckdb_tmp"))
    )
    secret_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    secret_dir.chmod(0o700)
    temp_dir.mkdir(parents=True, exist_ok=True)

    threads = int(os.getenv("DUCKDB_WORKER_THREADS", "2"))
    if threads < 1:
        raise ValueError("DUCKDB_WORKER_THREADS must be positive")
    preserve_order = os.getenv("DUCKDB_PRESERVE_INSERTION_ORDER", "false").lower()
    if preserve_order not in {"true", "false"}:
        raise ValueError("DUCKDB_PRESERVE_INSERTION_ORDER must be true or false")
    settings: dict[str, object] = {
        "secret_directory": str(secret_dir),
        "memory_limit": os.getenv("DUCKDB_MEMORY_LIMIT", "7GB"),
        "threads": threads,
        "preserve_insertion_order": preserve_order == "true",
        "temp_directory": str(temp_dir),
    }
    max_temp_size = os.getenv("DUCKDB_MAX_TEMP_DIRECTORY_SIZE", "").strip()
    if max_temp_size:
        settings["max_temp_directory_size"] = max_temp_size
    ca_file = os.getenv("DUCKDB_CA_CERT_FILE", "").strip()
    if ca_file:
        if not Path(ca_file).is_file():
            raise ValueError(f"DUCKDB_CA_CERT_FILE does not exist: {ca_file}")
        settings["ca_cert_file"] = ca_file
        settings["enable_server_cert_verification"] = True
    return settings
