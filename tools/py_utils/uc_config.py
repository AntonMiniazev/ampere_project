"""Unity Catalog local configuration helpers."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from .env import bool_env, load_project_env
from .paths import resolve_project_path
from .ssh_connection import SSHConfig
from .uc_client import UCConfig


def catalog_name(config: dict[str, Any]) -> str:
    """Return catalog name from either current string config or legacy object config."""
    catalog = config["catalog"]
    if isinstance(catalog, str):
        return catalog
    return catalog["name"]


def load_json(path: str | Path) -> dict[str, Any]:
    """Load a JSON file as a dictionary."""
    return json.loads(Path(path).read_text(encoding="utf-8"))


def load_uc_bulk_config(path: str | Path | None = None) -> dict[str, Any]:
    """Load UC bulk config and apply root `.env` overrides."""
    load_project_env()
    config_path = (
        resolve_project_path(path)
        if path is not None
        else resolve_project_path("tools/uc/config/uc_runtime_config.json")
    )
    config = load_json(config_path)
    overrides = {
        ("ssh", "jump_host"): "AMPERE_UC_SSH_JUMP_HOST",
        ("ssh", "cluster_host"): "AMPERE_UC_SSH_CLUSTER_HOST",
        ("kubernetes", "namespace"): "AMPERE_UC_K8S_NAMESPACE",
        ("kubernetes", "service_name"): "AMPERE_UC_SERVICE_NAME",
        ("kubernetes", "service_port"): "AMPERE_UC_SERVICE_PORT",
        ("storage", "endpoint"): "AMPERE_MINIO_ENDPOINT",
        ("storage", "access_key"): "AMPERE_MINIO_ACCESS_KEY",
        ("storage", "secret_key"): "AMPERE_MINIO_SECRET_KEY",
        ("storage", "region"): "AMPERE_MINIO_REGION",
    }
    for path_parts, env_name in overrides.items():
        value = os.environ.get(env_name)
        if value is not None:
            config.setdefault(path_parts[0], {})[path_parts[1]] = value

    catalog = os.environ.get("AMPERE_UC_CATALOG")
    if catalog is not None:
        config["catalog"] = catalog
    config.setdefault("storage", {})["ssl_verify"] = bool_env(
        "AMPERE_MINIO_SSL_VERIFY",
        default=bool(config.get("storage", {}).get("ssl_verify", True)),
    )
    return config


def uc_client_config(config: dict[str, Any], verbose: bool = True) -> UCConfig:
    """Build a `UCConfig` object from loaded local UC config."""
    return UCConfig(
        catalog=catalog_name(config),
        namespace=config["kubernetes"]["namespace"],
        service_name=config["kubernetes"]["service_name"],
        service_port=int(config["kubernetes"].get("service_port", 8080)),
        ssh_config=SSHConfig(
            jump_host=config["ssh"]["jump_host"],
            cluster_host=config["ssh"]["cluster_host"],
            verbose=verbose,
        ),
    )
