"""Helpers for the canonical Ampere Unity Catalog table contract."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable

from .paths import resolve_project_path

DEFAULT_CONTRACT = "tools/uc/contracts/ampere_tables.json"


def load_contract(path: str | Path = DEFAULT_CONTRACT) -> dict[str, Any]:
    """Load the canonical project metadata contract."""
    return json.loads(resolve_project_path(path).read_text(encoding="utf-8"))


def catalog(contract: dict[str, Any]) -> dict[str, Any]:
    """Return catalog metadata from a canonical contract."""
    catalog_meta = contract["catalog"]
    if not isinstance(catalog_meta, dict):
        raise ValueError("Contract catalog must be an object.")
    return catalog_meta


def catalog_name(contract: dict[str, Any]) -> str:
    """Return the catalog name from a canonical contract."""
    return str(catalog(contract)["name"])


def layers(contract: dict[str, Any]) -> dict[str, Any]:
    """Return layer metadata keyed by layer name."""
    layer_meta = catalog(contract)["layers"]
    if not isinstance(layer_meta, dict):
        raise ValueError("Contract catalog.layers must be an object.")
    return layer_meta


def layer(contract: dict[str, Any], layer_name: str) -> dict[str, Any]:
    """Return one layer definition."""
    all_layers = layers(contract)
    if layer_name not in all_layers:
        raise ValueError(f"Unknown layer {layer_name!r}. Available: {sorted(all_layers)}")
    return all_layers[layer_name]


def catalog_schemas(contract: dict[str, Any]) -> list[dict[str, str]]:
    """Return all catalog schema definitions."""
    return list(catalog(contract).get("schemas", []))


def layer_schema_names(contract: dict[str, Any], layer_name: str) -> list[str]:
    """Return schema names that belong to a selected layer."""
    return list(layer(contract, layer_name).get("schemas", []))


def layer_schemas(contract: dict[str, Any], layer_name: str) -> list[dict[str, str]]:
    """Return schema entries that belong to a selected layer."""
    selected = set(layer_schema_names(contract, layer_name))
    return [item for item in catalog_schemas(contract) if item["name"] in selected]


def iter_layer_tables(contract: dict[str, Any], layer_name: str) -> Iterable[dict[str, Any]]:
    """Yield table definitions for a selected layer."""
    yield from layer(contract, layer_name).get("tables", [])


def iter_tables(contract: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Yield every table definition from every layer."""
    for layer_name in layers(contract):
        yield from iter_layer_tables(contract, layer_name)


def uc_table_payload(table: dict[str, Any]) -> dict[str, Any]:
    """Convert a canonical table entry into a UC registration payload."""
    return {
        key: table[key]
        for key in (
            "schema_name",
            "table_name",
            "table_type",
            "data_source_format",
            "storage_location",
            "comment",
            "columns",
            "properties",
        )
        if key in table
    }


def layer_uc_payload(contract: dict[str, Any], layer_name: str) -> dict[str, Any]:
    """Return UC table registration payload for one layer."""
    tables = [uc_table_payload(table) for table in iter_layer_tables(contract, layer_name)]
    tables.sort(key=lambda item: (item["schema_name"], item["table_name"]))
    return {"tables": tables}
