"""Load and resolve Ampere's canonical Iceberg contract v3."""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any


CONTRACT_VERSION = 3
DEFAULT_CONTRACT_PATH = Path(__file__).with_name("ampere_tables.json")
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_TYPE = re.compile(
    r"^(?:boolean|date|timestamp|timestamp_ntz|smallint|int|bigint|float|double|string|decimal\(\d{1,2},\d{1,2}\))$",
    re.IGNORECASE,
)
_TRANSFORMS = {"identity", "years", "months", "days", "hours"}
_DISTRIBUTIONS = {"none", "hash", "range"}
_WRITE_MODES = {"append", "merge", "overwrite_partitions", "overwrite"}
_COMPACTION_POLICIES = {"conditional_binpack", "none"}
_DELETE_POLICIES = {"rewrite_on_threshold", "none"}
_MANIFEST_POLICIES = {"on_threshold", "always", "none"}
_DUCKDB_PARTITION_TRANSFORMS = {"identity", "bucket", "truncate"}
_MIN_RETENTION_DAYS = 14
_SOURCE_COMPLETENESS = {"partial", "full_snapshot"}


class ContractError(ValueError):
    """Raised when contract v3 is malformed or unsupported."""


@dataclass(frozen=True)
class ResolvedTable:
    layer: str
    namespace: str
    name: str
    columns: tuple[dict[str, Any], ...]
    profile_name: str
    format_version: int
    partition_spec: tuple[dict[str, str], ...]
    sort_order: tuple[dict[str, str], ...]
    write: dict[str, Any]
    maintenance: dict[str, Any]
    publication: dict[str, Any]

    @property
    def column_names(self) -> tuple[str, ...]:
        return tuple(column["name"] for column in self.columns)


@dataclass(frozen=True)
class AmpereContract:
    version: int
    catalog_namespace: str
    profiles: dict[str, dict[str, Any]]
    tables: dict[tuple[str, str], ResolvedTable]

    def table(self, layer: str, name: str) -> ResolvedTable:
        try:
            return self.tables[(layer, name)]
        except KeyError as exc:
            raise ContractError(f"Unknown Iceberg table {layer}.{name}") from exc

    def layer_tables(self, layer: str) -> tuple[ResolvedTable, ...]:
        return tuple(
            table for (table_layer, _), table in self.tables.items()
            if table_layer == layer
        )


def _fail(path: str, message: str) -> None:
    raise ContractError(f"{path}: {message}")


def _identifier(value: Any, path: str) -> str:
    if not isinstance(value, str) or not _IDENTIFIER.fullmatch(value):
        _fail(path, f"invalid SQL identifier {value!r}")
    return value


def _object(value: Any, path: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        _fail(path, "must be an object")
    return value


def _merge(default: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """Merge policy maps one level deep; table layout lists replace defaults."""
    result = dict(default)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = {**result[key], **value}
        else:
            result[key] = value
    return result


def _validate_maintenance(name: str, raw: Any) -> dict[str, Any]:
    path = f"{name}.maintenance"
    maintenance = _object(raw, path)
    policies = {
        "data_compaction": _COMPACTION_POLICIES,
        "delete_files": _DELETE_POLICIES,
        "rewrite_manifests": _MANIFEST_POLICIES,
    }
    for key, allowed in policies.items():
        value = maintenance.get(key)
        if value is not None and value not in allowed:
            _fail(f"{path}.{key}", f"unsupported value {value!r}")
    for key in ("min_input_files", "delete_file_count_threshold", "manifest_count_threshold"):
        value = maintenance.get(key)
        if value is not None and (
            isinstance(value, bool) or not isinstance(value, int) or value < 1
        ):
            _fail(f"{path}.{key}", "must be a positive integer")
    if maintenance.get("data_compaction", "none") != "none":
        minimum = maintenance.get("min_input_files", 5)
        if minimum < 2:
            _fail(f"{path}.min_input_files", "must be at least 2 for a useful file rewrite")
    if maintenance.get("delete_files") == "rewrite_on_threshold":
        if "delete_file_count_threshold" not in maintenance:
            _fail(f"{path}.delete_file_count_threshold", "required when delete_files is rewrite_on_threshold")
    if maintenance.get("rewrite_manifests") == "on_threshold":
        if "manifest_count_threshold" not in maintenance:
            _fail(f"{path}.manifest_count_threshold", "required when rewrite_manifests is on_threshold")
    for key in ("snapshot_retention_days", "orphan_retention_days"):
        value = maintenance.get(key, _MIN_RETENTION_DAYS)
        if (
            isinstance(value, bool)
            or not isinstance(value, int)
            or value < _MIN_RETENTION_DAYS
        ):
            _fail(f"{path}.{key}", f"must be at least {_MIN_RETENTION_DAYS} days")
    return maintenance


def _validate_publication(layer: str, table_path: str, raw: Any, columns: set[str]) -> dict[str, Any]:
    if layer not in {"silver", "gold"}:
        if raw is not None:
            _fail(f"{table_path}.publication", "is only supported for Silver and Gold tables")
        return {}
    publication = _object(raw, f"{table_path}.publication")
    raw_keys = publication.get("merge_keys")
    if not isinstance(raw_keys, list) or not raw_keys:
        _fail(f"{table_path}.publication.merge_keys", "must be a non-empty ordered list")
    keys = [_identifier(key, f"{table_path}.publication.merge_keys") for key in raw_keys]
    if len(set(keys)) != len(keys):
        _fail(f"{table_path}.publication.merge_keys", "must not contain duplicates")
    missing = set(keys) - columns
    if missing:
        _fail(f"{table_path}.publication.merge_keys", f"unknown contracted columns {sorted(missing)}")

    completeness = _object(
        publication.get("source_completeness"),
        f"{table_path}.publication.source_completeness",
    )
    if set(completeness) != {"daily_refresh", "full_history"}:
        _fail(
            f"{table_path}.publication.source_completeness",
            "must define daily_refresh and full_history",
        )
    for run_mode, value in completeness.items():
        if not isinstance(value, str) or value not in _SOURCE_COMPLETENESS:
            _fail(
                f"{table_path}.publication.source_completeness.{run_mode}",
                f"unsupported value {value!r}",
            )
    if completeness["full_history"] != "full_snapshot":
        _fail(
            f"{table_path}.publication.source_completeness.full_history",
            "full-history publication must be a full_snapshot",
        )
    if layer == "gold" and completeness["daily_refresh"] != "full_snapshot":
        _fail(
            f"{table_path}.publication.source_completeness.daily_refresh",
            "Gold daily publication must be a full_snapshot",
        )
    return {"merge_keys": tuple(keys), "source_completeness": dict(completeness)}


def _validate_profile(name: str, raw: Any) -> dict[str, Any]:
    path = f"physical_profiles.{name}"
    profile = _object(raw, path)
    version = profile.get("format_version")
    if version not in {2}:
        _fail(f"{path}.format_version", "only Iceberg format version 2 is supported")
    write = _object(profile.get("write", {}), f"{path}.write")
    distribution = write.get("distribution", "none")
    if distribution not in _DISTRIBUTIONS:
        _fail(f"{path}.write.distribution", f"unsupported value {distribution!r}")
    if "mode" in write and write["mode"] not in _WRITE_MODES:
        _fail(f"{path}.write.mode", f"unsupported value {write['mode']!r}")
    target_size = write.get("target_file_size_bytes")
    if target_size is not None and (not isinstance(target_size, int) or target_size <= 0):
        _fail(f"{path}.write.target_file_size_bytes", "must be a positive integer")

    maintenance = _validate_maintenance(path, profile.get("maintenance", {}))
    return profile


def load_contract(path: str | Path = DEFAULT_CONTRACT_PATH) -> AmpereContract:
    """Read, validate, and resolve contract v3 defaults for all table specs."""
    contract_path = Path(path)
    try:
        raw = json.loads(contract_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ContractError(f"Cannot read contract {contract_path}: {exc}") from exc
    root = _object(raw, "contract")
    if root.get("contract_version") != CONTRACT_VERSION:
        _fail("contract_version", f"expected {CONTRACT_VERSION}")
    catalog = _object(root.get("catalog"), "catalog")
    ampere = _object(catalog.get("ampere"), "catalog.ampere")
    layers = _object(ampere.get("layers"), "catalog.ampere.layers")
    if set(layers) != {"bronze", "silver", "gold"}:
        _fail("catalog.ampere.layers", "must declare bronze, silver, and gold")
    profile_raw = _object(ampere.get("physical_profiles"), "catalog.ampere.physical_profiles")
    profiles = {
        _identifier(name, f"physical_profiles.{name}"): _validate_profile(name, value)
        for name, value in profile_raw.items()
    }

    resolved: dict[tuple[str, str], ResolvedTable] = {}
    for layer in ("bronze", "silver", "gold"):
        layer_tables = _object(layers[layer], f"layers.{layer}")
        for raw_name, raw_table in layer_tables.items():
            name = _identifier(raw_name, f"layers.{layer}.{raw_name}")
            table_path = f"layers.{layer}.{name}"
            table = _object(raw_table, table_path)
            raw_columns = table.get("columns")
            if not isinstance(raw_columns, list) or not raw_columns:
                _fail(f"{table_path}.columns", "must be a non-empty ordered list")
            columns: list[dict[str, Any]] = []
            names: set[str] = set()
            for position, raw_column in enumerate(raw_columns, start=1):
                column_path = f"{table_path}.columns[{position - 1}]"
                column = _object(raw_column, column_path)
                column_name = _identifier(column.get("name"), f"{column_path}.name")
                data_type = column.get("type_text")
                if not isinstance(data_type, str) or not _TYPE.fullmatch(data_type.strip()):
                    _fail(f"{column_path}.type_text", f"unsupported type {data_type!r}")
                if column.get("position") != position:
                    _fail(f"{column_path}.position", f"expected {position}")
                if column_name in names:
                    _fail(f"{column_path}.name", f"duplicate column {column_name!r}")
                names.add(column_name)
                columns.append({"name": column_name, "type_text": data_type.strip().lower(), "position": position})

            table_profile = _object(table.get("profile"), f"{table_path}.profile")
            layout_override = _object(table_profile.get("physical_layout", {}), f"{table_path}.profile.physical_layout")
            profile_name = layout_override.get("profile")
            if profile_name not in profiles:
                _fail(f"{table_path}.profile.physical_layout.profile", f"unknown physical profile {profile_name!r}")
            profile = profiles[profile_name]
            profile_layout = _object(profile.get("physical_layout", {}), f"physical_profiles.{profile_name}.physical_layout")
            layout = _merge(profile_layout, layout_override)
            partition_spec = layout.get("partition_spec", [])
            sort_order = layout.get("sort_order", [])
            if not isinstance(partition_spec, list) or not isinstance(sort_order, list):
                _fail(f"{table_path}.profile.physical_layout", "partition_spec and sort_order must be lists")
            for index, item in enumerate(partition_spec):
                item_path = f"{table_path}.partition_spec[{index}]"
                item = _object(item, item_path)
                source = _identifier(item.get("source"), f"{item_path}.source")
                transform = item.get("transform")
                if source not in names:
                    _fail(item_path, f"partition source {source!r} is not a column")
                if transform not in _TRANSFORMS:
                    _fail(f"{item_path}.transform", f"unsupported transform {transform!r}")
            for index, item in enumerate(sort_order):
                item_path = f"{table_path}.sort_order[{index}]"
                item = _object(item, item_path)
                source = _identifier(item.get("source"), f"{item_path}.source")
                if source not in names:
                    _fail(item_path, f"sort source {source!r} is not a column")
                if item.get("direction", "asc") not in {"asc", "desc"}:
                    _fail(f"{item_path}.direction", "must be asc or desc")
            write = _merge(profile.get("write", {}), table_profile.get("write", {}))
            publication = _validate_publication(layer, table_path, table.get("publication"), names)
            if layer in {"silver", "gold"}:
                if write.get("distribution", "none") != "none":
                    _fail(
                        f"{table_path}.write.distribution",
                        "DuckDB Iceberg publishing does not enforce Spark distribution modes",
                    )
                if sort_order:
                    _fail(
                        f"{table_path}.sort_order",
                        "DuckDB 1.5 Iceberg UPDATE/DELETE does not support sorted tables",
                    )
                for index, item in enumerate(partition_spec):
                    if item["transform"] not in _DUCKDB_PARTITION_TRANSFORMS:
                        _fail(
                            f"{table_path}.partition_spec[{index}].transform",
                            f"DuckDB 1.5 Iceberg writes do not support {item['transform']!r}",
                        )
            resolved[(layer, name)] = ResolvedTable(
                layer=layer,
                namespace="ops" if layer == "bronze" and name == "bronze_apply_registry" else layer,
                name=name,
                columns=tuple(columns),
                profile_name=profile_name,
                format_version=profile["format_version"],
                partition_spec=tuple(partition_spec),
                sort_order=tuple(sort_order),
                write=write,
                maintenance=_validate_maintenance(
                    table_path,
                    _merge(profile.get("maintenance", {}), table_profile.get("maintenance", {})),
                ),
                publication=publication,
            )
    if len(resolved) != 42:
        _fail("catalog.ampere.layers", f"expected 42 tables including the ops registry, found {len(resolved)}")
    return AmpereContract(CONTRACT_VERSION, "ampere", profiles, resolved)


def validate_contract(path: str | Path = DEFAULT_CONTRACT_PATH) -> AmpereContract:
    """Load the contract and return the resolved table set after validation."""
    contract = load_contract(path)
    print(
        f"Validated contract v{contract.version}: "
        + ", ".join(f"{layer}={len(contract.layer_tables(layer))}" for layer in ("bronze", "silver", "gold"))
        + f"; total={len(contract.tables)}"
    )
    return contract


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--contract", type=Path, default=DEFAULT_CONTRACT_PATH)
    args = parser.parse_args()
    validate_contract(args.contract)


if __name__ == "__main__":
    main()
