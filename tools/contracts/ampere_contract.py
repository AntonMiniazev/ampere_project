"""Load and resolve Ampere's canonical Iceberg contract."""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any


CONTRACT_VERSION = 7
LAYERS = ("raw", "bronze", "silver", "gold")
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
_SOURCE_COMPLETENESS = {"partial", "full_snapshot", "affected_month_snapshot"}


class ContractError(ValueError):
    """Raised when the canonical contract is malformed or unsupported."""


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
    ingestion: dict[str, Any] | None

    @property
    def column_names(self) -> tuple[str, ...]:
        return tuple(column["name"] for column in self.columns)


@dataclass(frozen=True)
class AmpereContract:
    version: int
    catalog_namespace: str
    profiles: dict[str, dict[str, Any]]
    layer_inventory: dict[str, int]
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
    if layer == "gold" and completeness["daily_refresh"] != "affected_month_snapshot":
        _fail(
            f"{table_path}.publication.source_completeness.daily_refresh",
            "Gold daily publication must be an affected_month_snapshot",
        )
    return {"merge_keys": tuple(keys), "source_completeness": dict(completeness)}


def _validate_ingestion(table_path: str, raw: Any, columns: dict[str, str]) -> dict[str, Any]:
    """Validate Raw change-history semantics without discarding contract fields."""
    ingestion = _object(raw, f"{table_path}.ingestion")
    if ingestion.get("kind") != "staged_typed_parquet_manifest":
        _fail(f"{table_path}.ingestion.kind", "must be staged_typed_parquet_manifest")

    def names(key: str, *, required: bool = True) -> tuple[str, ...]:
        value = ingestion.get(key)
        if not isinstance(value, list) or (required and not value):
            _fail(f"{table_path}.ingestion.{key}", "must be a non-empty ordered list")
        result = tuple(_identifier(item, f"{table_path}.ingestion.{key}") for item in value)
        if len(set(result)) != len(result):
            _fail(f"{table_path}.ingestion.{key}", "must not contain duplicates")
        missing = set(result) - columns.keys()
        if missing:
            _fail(f"{table_path}.ingestion.{key}", f"unknown contracted columns {sorted(missing)}")
        return result

    for key in ("source_schema", "source_table"):
        _identifier(ingestion.get(key), f"{table_path}.ingestion.{key}")
    change_keys = names("change_keys")
    entity_keys = names("entity_keys")
    source_order = names("source_order")
    business_columns = names("business_columns", required=False)
    required_metadata = names("required_metadata")
    if not business_columns:
        _fail(f"{table_path}.ingestion.business_columns", "must identify the typed source payload")

    metadata_required = {
        "_dataset_id", "_batch_id", "_batch_sequence", "_change_id",
        "_source_row_id", "_op", "_business_date", "_captured_at",
        "_transfer_id", "_ingest_run_id", "_ingest_ts", "_source_system",
        "_source_table", "_contract_version",
    }
    if not metadata_required.issubset(required_metadata):
        _fail(
            f"{table_path}.ingestion.required_metadata",
            f"must include {sorted(metadata_required)}",
        )
    for column in required_metadata:
        if column not in columns:
            _fail(f"{table_path}.ingestion.required_metadata", f"unknown contracted column {column!r}")

    operation_column = _identifier(ingestion.get("operation_column"), f"{table_path}.ingestion.operation_column")
    business_date_column = _identifier(ingestion.get("business_date_column"), f"{table_path}.ingestion.business_date_column")
    transfer_column = _identifier(ingestion.get("transfer_column"), f"{table_path}.ingestion.transfer_column")
    for key, value in (("operation_column", operation_column), ("business_date_column", business_date_column), ("transfer_column", transfer_column)):
        if value not in columns:
            _fail(f"{table_path}.ingestion.{key}", f"unknown contracted column {value!r}")
    operations = ingestion.get("operations")
    if (
        not isinstance(operations, list)
        or any(not isinstance(operation, str) for operation in operations)
        or set(operations) != {"I", "U", "D", "B"}
        or len(operations) != 4
    ):
        _fail(f"{table_path}.ingestion.operations", "must contain I, U, D, and B exactly once")
    if operation_column != "_op" or business_date_column != "_business_date" or transfer_column != "_transfer_id":
        _fail(f"{table_path}.ingestion", "operation, business-date, and transfer columns must use the canonical Raw envelope")
    if columns.get(business_date_column) != "date":
        _fail(f"{table_path}.ingestion.business_date_column", "must have type date")
    if not {"_dataset_id", "_batch_id", "_change_id"}.issubset(change_keys):
        _fail(f"{table_path}.ingestion.change_keys", "must identify dataset, batch, and change")
    if not {"_dataset_id", "_source_row_id"}.issubset(entity_keys):
        _fail(f"{table_path}.ingestion.entity_keys", "must identify dataset and stable source row")
    if not {"_batch_sequence", "_change_id"}.issubset(source_order):
        _fail(f"{table_path}.ingestion.source_order", "must order by batch sequence and change id")
    if ingestion.get("history_policy") != "retain_until_explicit_dataset_retirement":
        _fail(f"{table_path}.ingestion.history_policy", "unsupported Raw history policy")
    if ingestion.get("batch_visibility") != "completed_source_batch_and_acknowledged_raw_transfer":
        _fail(f"{table_path}.ingestion.batch_visibility", "unsupported Raw batch visibility policy")
    staging = _object(ingestion.get("staging"), f"{table_path}.ingestion.staging")
    if staging != {
        "manifest_version": 1,
        "file_format": "parquet",
        "compression": "zstd",
        "target_file_size_bytes": 134217728,
        "max_rows_per_chunk": 50000,
        "commit_marker": "COMMITTED.json",
        "eligibility": "completed_postgres_batch_and_validated_manifest",
    }:
        _fail(f"{table_path}.ingestion.staging", "must define the typed immutable batch staging contract")
    if ingestion["source_table"] != table_path.rsplit(".", 1)[-1]:
        _fail(f"{table_path}.ingestion.source_table", "must match the Raw table name")

    metadata_types = {
        "_dataset_id": {"string"},
        "_batch_id": {"string"},
        "_batch_sequence": {"bigint"},
        "_change_id": {"bigint"},
        "_source_row_id": {"string"},
        "_op": {"string"},
        "_business_date": {"date"},
        "_captured_at": {"timestamp", "timestamp_ntz"},
        "_transfer_id": {"string"},
        "_ingest_run_id": {"string"},
        "_ingest_ts": {"timestamp", "timestamp_ntz"},
        "_source_system": {"string"},
        "_source_table": {"string"},
        "_contract_version": {"int"},
    }
    for column, allowed_types in metadata_types.items():
        if columns.get(column) not in allowed_types:
            _fail(
                f"{table_path}.ingestion.required_metadata",
                f"{column} must have one of {sorted(allowed_types)}",
            )

    result = dict(ingestion)
    result.update(
        change_keys=change_keys,
        entity_keys=entity_keys,
        source_order=source_order,
        operations=tuple(operations),
        business_columns=business_columns,
        required_metadata=required_metadata,
    )
    return result


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

    _validate_maintenance(path, profile.get("maintenance", {}))
    return profile


def load_contract(path: str | Path = DEFAULT_CONTRACT_PATH) -> AmpereContract:
    """Read, validate, and resolve the current defaults for all declared table specs."""
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
    if set(layers) != set(LAYERS):
        _fail("catalog.ampere.layers", f"must declare {', '.join(LAYERS)}")
    inventory_raw = _object(ampere.get("table_inventory"), "catalog.ampere.table_inventory")
    if set(inventory_raw) != {*LAYERS, "total"}:
        _fail("catalog.ampere.table_inventory", f"must declare counts for {', '.join(LAYERS)} and total")
    inventory: dict[str, int] = {}
    for name, value in inventory_raw.items():
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            _fail(f"catalog.ampere.table_inventory.{name}", "must be a positive integer")
        inventory[name] = value
    if inventory["total"] != sum(inventory[layer] for layer in LAYERS):
        _fail("catalog.ampere.table_inventory.total", "must equal the sum of declared layer counts")
    profile_raw = _object(ampere.get("physical_profiles"), "catalog.ampere.physical_profiles")
    profiles = {
        _identifier(name, f"physical_profiles.{name}"): _validate_profile(name, value)
        for name, value in profile_raw.items()
    }

    resolved: dict[tuple[str, str], ResolvedTable] = {}
    for layer in LAYERS:
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
            ingestion = _validate_ingestion(
                table_path, table.get("ingestion"),
                {column["name"]: column["type_text"] for column in columns},
            ) if layer == "raw" else None
            if layer != "raw" and table.get("ingestion") is not None:
                _fail(f"{table_path}.ingestion", "is supported only for Raw tables")
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
                ingestion=ingestion,
            )
    observed_inventory = {
        layer: sum(1 for table_layer, _ in resolved if table_layer == layer)
        for layer in LAYERS
    }
    for layer in LAYERS:
        if observed_inventory[layer] != inventory[layer]:
            _fail(
                f"catalog.ampere.table_inventory.{layer}",
                f"declares {inventory[layer]} tables, found {observed_inventory[layer]}",
            )
    if len(resolved) != inventory["total"]:
        _fail("catalog.ampere.table_inventory.total", f"declares {inventory['total']} tables, found {len(resolved)}")
    return AmpereContract(CONTRACT_VERSION, "ampere", profiles, inventory, resolved)


def validate_contract(path: str | Path = DEFAULT_CONTRACT_PATH) -> AmpereContract:
    """Load the contract and return the resolved table set after validation."""
    contract = load_contract(path)
    print(
        f"Validated contract v{contract.version}: "
        + ", ".join(f"{layer}={len(contract.layer_tables(layer))}" for layer in LAYERS)
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
