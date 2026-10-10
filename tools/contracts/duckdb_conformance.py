"""Validate DuckDB-visible Iceberg tables against contract v3."""

from __future__ import annotations

import json
from typing import Any


def _as_mapping(value: Any, description: str) -> dict[str, Any]:
    if isinstance(value, bytes):
        value = value.decode("utf-8")
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Cannot parse Iceberg {description} metadata") from exc
    if not isinstance(value, dict):
        raise ValueError(f"Iceberg {description} metadata is not an object")
    return value


def _transform(value: str) -> str:
    return {"month": "months", "year": "years", "day": "days", "hour": "hours"}.get(value, value)


def validate_duckdb_table(con: Any, relation: str, table: Any) -> None:
    """Fail before publication if schema, partition/sort layout, or properties drift."""
    actual_schema = [
        (row[0], row[1].strip().lower().replace(" ", ""))
        for row in con.execute(f"DESCRIBE {relation}").fetchall()
    ]
    aliases = {"varchar": "string", "text": "string", "integer": "int", "timestamp": "timestamp_ntz"}
    actual_schema = [(name, aliases.get(dtype, dtype)) for name, dtype in actual_schema]
    expected_schema = [
        (column["name"], aliases.get(column["type_text"].strip().lower().replace(" ", ""), column["type_text"].strip().lower().replace(" ", "")))
        for column in table.columns
    ]
    if actual_schema != expected_schema:
        raise ValueError(
            f"Contract v3 schema mismatch for {relation}: "
            f"expected {expected_schema}, observed {actual_schema}"
        )

    property_rows = con.execute(f"SELECT * FROM iceberg_table_properties({relation})").fetchall()
    property_names = [column[0].lower() for column in con.description]
    if not {"key", "value"}.issubset(property_names):
        raise ValueError(f"Unexpected iceberg_table_properties() shape for {relation}: {property_names}")
    key_index, value_index = property_names.index("key"), property_names.index("value")
    properties = {str(row[key_index]): str(row[value_index]) for row in property_rows}
    response = con.execute(f"SELECT metadata FROM iceberg_load_table_response({relation})").fetchone()
    if response is None:
        raise ValueError(f"Lakekeeper returned no metadata for {relation}")
    metadata = _as_mapping(response[0], f"table {relation}")
    observed_version = int(metadata.get("format-version", -1))
    if observed_version != table.format_version:
        raise ValueError(
            f"Iceberg format version mismatch for {relation}: "
            f"expected {table.format_version}, observed {observed_version}"
        )

    expected_properties = {
        "write.distribution-mode": str(table.write.get("distribution", "none")),
    }
    target_size = table.write.get("target_file_size_bytes")
    if target_size is not None:
        expected_properties["write.target-file-size-bytes"] = str(target_size)
    mismatches = {
        key: {"expected": value, "actual": properties.get(key)}
        for key, value in expected_properties.items()
        if properties.get(key) != value
    }
    if mismatches:
        raise ValueError(f"Iceberg property mismatch for {relation}: {mismatches}")

    current_schema_id = metadata.get("current-schema-id")
    schemas = {int(schema["schema-id"]): schema for schema in metadata.get("schemas", [])}
    schema = schemas.get(int(current_schema_id)) if current_schema_id is not None else None
    if schema is None:
        raise ValueError(f"Lakekeeper metadata has no current schema for {relation}")
    names_by_id = {int(field["id"]): field["name"] for field in schema.get("fields", [])}

    default_spec_id = int(metadata.get("default-spec-id", -1))
    specs = {int(spec["spec-id"]): spec for spec in metadata.get("partition-specs", [])}
    partition_spec = specs.get(default_spec_id)
    if partition_spec is None:
        raise ValueError(f"Lakekeeper metadata has no default partition spec for {relation}")
    observed_partitions = [
        (names_by_id.get(int(field["source-id"])), _transform(str(field["transform"])))
        for field in partition_spec.get("fields", [])
    ]
    expected_partitions = [
        (field["source"], field["transform"]) for field in table.partition_spec
    ]
    if observed_partitions != expected_partitions:
        raise ValueError(
            f"Iceberg partition spec mismatch for {relation}: "
            f"expected {expected_partitions}, observed {observed_partitions}"
        )

    default_sort_id = int(metadata.get("default-sort-order-id", 0))
    sort_orders = {int(order["order-id"]): order for order in metadata.get("sort-orders", [])}
    sort_order = sort_orders.get(default_sort_id, {"fields": []})
    observed_sort = [
        (names_by_id.get(int(field["source-id"])), str(field["direction"]).lower())
        for field in sort_order.get("fields", [])
    ]
    expected_sort = [
        (field["source"], field.get("direction", "asc").lower())
        for field in table.sort_order
    ]
    if observed_sort != expected_sort:
        raise ValueError(
            f"Iceberg sort order mismatch for {relation}: "
            f"expected {expected_sort}, observed {observed_sort}"
        )
