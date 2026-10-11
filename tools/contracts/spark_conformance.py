"""Spark Iceberg schema, partition, and property conformance checks."""

from __future__ import annotations

import gzip
import json
from typing import Any


def quote_identifier(value: str) -> str:
    return "`" + value.replace("`", "``") + "`"


def spark_type(value: str) -> str:
    normalized = value.strip().lower()
    return {"integer": "int"}.get(normalized, normalized)


def _active_metadata(spark: Any, target: str) -> dict[str, Any]:
    """Read the active Iceberg metadata file through binaryFile."""
    try:
        rows = spark.sql(
            f"SELECT file FROM {target}.metadata_log_entries "
            "ORDER BY timestamp DESC LIMIT 1"
        ).collect()
    except Exception as exc:
        raise RuntimeError(f"Cannot read Iceberg metadata_log_entries for {target}") from exc
    if not rows or not rows[0]["file"]:
        raise RuntimeError(f"No current Iceberg metadata file found for {target}")

    metadata_uri = str(rows[0]["file"])
    if metadata_uri.startswith("s3://"):
        metadata_uri = "s3a://" + metadata_uri[len("s3://"):]
    try:
        metadata_row = (
            spark.read.format("binaryFile")
            .load(metadata_uri)
            .select("content")
            .first()
        )
        if metadata_row is None:
            raise ValueError("metadata file contains no row")
        content = metadata_row["content"]
        if not isinstance(content, (bytes, bytearray, memoryview)):
            raise TypeError("binaryFile returned metadata content that is not bytes")
        payload = bytes(content)
        if payload.startswith(b"\x1f\x8b"):
            payload = gzip.decompress(payload)
        metadata = json.loads(payload)
        if not isinstance(metadata, dict):
            raise ValueError("metadata JSON contains no object")
    except Exception as exc:
        raise RuntimeError(f"Cannot load Iceberg metadata JSON {metadata_uri} for {target}") from exc
    return metadata


def _schema_names(metadata: dict[str, Any], target: str) -> dict[int, str]:
    current_schema_id = metadata.get("current-schema-id")
    schemas = metadata.get("schemas")
    if current_schema_id is None or not isinstance(schemas, list):
        raise ValueError(f"Iceberg metadata for {target} has no current schema")
    current_schemas = [schema for schema in schemas if schema.get("schema-id") == current_schema_id]
    if len(current_schemas) != 1:
        raise ValueError(
            f"Iceberg metadata for {target} has {len(current_schemas)} schemas matching "
            f"current-schema-id {current_schema_id}"
        )
    return {
        field.get("id"): field.get("name")
        for field in current_schemas[0].get("fields", [])
        if field.get("id") is not None and field.get("name") is not None
    }


def _active_partition_spec(
    spark: Any, target: str, *, metadata: dict[str, Any] | None = None
) -> list[tuple[str, str]]:
    """Read the active Iceberg partition spec from structured metadata JSON."""
    metadata = metadata or _active_metadata(spark, target)
    default_spec_id = metadata.get("default-spec-id")
    specs = metadata.get("partition-specs")
    if default_spec_id is None or not isinstance(specs, list):
        raise ValueError(f"Iceberg metadata for {target} has no default partition spec")
    active = [spec for spec in specs if spec.get("spec-id") == default_spec_id]
    if len(active) != 1:
        raise ValueError(
            f"Iceberg metadata for {target} has {len(active)} specs matching "
            f"default-spec-id {default_spec_id}"
        )

    field_names = _schema_names(metadata, target)

    observed = []
    for field in active[0].get("fields", []):
        source_id = field.get("source-id")
        source_name = field_names.get(source_id)
        transform = field.get("transform")
        if source_name is None or not isinstance(transform, str):
            raise ValueError(
                f"Iceberg metadata for {target} has an invalid partition field {field}"
            )
        observed.append((str(source_name), transform.lower()))
    return observed


def _active_sort_order(
    spark: Any, target: str, *, metadata: dict[str, Any] | None = None
) -> list[tuple[str, str]]:
    """Read and resolve the active Iceberg sort order from table metadata."""
    metadata = metadata or _active_metadata(spark, target)
    order_id = metadata.get("default-sort-order-id")
    orders = metadata.get("sort-orders")
    if order_id is None or not isinstance(orders, list):
        raise ValueError(f"Iceberg metadata for {target} has no default sort order")
    active = [order for order in orders if order.get("order-id") == order_id]
    if len(active) != 1:
        raise ValueError(
            f"Iceberg metadata for {target} has {len(active)} sort orders matching "
            f"default-sort-order-id {order_id}"
        )
    field_names = _schema_names(metadata, target)
    result = []
    for field in active[0].get("fields", []):
        source_name = field_names.get(field.get("source-id"))
        direction = field.get("direction")
        if source_name is None or direction not in {"asc", "desc"}:
            raise ValueError(f"Iceberg metadata for {target} has an invalid sort field {field}")
        result.append((str(source_name), direction))
    return result


def validate_spark_table(spark: Any, catalog: str, table: Any) -> str:
    """Fail when a Spark-visible Iceberg table differs from its v4 contract."""
    target = ".".join(
        quote_identifier(part)
        for part in (catalog, table.namespace, table.name)
    )
    try:
        observed_schema = spark.table(target).schema
    except Exception as exc:
        raise RuntimeError(f"Contract expects initialized table {target}") from exc
    expected = [(column["name"], spark_type(column["type_text"])) for column in table.columns]
    observed = [(field.name, spark_type(field.dataType.simpleString())) for field in observed_schema.fields]
    if observed != expected:
        raise ValueError(f"Iceberg schema mismatch for {target}: expected {expected}, observed {observed}")

    properties = {
        str(row.key): str(row.value)
        for row in spark.sql(f"SHOW TBLPROPERTIES {target}").collect()
    }
    expected_properties = {
        "format-version": str(table.format_version),
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
        raise ValueError(f"Iceberg properties mismatch for {target}: {mismatches}")

    expected_spec = []
    transform_names = {"months": "month", "days": "day", "years": "year", "hours": "hour"}
    for field in table.partition_spec:
        transform = field["transform"].lower()
        expected_spec.append((field["source"], transform_names.get(transform, transform)))
    metadata = _active_metadata(spark, target)
    observed_spec = _active_partition_spec(spark, target, metadata=metadata)
    if observed_spec != expected_spec:
        raise ValueError(
            f"Iceberg active partition spec mismatch for {target}: "
            f"expected {expected_spec}, observed {observed_spec}"
        )
    expected_sort = [
        (field["source"], field.get("direction", "asc").lower())
        for field in table.sort_order
    ]
    observed_sort = _active_sort_order(spark, target, metadata=metadata)
    if observed_sort != expected_sort:
        raise ValueError(
            f"Iceberg active sort order mismatch for {target}: "
            f"expected {expected_sort}, observed {observed_sort}"
        )
    return target
