"""Spark Iceberg schema, partition, and property conformance checks."""

from __future__ import annotations

from typing import Any


def quote_identifier(value: str) -> str:
    return "`" + value.replace("`", "``") + "`"


def spark_type(value: str) -> str:
    normalized = value.strip().lower()
    return {"integer": "int"}.get(normalized, normalized)


def _active_partition_spec(spark: Any, target: str) -> list[tuple[str, str]]:
    """Read the active Iceberg spec from structured table metadata JSON."""
    try:
        rows = spark.sql(
            f"SELECT file FROM {target}.metadata_log_entries "
            "ORDER BY timestamp DESC LIMIT 1"
        ).collect()
    except Exception as exc:
        raise RuntimeError(
            f"Cannot read Iceberg metadata_log_entries for {target}"
        ) from exc
    if not rows or not rows[0]["file"]:
        raise RuntimeError(f"No current Iceberg metadata file found for {target}")

    metadata_uri = str(rows[0]["file"])
    if metadata_uri.startswith("s3://"):
        metadata_uri = "s3a://" + metadata_uri[len("s3://"):]
    try:
        metadata_row = (
            spark.read.option("multiLine", "true").json(metadata_uri).first()
        )
        if metadata_row is None:
            raise ValueError("metadata JSON contains no object")
        metadata = metadata_row.asDict(recursive=True)
    except Exception as exc:
        raise RuntimeError(
            f"Cannot load Iceberg metadata JSON {metadata_uri} for {target}"
        ) from exc

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
    field_names = {
        field.get("id"): field.get("name")
        for field in current_schemas[0].get("fields", [])
        if field.get("id") is not None and field.get("name") is not None
    }

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


def validate_spark_table(spark: Any, catalog: str, table: Any) -> str:
    """Fail when a Spark-visible Iceberg table differs from its v3 contract."""
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
    observed_spec = _active_partition_spec(spark, target)
    if observed_spec != expected_spec:
        raise ValueError(
            f"Iceberg active partition spec mismatch for {target}: "
            f"expected {expected_spec}, observed {observed_spec}"
        )
    return target
