"""Spark Iceberg schema, partition, and property conformance checks."""

from __future__ import annotations

from typing import Any


def quote_identifier(value: str) -> str:
    return "`" + value.replace("`", "``") + "`"


def spark_type(value: str) -> str:
    normalized = value.strip().lower()
    return {"integer": "int"}.get(normalized, normalized)


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

    description = spark.sql(f"DESCRIBE TABLE EXTENDED {target}").collect()
    rendered = " ".join(
        " ".join(str(value) for value in row)
        for row in description
    ).lower()
    missing = []
    for partition in table.partition_spec:
        source = partition["source"].lower()
        transform = partition["transform"].lower()
        alternatives = (
            (source, f"identity({source})")
            if transform == "identity"
            else (f"{transform}({source})",)
        )
        if not any(value in rendered for value in alternatives):
            missing.append(f"{transform}({source})")
    if missing:
        raise ValueError(f"Iceberg partition spec mismatch for {target}; missing {missing}")
    if not table.partition_spec and "# partitioning" in rendered and "not partitioned" not in rendered:
        raise ValueError(f"Iceberg table {target} should be unpartitioned")
    return target
