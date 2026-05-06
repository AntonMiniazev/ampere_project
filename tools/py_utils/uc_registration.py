"""Shared UC registration helpers for Ampere layer notebooks."""

from __future__ import annotations

import json
import re
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.config import Config
from deltalake import DeltaTable

from .uc_client import UCClient


@dataclass(frozen=True)
class UCTableSpec:
    """Config-derived description of one external table registration target."""

    schema_name: str
    table_name: str
    storage_location: str
    data_source_format: str
    comment: str
    path: str
    columns: list[dict[str, Any]]
    properties: dict[str, Any]


def catalog_name(config: dict[str, Any]) -> str:
    """Return catalog name from either legacy object config or current string config."""
    catalog = config["catalog"]
    if isinstance(catalog, str):
        return catalog
    return catalog["name"]


def layer_table_specs(config: dict[str, Any], layer_name: str) -> list[UCTableSpec]:
    """Expand `layers` and `tables` config into concrete UC table specs."""
    layer = config["layers"][layer_name]
    bucket = layer["bucket"]
    path_prefix = layer.get("path_prefix", layer_name).strip("/")
    data_source_format = layer.get("format", "DELTA").upper()
    table_groups = config["tables"]

    specs: list[UCTableSpec] = []
    for schema_name in layer["schemas"]:
        for table_name, table_path in table_groups.get(schema_name, {}).items():
            normalized_path = table_path.strip("/")
            storage_location = f"s3://{bucket}/{path_prefix}/{normalized_path}"
            specs.append(
                UCTableSpec(
                    schema_name=schema_name,
                    table_name=table_name,
                    storage_location=storage_location,
                    data_source_format=data_source_format,
                    comment=f"{layer_name.capitalize()} table: {table_name}",
                    path=normalized_path,
                    columns=[],
                    properties={},
                )
            )
    return specs


def load_table_contracts(path: str | Path) -> list[UCTableSpec]:
    """Load explicit UC table metadata contracts from JSON."""
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    specs: list[UCTableSpec] = []
    for table in payload.get("tables", []):
        storage_location = table["storage_location"]
        _, _, table_path = storage_location[5:].partition("/")
        specs.append(
            UCTableSpec(
                schema_name=table["schema_name"],
                table_name=table["table_name"],
                storage_location=storage_location,
                data_source_format=table.get("data_source_format", "DELTA").upper(),
                comment=table.get("comment") or f"Table: {table['table_name']}",
                path=table_path,
                columns=table.get("columns", []),
                properties=table.get("properties", {}),
            )
        )
    return specs


def layer_schema_entries(config: dict[str, Any], layer_name: str) -> list[dict[str, str]]:
    """Return schema config entries that belong to a configured layer."""
    schema_names = set(config["layers"][layer_name]["schemas"])
    schema_entries = config.get("schemas", [])
    return [entry for entry in schema_entries if entry["name"] in schema_names]


def storage_options(config: dict[str, Any]) -> dict[str, str]:
    """Build delta-rs storage options from UC bulk storage config."""
    storage = config["storage"]
    options = {
        "AWS_ENDPOINT_URL": storage["endpoint"],
        "AWS_ACCESS_KEY_ID": storage["access_key"],
        "AWS_SECRET_ACCESS_KEY": storage["secret_key"],
        "AWS_REGION": storage.get("region", "us-east-1"),
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
        "AWS_S3_FORCE_PATH_STYLE": "true",
    }
    if storage["endpoint"].startswith("http://"):
        options["AWS_ALLOW_HTTP"] = "true"
    return options


def s3_client(config: dict[str, Any]):
    """Create a boto3 S3 client using the shared MinIO config."""
    storage = config["storage"]
    return boto3.client(
        "s3",
        endpoint_url=storage["endpoint"],
        aws_access_key_id=storage["access_key"],
        aws_secret_access_key=storage["secret_key"],
        region_name=storage.get("region", "us-east-1"),
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        verify=storage.get("ssl_verify", True),
    )


def arrow_type_name(dtype: Any) -> str:
    """Return a normalized type name for PyArrow and arro3 schema types."""
    return re.sub(r"[^a-z0-9]+", "", str(dtype).strip().lower())


def is_pyarrow_type(check: Any, dtype: Any) -> bool:
    """Run a PyArrow type predicate and treat non-PyArrow types as no match."""
    try:
        return bool(check(dtype))
    except (AttributeError, TypeError):
        return False


def decimal_precision_scale(dtype: Any) -> tuple[int, int]:
    """Extract decimal precision/scale from PyArrow or arro3 decimal types."""
    precision = getattr(dtype, "precision", None)
    scale = getattr(dtype, "scale", None)
    if precision is not None and scale is not None:
        return int(precision), int(scale)
    numbers = re.findall(r"\d+", str(dtype).lower())
    if len(numbers) >= 2:
        precision_text, scale_text = numbers[-2:]
        return int(precision_text), int(scale_text)
    return 38, 18


def uc_column_type(dtype: Any) -> tuple[str, str, str]:
    """Map PyArrow or arro3 type to Unity Catalog column metadata fields."""
    normalized = arrow_type_name(dtype)
    if is_pyarrow_type(pa.types.is_boolean, dtype) or "bool" in normalized:
        return ("BOOLEAN", "BOOLEAN", json.dumps({"name": "boolean"}))
    if (
        is_pyarrow_type(pa.types.is_int8, dtype)
        or is_pyarrow_type(pa.types.is_int16, dtype)
        or "int8" in normalized
        or "int16" in normalized
    ):
        return ("SHORT", "SMALLINT", json.dumps({"name": "short"}))
    if is_pyarrow_type(pa.types.is_int32, dtype) or "int32" in normalized:
        return ("INT", "INT", json.dumps({"name": "integer"}))
    if is_pyarrow_type(pa.types.is_int64, dtype) or "int64" in normalized:
        return ("LONG", "BIGINT", json.dumps({"name": "long"}))
    if (
        is_pyarrow_type(pa.types.is_float16, dtype)
        or is_pyarrow_type(pa.types.is_float32, dtype)
        or "float16" in normalized
        or "float32" in normalized
    ):
        return ("FLOAT", "FLOAT", json.dumps({"name": "float"}))
    if (
        is_pyarrow_type(pa.types.is_float64, dtype)
        or "float64" in normalized
        or "double" in normalized
    ):
        return ("DOUBLE", "DOUBLE", json.dumps({"name": "double"}))
    if is_pyarrow_type(pa.types.is_decimal, dtype) or "decimal" in normalized:
        precision, scale = decimal_precision_scale(dtype)
        return (
            "DECIMAL",
            f"DECIMAL({precision},{scale})",
            json.dumps(
                {
                    "name": "decimal",
                    "precision": precision,
                    "scale": scale,
                }
            ),
        )
    if (
        is_pyarrow_type(pa.types.is_string, dtype)
        or is_pyarrow_type(pa.types.is_large_string, dtype)
        or "string" in normalized
        or "utf8" in normalized
    ):
        return ("STRING", "STRING", json.dumps({"name": "string"}))
    if (
        is_pyarrow_type(pa.types.is_binary, dtype)
        or is_pyarrow_type(pa.types.is_large_binary, dtype)
        or "binary" in normalized
    ):
        return ("BINARY", "BINARY", json.dumps({"name": "binary"}))
    if (
        is_pyarrow_type(pa.types.is_date32, dtype)
        or is_pyarrow_type(pa.types.is_date64, dtype)
        or "date" in normalized
    ):
        return ("DATE", "DATE", json.dumps({"name": "date"}))
    if is_pyarrow_type(pa.types.is_timestamp, dtype) or "timestamp" in normalized:
        return ("TIMESTAMP", "TIMESTAMP", json.dumps({"name": "timestamp"}))
    return ("STRING", "STRING", json.dumps({"name": "string"}))


def uc_columns_from_arrow(
    schema: pa.Schema,
    extra_fields: list[pa.Field] | None = None,
) -> list[dict[str, Any]]:
    """Convert a PyArrow schema to a UC `columns` payload."""
    fields = list(schema)
    if extra_fields:
        existing_names = {field.name for field in fields}
        fields.extend(field for field in extra_fields if field.name not in existing_names)

    columns: list[dict[str, Any]] = []
    for position, field in enumerate(fields, start=1):
        type_name, type_text, type_json = uc_column_type(field.type)
        columns.append(
            {
                "name": field.name,
                "type_name": type_name,
                "type_text": type_text,
                "type_json": type_json,
                "position": position,
                "nullable": bool(field.nullable),
            }
        )
    return columns


def delta_columns(storage_location: str, options: dict[str, str]) -> list[dict[str, Any]]:
    """Read Delta schema and return UC column payload."""
    table = DeltaTable(storage_location, storage_options=options)
    return uc_columns_from_arrow(table.schema().to_arrow())


def first_parquet_key(client: Any, bucket: str, prefix: str) -> str:
    """Return the first Parquet object key under an S3 prefix."""
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix.rstrip("/") + "/"):
        for item in page.get("Contents", []):
            if item["Key"].endswith(".parquet"):
                return item["Key"]
    raise FileNotFoundError(f"No Parquet files found under s3://{bucket}/{prefix}")


def parquet_columns(
    client: Any,
    bucket: str,
    prefix: str,
    extra_fields: list[pa.Field] | None = None,
) -> list[dict[str, Any]]:
    """Read first Parquet file under a prefix and return UC column payload."""
    key = first_parquet_key(client, bucket, prefix)
    with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
        client.download_file(bucket, key, tmp.name)
        schema = pq.read_schema(tmp.name)
    return uc_columns_from_arrow(schema, extra_fields=extra_fields)


def ensure_schemas(
    uc: UCClient,
    schema_entries: list[dict[str, str]],
    report: dict[str, Any],
) -> None:
    """Create missing UC schemas and update the report in place."""
    existing_schemas = set(uc.list_schemas())
    for schema in schema_entries:
        schema_name = schema["name"]
        if schema_name in existing_schemas:
            print(f"[schema] exists: {schema_name}")
            report["schemas"]["skipped_existing"].append(schema_name)
            continue
        try:
            uc.create_schema(schema_name, comment=schema.get("comment"))
            print(f"[schema] created: {schema_name}")
            report["schemas"]["created"].append(schema_name)
            existing_schemas.add(schema_name)
        except Exception as exc:  # noqa: BLE001
            print(f"[schema] error creating {schema_name}: {exc}")
            report["schemas"]["errors"].append(
                {"schema": schema_name, "error": str(exc)}
            )


def ensure_catalog(
    uc: UCClient,
    comment: str | None,
    report: dict[str, Any],
) -> None:
    """Create the configured catalog if it does not exist."""
    if uc.config.catalog in set(uc.list_catalogs()):
        print(f"[catalog] exists: {uc.config.catalog}")
        report["catalog_status"] = {
            "status": "skipped_existing",
            "name": uc.config.catalog,
        }
        return
    uc.create_catalog(comment=comment)
    print(f"[catalog] created: {uc.config.catalog}")
    report["catalog_status"] = {"status": "created", "name": uc.config.catalog}


def register_external_tables(
    uc: UCClient,
    specs: list[UCTableSpec],
    columns_for_table: Callable[[UCTableSpec], list[dict[str, Any]]],
    report: dict[str, Any],
) -> None:
    """Register missing external tables and update the report in place."""
    existing_by_schema: dict[str, set[str]] = {}
    for spec in specs:
        if spec.schema_name not in existing_by_schema:
            existing_by_schema[spec.schema_name] = {
                item["name"] for item in uc.list_tables(spec.schema_name)
            }
        if spec.table_name in existing_by_schema[spec.schema_name]:
            full_name = f"{spec.schema_name}.{spec.table_name}"
            print(f"[table] exists: {full_name}")
            report["tables"]["skipped_existing"].append(full_name)
            continue

        try:
            columns = columns_for_table(spec)
            uc.create_table(
                schema_name=spec.schema_name,
                table_name=spec.table_name,
                storage_location=spec.storage_location,
                columns=columns,
                comment=spec.comment,
                data_source_format=spec.data_source_format,
                properties=spec.properties,
            )
            full_name = f"{spec.schema_name}.{spec.table_name}"
            print(f"[table] created: {full_name}")
            report["tables"]["created"].append(
                {
                    "full_name": full_name,
                    "storage_location": spec.storage_location,
                    "format": spec.data_source_format,
                    "column_count": len(columns),
                }
            )
        except Exception as exc:  # noqa: BLE001
            full_name = f"{spec.schema_name}.{spec.table_name}"
            print(f"[table] error creating {full_name}: {exc}")
            report["tables"]["errors"].append(
                {
                    "schema": spec.schema_name,
                    "name": spec.table_name,
                    "storage_location": spec.storage_location,
                    "format": spec.data_source_format,
                    "error": str(exc),
                }
            )


def replace_external_tables(
    uc: UCClient,
    specs: list[UCTableSpec],
    report: dict[str, Any],
) -> None:
    """Drop existing UC table metadata and recreate it from JSON contracts."""
    existing_by_schema: dict[str, set[str]] = {}
    for spec in specs:
        if spec.schema_name not in existing_by_schema:
            existing_by_schema[spec.schema_name] = {
                item["name"] for item in uc.list_tables(spec.schema_name)
            }
        full_name = f"{spec.schema_name}.{spec.table_name}"
        try:
            if spec.table_name in existing_by_schema[spec.schema_name]:
                uc.delete_table(spec.schema_name, spec.table_name)
                print(f"[table] deleted: {full_name}")
                report["tables"].setdefault("replaced", []).append(full_name)

            uc.create_table(
                schema_name=spec.schema_name,
                table_name=spec.table_name,
                storage_location=spec.storage_location,
                columns=spec.columns,
                comment=spec.comment,
                data_source_format=spec.data_source_format,
                properties=spec.properties,
            )
            print(f"[table] created: {full_name}")
            report["tables"]["created"].append(
                {
                    "full_name": full_name,
                    "storage_location": spec.storage_location,
                    "format": spec.data_source_format,
                    "column_count": len(spec.columns),
                }
            )
            existing_by_schema[spec.schema_name].add(spec.table_name)
        except Exception as exc:  # noqa: BLE001
            print(f"[table] error replacing {full_name}: {exc}")
            report["tables"]["errors"].append(
                {
                    "schema": spec.schema_name,
                    "name": spec.table_name,
                    "storage_location": spec.storage_location,
                    "format": spec.data_source_format,
                    "error": str(exc),
                }
            )
def empty_registration_report(
    catalog: str,
    layer_name: str,
    report_format: str,
) -> dict[str, Any]:
    """Create a standard registration report structure."""
    from datetime import datetime, timezone

    return {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "catalog": catalog,
        "catalog_status": {"status": None, "name": catalog},
        "layer": layer_name,
        "format": report_format,
        "schemas": {"created": [], "skipped_existing": [], "errors": []},
        "tables": {"created": [], "replaced": [], "skipped_existing": [], "errors": []},
    }


def write_report(report: dict[str, Any], path: str | Path) -> None:
    """Write registration report as pretty JSON and print a one-line summary."""
    report_path = Path(path)
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"\nReport written to: {report_path}")
    print(
        "Summary: "
        f"schemas created={len(report['schemas']['created'])}, "
        f"tables created={len(report['tables']['created'])}, "
        f"errors={len(report['schemas']['errors']) + len(report['tables']['errors'])}"
    )
