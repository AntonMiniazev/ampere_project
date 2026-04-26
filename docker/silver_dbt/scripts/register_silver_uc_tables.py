from __future__ import annotations

import argparse
import json
import os
import re
from pathlib import Path
from typing import Any
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import pyarrow as pa
from deltalake import DeltaTable


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Check published silver Delta tables against Unity Catalog locations."
    )
    parser.add_argument(
        "--publish-manifest-path",
        default=os.getenv(
            "SILVER_PUBLISH_MANIFEST_PATH",
            "/app/artifacts/silver_publish_manifest.json",
        ),
        help="Local publish manifest produced by publish_silver_tables.py.",
    )
    parser.add_argument(
        "--uc-api-uri",
        default=os.getenv("UC_API_URI", ""),
        help="Unity Catalog API base URI.",
    )
    parser.add_argument(
        "--catalog",
        default=os.getenv("SILVER_UC_CATALOG", os.getenv("BRONZE_UC_CATALOG", "ampere")),
        help="Unity Catalog catalog for silver tables.",
    )
    parser.add_argument(
        "--schema",
        default=os.getenv("SILVER_UC_SCHEMA", "silver"),
        help="Unity Catalog schema for silver tables.",
    )
    return parser.parse_args()


def endpoint_url() -> str:
    raw_endpoint = os.getenv("MINIO_S3_ENDPOINT", "minio.ampere.svc.cluster.local:9000")
    if raw_endpoint.startswith(("http://", "https://")):
        return raw_endpoint
    return f"http://{raw_endpoint}"


def delta_storage_options() -> dict[str, str]:
    endpoint = endpoint_url()
    options = {
        "AWS_ENDPOINT_URL": endpoint,
        "AWS_ACCESS_KEY_ID": os.getenv("MINIO_ACCESS_KEY", ""),
        "AWS_SECRET_ACCESS_KEY": os.getenv("MINIO_SECRET_KEY", ""),
        "AWS_REGION": os.getenv("MINIO_S3_REGION", "us-east-1"),
        "AWS_S3_FORCE_PATH_STYLE": "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }
    if endpoint.startswith("http://"):
        options["AWS_ALLOW_HTTP"] = "true"
    return options


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


def delta_columns(delta_uri: str) -> list[dict[str, Any]]:
    table = DeltaTable(delta_uri, storage_options=delta_storage_options())
    schema = table.schema().to_arrow()
    columns: list[dict[str, Any]] = []
    for position, field in enumerate(schema, start=1):
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


def assert_delta_readable(delta_uri: str) -> None:
    """Fail when the published Delta table cannot be opened at its target path."""
    DeltaTable(delta_uri, storage_options=delta_storage_options())


def uc_request(
    base_uri: str,
    method: str,
    path: str,
    query: dict[str, str] | None = None,
) -> dict[str, Any]:
    url = f"{base_uri.rstrip('/')}{path}"
    if query:
        url = f"{url}?{urlencode(query)}"

    headers = {"Content-Type": "application/json"}
    token = os.getenv("UC_TOKEN", "")
    if token:
        headers["Authorization"] = f"Bearer {token}"

    request = Request(url, method=method, headers=headers)
    try:
        with urlopen(request, timeout=60) as response:
            body = response.read().decode("utf-8")
    except HTTPError as exc:
        body = exc.read().decode("utf-8")
        try:
            error = json.loads(body)
        except json.JSONDecodeError:
            raise RuntimeError(f"UC API HTTP {exc.code}: {body}") from exc
        if exc.code == 404:
            raise FileNotFoundError(error.get("message", body)) from exc
        raise RuntimeError(f"UC API HTTP {exc.code}: {error}") from exc

    if not body:
        return {}
    data_obj = json.loads(body)
    if isinstance(data_obj, dict) and data_obj.get("error_code"):
        raise RuntimeError(f"UC API error {data_obj.get('error_code')}: {data_obj}")
    return data_obj


def get_uc_table(
    base_uri: str,
    catalog: str,
    schema: str,
    table_name: str,
) -> dict[str, Any]:
    try:
        return uc_request(
            base_uri,
            "GET",
            f"/api/2.1/unity-catalog/tables/{catalog}.{schema}.{table_name}",
        )
    except FileNotFoundError as exc:
        raise RuntimeError(
            f"Missing UC silver table {catalog}.{schema}.{table_name}. "
            "Initialize or repair UC metadata from "
            "internal_docs/test/unity_catalog_check/schemas/uc_silver_tables.json "
            "before running the silver pipeline."
        ) from exc


def normalize_location(value: str) -> str:
    return value.strip().rstrip("/")


def comparable_column(column: dict[str, Any]) -> dict[str, Any]:
    return {
        "name": column.get("name"),
        "type_name": str(column.get("type_name") or "").upper(),
        "type_text": str(column.get("type_text") or "").lower(),
        "position": int(column.get("position") or 0),
        "nullable": bool(column.get("nullable", True)),
    }


def validate_uc_table(
    table_name: str,
    storage_location: str,
    uc_payload: dict[str, Any],
) -> None:
    """Validate post-publish table existence, format, location, and readability.

    Schema compatibility is enforced before publish in publish_silver_tables.py.
    Re-reading the written Delta schema here is only detection after data is
    already published, so this post-publish step intentionally avoids a second
    hard schema comparison.
    """
    errors: list[str] = []
    if str(uc_payload.get("table_type") or "").upper() != "EXTERNAL":
        errors.append(f"table_type={uc_payload.get('table_type')!r}, expected EXTERNAL")
    if str(uc_payload.get("data_source_format") or "").upper() != "DELTA":
        errors.append(
            "data_source_format="
            f"{uc_payload.get('data_source_format')!r}, expected DELTA"
        )
    if normalize_location(str(uc_payload.get("storage_location") or "")) != normalize_location(
        storage_location
    ):
        errors.append(
            "storage_location="
            f"{uc_payload.get('storage_location')!r}, expected {storage_location!r}"
        )

    try:
        assert_delta_readable(storage_location)
    except Exception as exc:  # noqa: BLE001
        errors.append(f"published Delta table is not readable: {exc}")

    if errors:
        joined_errors = "; ".join(errors)
        raise RuntimeError(
            f"UC post-publish check failed for {table_name}: {joined_errors}"
        )


def main() -> None:
    args = parse_args()
    if not args.uc_api_uri:
        raise SystemExit("UC_API_URI is required for silver UC validation.")

    publish_manifest = json.loads(
        Path(args.publish_manifest_path).read_text(encoding="utf-8")
    )
    failures = []
    for table in publish_manifest.get("tables", []):
        table_name = table["model_name"]
        storage_location = table["data_uri"]
        try:
            uc_payload = get_uc_table(
                args.uc_api_uri,
                args.catalog,
                args.schema,
                table_name,
            )
            validate_uc_table(table_name, storage_location, uc_payload)
            print(f"UC post-publish check valid: {args.catalog}.{args.schema}.{table_name}")
        except Exception as exc:  # noqa: BLE001
            failures.append(str(exc))
            print(
                f"UC post-publish check invalid: "
                f"{args.catalog}.{args.schema}.{table_name}: {exc}"
            )

    if failures:
        raise SystemExit(
            "Silver post-publish check failed. Confirm published Delta paths are "
            "readable and UC locations/formats match the JSON contract before rerunning.\n"
            + "\n".join(failures)
        )


if __name__ == "__main__":
    main()
