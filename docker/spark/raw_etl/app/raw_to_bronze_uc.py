"""Unity Catalog helpers for raw-to-bronze Spark jobs.

This module keeps UC-specific logic isolated so path-based ETL can stay
unchanged and UC enablement can be toggled with runtime flags.
"""

from __future__ import annotations

import json
import logging
from urllib import error as urlerror
from urllib import request as urlrequest

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
    TimestampType,
)


def parse_bool_flag(value: str | bool | None, default: bool = False) -> bool:
    """Parse a boolean-like CLI/env value.

    Examples:
        parse_bool_flag("true") -> True
        parse_bool_flag("0") -> False
        parse_bool_flag(None, default=True) -> True
    """
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False
    return default


def quote_ident(value: str) -> str:
    """Quote Spark SQL identifier safely."""
    return "`" + value.replace("`", "``") + "`"


def uc_table_name(catalog: str, schema: str, table: str) -> str:
    """Build a fully-qualified UC table name."""
    return ".".join((quote_ident(catalog), quote_ident(schema), quote_ident(table)))


def ensure_uc_schema(
    spark: SparkSession,
    catalog: str,
    schema: str,
    logger: logging.Logger,
) -> None:
    """Create UC schema if missing.

    This is idempotent and safe to run on every batch job.
    """
    spark.sql(
        f"CREATE SCHEMA IF NOT EXISTS {quote_ident(catalog)}.{quote_ident(schema)}"
    )
    logger.info("Ensured UC schema exists: %s.%s", catalog, schema)


def _column_type_sql(data_type) -> str:
    """Return Spark SQL type text for a PySpark data type."""
    # `simpleString()` is accepted by Spark SQL DDL for common/complex types.
    return data_type.simpleString()


def _uc_column_type_parts(data_type) -> tuple[str, str, str]:
    """Map Spark SQL types to UC column metadata fields."""
    if isinstance(data_type, BooleanType):
        return ("BOOLEAN", "BOOLEAN", json.dumps({"name": "boolean"}))
    if isinstance(data_type, ShortType):
        return ("SHORT", "SMALLINT", json.dumps({"name": "short"}))
    if isinstance(data_type, IntegerType):
        return ("INT", "INT", json.dumps({"name": "integer"}))
    if isinstance(data_type, LongType):
        return ("LONG", "BIGINT", json.dumps({"name": "long"}))
    if isinstance(data_type, FloatType):
        return ("FLOAT", "FLOAT", json.dumps({"name": "float"}))
    if isinstance(data_type, DoubleType):
        return ("DOUBLE", "DOUBLE", json.dumps({"name": "double"}))
    if isinstance(data_type, DecimalType):
        return (
            "DECIMAL",
            f"DECIMAL({data_type.precision},{data_type.scale})",
            json.dumps(
                {
                    "name": "decimal",
                    "precision": int(data_type.precision),
                    "scale": int(data_type.scale),
                }
            ),
        )
    if isinstance(data_type, StringType):
        return ("STRING", "STRING", json.dumps({"name": "string"}))
    if isinstance(data_type, BinaryType):
        return ("BINARY", "BINARY", json.dumps({"name": "binary"}))
    if isinstance(data_type, DateType):
        return ("DATE", "DATE", json.dumps({"name": "date"}))
    if isinstance(data_type, (TimestampType, TimestampNTZType)):
        return ("TIMESTAMP", "TIMESTAMP", json.dumps({"name": "timestamp"}))
    # Conservative fallback for complex / unsupported types in this helper.
    return ("STRING", _column_type_sql(data_type).upper(), json.dumps({"name": "string"}))


def _columns_ddl(struct: StructType) -> str:
    """Build column DDL for CREATE TABLE from a Spark StructType."""
    columns = []
    for field in struct.fields:
        nullable_suffix = "" if field.nullable else " NOT NULL"
        columns.append(
            f"{quote_ident(field.name)} {_column_type_sql(field.dataType)}{nullable_suffix}"
        )
    return ", ".join(columns)


def _uc_columns_payload(struct: StructType) -> list[dict]:
    """Build UC REST API column payload from Spark schema."""
    payload: list[dict] = []
    for idx, field in enumerate(struct.fields, start=1):
        type_name, type_text, type_json = _uc_column_type_parts(field.dataType)
        payload.append(
            {
                "name": field.name,
                "type_name": type_name,
                "type_text": type_text,
                "type_json": type_json,
                "position": idx,
                "nullable": bool(field.nullable),
            }
        )
    return payload


def _table_has_columns(spark: SparkSession, fq_table: str) -> bool:
    """Best-effort check whether a registered table has visible columns."""
    try:
        described = spark.sql(f"DESCRIBE TABLE {fq_table}").collect()
    except Exception:
        return False
    for row in described:
        col_name = (row[0] or "").strip() if len(row) > 0 else ""
        if not col_name or col_name.startswith("#"):
            continue
        if col_name.lower() in {"col_name"}:
            continue
        return True
    return False


def _uc_api_base(spark: SparkSession, catalog: str) -> str:
    """Read UC API base URL from Spark catalog config."""
    uri = spark.conf.get(f"spark.sql.catalog.{catalog}.uri", "").strip().rstrip("/")
    if not uri:
        raise RuntimeError(
            f"Missing Spark config spark.sql.catalog.{catalog}.uri required for UC sync."
        )
    return uri


def _uc_auth_headers(spark: SparkSession, catalog: str) -> dict[str, str]:
    """Build optional auth headers for UC REST calls from Spark config."""
    headers = {"Content-Type": "application/json"}
    token = spark.conf.get(f"spark.sql.catalog.{catalog}.auth.token", "").strip()
    auth_type = spark.conf.get(f"spark.sql.catalog.{catalog}.auth.type", "").strip().lower()
    if token and auth_type in {"", "static"}:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def _uc_http_json(
    spark: SparkSession,
    catalog: str,
    method: str,
    path: str,
    logger: logging.Logger,
    payload: dict | None = None,
) -> tuple[int, dict]:
    """Execute a UC REST request and parse JSON response."""
    url = _uc_api_base(spark, catalog) + path
    body = None
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
    req = urlrequest.Request(
        url,
        data=body,
        method=method.upper(),
        headers=_uc_auth_headers(spark, catalog),
    )
    try:
        with urlrequest.urlopen(req, timeout=20) as resp:  # noqa: S310
            raw = resp.read().decode("utf-8") if resp.length != 0 else ""
            return resp.status, (json.loads(raw) if raw else {})
    except urlerror.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        try:
            data = json.loads(raw) if raw else {}
        except Exception:  # noqa: BLE001
            data = {"raw": raw}
        logger.debug("UC API %s %s failed: %s", method, path, raw[:1000])
        return exc.code, data


def _uc_get_table(
    spark: SparkSession,
    catalog: str,
    schema: str,
    table: str,
    logger: logging.Logger,
) -> tuple[int, dict]:
    fq = f"{catalog}.{schema}.{table}"
    return _uc_http_json(
        spark=spark,
        catalog=catalog,
        method="GET",
        path=f"/api/2.1/unity-catalog/tables/{fq}",
        logger=logger,
    )


def _uc_create_external_delta_table(
    spark: SparkSession,
    catalog: str,
    schema: str,
    table: str,
    uc_location: str,
    delta_schema: StructType,
    logger: logging.Logger,
) -> None:
    """Create an external Delta table in UC via REST with explicit columns."""
    payload = {
        "name": table,
        "catalog_name": catalog,
        "schema_name": schema,
        "table_type": "EXTERNAL",
        "data_source_format": "DELTA",
        "storage_location": uc_location,
        "columns": _uc_columns_payload(delta_schema),
    }
    status, data = _uc_http_json(
        spark=spark,
        catalog=catalog,
        method="POST",
        path="/api/2.1/unity-catalog/tables",
        logger=logger,
        payload=payload,
    )
    if status in {200, 201}:
        return
    body_text = json.dumps(data)[:2000]
    if status in {400, 409} and ("already exists" in body_text.lower() or "already_exists" in body_text.lower()):
        return
    raise RuntimeError(
        f"UC create table failed for {catalog}.{schema}.{table} (HTTP {status}): {body_text}"
    )


def sync_external_delta_table(
    spark: SparkSession,
    catalog: str,
    schema: str,
    table: str,
    location: str,
    logger: logging.Logger,
) -> None:
    """Register or keep UC external table pointing to Delta path.

    The `CREATE TABLE IF NOT EXISTS ... USING DELTA LOCATION ...` command is
    metadata-only for existing Delta tables, so it is cheap and idempotent.
    """
    # Spark/Delta on Hadoop resolves MinIO via `s3a://`, while UC REST/storage
    # mappings and temporary-credentials API use `s3://`.
    uc_location = location.replace("s3a://", "s3://", 1)
    fq_table = uc_table_name(catalog, schema, table)
    delta_schema = spark.read.format("delta").load(location).schema
    status, table_payload = _uc_get_table(
        spark=spark,
        catalog=catalog,
        schema=schema,
        table=table,
        logger=logger,
    )
    table_exists = status == 200
    if not table_exists:
        _uc_create_external_delta_table(
            spark=spark,
            catalog=catalog,
            schema=schema,
            table=table,
            uc_location=uc_location,
            delta_schema=delta_schema,
            logger=logger,
        )
    else:
        columns = table_payload.get("columns") or []
        if not columns:
            logger.warning(
                "UC table %s exists but has no visible columns; consider drop/recreate "
                "to repopulate metadata.",
                f"{catalog}.{schema}.{table}",
            )
    logger.info(
        "Synced UC table %s -> %s",
        f"{catalog}.{schema}.{table}",
        uc_location,
    )
