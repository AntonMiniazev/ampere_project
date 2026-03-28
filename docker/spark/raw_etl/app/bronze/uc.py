"""Unity Catalog helpers for raw-to-bronze Spark jobs."""

from __future__ import annotations

import json
import logging
from urllib import error as urlerror
from urllib import request as urlrequest

from pyspark.sql import DataFrame, SparkSession, functions as F


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
    """Fetch a single UC table payload through the REST API.

    The Bronze pipeline uses this helper when it needs authoritative catalog
    metadata, for example to validate that a target table exists or to inspect
    its column contract. Returning the raw HTTP status together with the JSON
    body lets callers decide whether absence is acceptable or should fail the
    current Spark job.
    """
    fq = f"{catalog}.{schema}.{table}"
    return _uc_http_json(
        spark=spark,
        catalog=catalog,
        method="GET",
        path=f"/api/2.1/unity-catalog/tables/{fq}",
        logger=logger,
    )


def get_uc_table_payload(
    spark: SparkSession,
    catalog: str,
    schema: str,
    table: str,
    logger: logging.Logger,
) -> dict:
    """Fetch UC table metadata and fail clearly when table is missing."""
    status, payload = _uc_get_table(
        spark=spark,
        catalog=catalog,
        schema=schema,
        table=table,
        logger=logger,
    )
    if status != 200:
        raise RuntimeError(
            f"UC metadata check failed: table {catalog}.{schema}.{table} is not "
            f"available in Unity Catalog (HTTP {status}). Pre-create it in UC first."
        )
    return payload


def ensure_uc_external_delta_table(
    spark: SparkSession,
    catalog: str,
    schema: str,
    table: str,
    logger: logging.Logger,
) -> str:
    """Return the UC table name after verifying the table exists as external Delta."""
    payload = get_uc_table_payload(spark, catalog, schema, table, logger)
    table_type = str(payload.get("table_type") or "").upper()
    data_source_format = str(payload.get("data_source_format") or "").upper()
    if table_type != "EXTERNAL":
        raise RuntimeError(
            f"UC table {catalog}.{schema}.{table} must be EXTERNAL, got {table_type!r}."
        )
    if data_source_format != "DELTA":
        raise RuntimeError(
            f"UC table {catalog}.{schema}.{table} must use DELTA, got {data_source_format!r}."
        )
    return uc_table_name(catalog, schema, table)


def align_df_to_uc_schema(
    spark: SparkSession,
    df: DataFrame,
    catalog: str,
    schema: str,
    table: str,
    logger: logging.Logger,
) -> DataFrame:
    """Align a Spark DataFrame to the UC table schema.

    The function reads UC table metadata via REST and:
    - reorders columns to UC order,
    - casts present columns to UC types,
    - fills missing nullable columns with nulls,
    - drops extra DF columns not present in UC (with warning).

    Args:
        spark: Active SparkSession.
        df: DataFrame to align before bronze write/merge.
        catalog: UC catalog, e.g. "ampere".
        schema: UC schema, e.g. "bronze".
        table: Table name, e.g. "orders".
        logger: Run logger.
    Examples:
        df = align_df_to_uc_schema(spark, df, "ampere", "bronze", "orders", logger)
    """
    payload = get_uc_table_payload(spark, catalog, schema, table, logger)
    uc_columns = payload.get("columns") or []
    if not uc_columns:
        raise RuntimeError(
            f"UC metadata check failed: {catalog}.{schema}.{table} has no columns[] "
            "metadata. Recreate the UC table with explicit columns."
        )
    uc_columns = sorted(
        uc_columns,
        key=lambda c: (
            c.get("position") is None,
            c.get("position") or 0,
            c.get("name") or "",
        ),
    )
    uc_names = [str(col["name"]) for col in uc_columns if col.get("name")]
    uc_map = {str(col["name"]): col for col in uc_columns if col.get("name")}
    df_names = list(df.columns)
    df_set = set(df_names)
    uc_set = set(uc_names)

    extra_columns = [name for name in df_names if name not in uc_set]
    if extra_columns:
        logger.warning(
            "UC dropping %s extra columns for %s.%s.%s: %s",
            len(extra_columns),
            catalog,
            schema,
            table,
            extra_columns,
        )

    missing_required = []
    missing_nullable = []
    for name in uc_names:
        if name in df_set:
            continue
        if bool(uc_map[name].get("nullable", True)):
            missing_nullable.append(name)
        else:
            missing_required.append(name)
    if missing_required:
        raise ValueError(
            f"UC schema mismatch for {catalog}.{schema}.{table}: missing non-nullable "
            f"columns in DataFrame: {missing_required}"
        )

    aligned = df
    for name in missing_nullable:
        type_text = str(uc_map[name].get("type_text") or "string")
        aligned = aligned.withColumn(name, F.lit(None).cast(type_text))
    select_exprs = []
    for name in uc_names:
        type_text = str(uc_map[name].get("type_text") or "string")
        select_exprs.append(F.col(name).cast(type_text).alias(name))
    return aligned.select(*select_exprs)
