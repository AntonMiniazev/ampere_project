"""Delta Lake helpers for local inspection tools."""

from __future__ import annotations

from typing import Any

import polars as pl
from deltalake import DeltaTable

from .uc_config import load_uc_bulk_config


def storage_options(config: dict[str, Any] | None = None) -> dict[str, str]:
    """Build delta-rs storage options from local UC/MinIO config."""
    cfg = config or load_uc_bulk_config()
    storage = cfg["storage"]
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
    if not bool(storage.get("ssl_verify", True)):
        options["AWS_ALLOW_INVALID_CERTIFICATES"] = "true"
    return options


def is_table_not_found(exc: Exception) -> bool:
    """Return true when a delta-rs exception means the table is absent."""
    msg = str(exc).lower()
    type_name = type(exc).__name__.lower()
    if "tablenotfounderror" in type_name:
        return True
    needles = [
        "no files in log segment",
        "_delta_log",
        "not a delta table",
        "invalid delta table",
        "table not found",
        "not found",
    ]
    return any(needle in msg for needle in needles)


def load_delta_table_as_df(
    storage_location: str,
    options: dict[str, str] | None = None,
) -> pl.DataFrame:
    """Load a Delta table from object storage into a Polars DataFrame."""
    table = DeltaTable(storage_location, storage_options=options or storage_options())
    return pl.from_arrow(table.to_pyarrow_table())


def load_latest_snapshot_as_df(
    storage_location: str,
    snapshot_column: str = "snapshot_date",
    options: dict[str, str] | None = None,
) -> pl.DataFrame:
    """Load a Delta table and filter to the latest snapshot partition if present."""
    df = load_delta_table_as_df(storage_location, options=options)
    if snapshot_column in df.columns and not df.is_empty():
        latest_snapshot = df.select(pl.col(snapshot_column).max()).to_series().item()
        return df.filter(pl.col(snapshot_column) == latest_snapshot)
    return df


def _shared_compare_columns(raw_df: pl.DataFrame, bronze_df: pl.DataFrame) -> list[str]:
    """Return comparable non-technical columns present in both DataFrames."""
    tech_prefixes = ("_bronze_", "__batch_")
    tech_columns = {
        "_ingest_run_id",
        "_ingest_ts",
        "_source_system",
        "_source_table",
        "_op",
        "_row_hash",
    }
    return [
        column
        for column in raw_df.columns
        if column in bronze_df.columns
        and column not in tech_columns
        and not any(column.startswith(prefix) for prefix in tech_prefixes)
    ]


def _normalized_key_expr(df: pl.DataFrame, column: str, alias: str):
    """Build a normalized comparison expression for a key column."""
    dtype = df.schema[column]
    expr = pl.col(column)
    if isinstance(dtype, pl.datatypes.Datetime):
        if dtype.time_zone is None:
            expr = expr.dt.replace_time_zone("UTC")
        else:
            expr = expr.dt.convert_time_zone("UTC")
        return expr.dt.timestamp("us").alias(alias)
    if isinstance(dtype, pl.datatypes.Date):
        return expr.cast(pl.Date).alias(alias)
    if isinstance(dtype, pl.datatypes.Categorical):
        return expr.cast(pl.Utf8).alias(alias)
    return expr.alias(alias)


def _normalized_keys_df(df: pl.DataFrame, keys: list[str]) -> pl.DataFrame:
    """Return normalized comparison keys as a new DataFrame."""
    return df.select([_normalized_key_expr(df, column, column) for column in keys])


def _with_normalized_cmp_keys(
    df: pl.DataFrame,
    keys: list[str],
) -> tuple[pl.DataFrame, list[str]]:
    """Append normalized comparison keys to the input DataFrame."""
    cmp_keys = [f"__cmp_{column}" for column in keys]
    exprs = [
        _normalized_key_expr(df, column, cmp_column)
        for column, cmp_column in zip(keys, cmp_keys)
    ]
    return df.with_columns(exprs), cmp_keys


def rows_missing_in_delta(
    raw_df: pl.DataFrame,
    delta_df: pl.DataFrame,
    key_columns: list[str] | None = None,
) -> pl.DataFrame:
    """Return raw rows whose keys are absent from a Delta-loaded DataFrame."""
    if raw_df.is_empty() or delta_df.is_empty():
        return raw_df

    keys = [key for key in (key_columns or []) if key in raw_df.columns and key in delta_df.columns]
    if not keys:
        keys = _shared_compare_columns(raw_df, delta_df)
    if not keys:
        return raw_df

    raw_key_df = _normalized_keys_df(raw_df, keys)
    delta_key_df = _normalized_keys_df(delta_df, keys).unique(subset=keys)
    missing_key_df = raw_key_df.unique(subset=keys).join(delta_key_df, on=keys, how="anti")

    raw_with_cmp_keys, cmp_keys = _with_normalized_cmp_keys(raw_df, keys)
    missing_cmp_keys = missing_key_df.rename(
        {key: cmp_key for key, cmp_key in zip(keys, cmp_keys)}
    )
    missing_rows = raw_with_cmp_keys.join(missing_cmp_keys, on=cmp_keys, how="inner")
    return missing_rows.drop(cmp_keys)
