"""Helpers for applying raw landing batches to Bronze Delta tables.

These utilities keep registry and Delta helpers centralized so the per-table
processors can stay focused on their data flow.
"""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


def load_registry_schema(schema_path: str) -> StructType:
    """Load the registry schema JSON template into a Spark StructType.

    Args:
        schema_path: JSON schema path, e.g. "/opt/spark/app/bronze_apply_registry_schema.json".

    Examples:
        load_registry_schema("/opt/spark/app/bronze_apply_registry_schema.json")
    """
    data = json.loads(Path(schema_path).read_text())
    type_map = {
        "string": StringType(),
        "int": IntegerType(),
    }
    fields = []
    for field in data.get("fields", []):
        field_type = type_map.get(field.get("type"))
        if field_type is None:
            raise ValueError(
                f"Unsupported registry field type: {field.get('type')}"
            )
        fields.append(
            StructField(
                field.get("name"),
                field_type,
                bool(field.get("nullable", True)),
            )
        )
    if not fields:
        raise ValueError("Registry schema template has no fields.")
    return StructType(fields)


def append_registry_row(
    spark: SparkSession, path_str: str, schema: StructType, row: dict
) -> None:
    """Append a single apply-registry row with retry on concurrent commits.

    Args:
        spark: Active SparkSession, e.g. SparkSession.builder.getOrCreate().
        path_str: Registry table path, e.g. "s3a://ampere-bronze/bronze/ops/bronze_apply_registry".
        schema: Registry schema, e.g. StructType([...]).
        row: Row data, e.g. {"source_table": "orders", "status": "applied"}.

    Examples:
        append_registry_row(spark, registry_path, registry_schema, {"status": "applied"})
    """
    logger = logging.getLogger("raw-to-bronze-apply-utils")
    df = spark.createDataFrame([row], schema=schema)
    retries = 5
    last_exc = None
    retry_tokens = (
        "ProtocolChangedException",
        "ConcurrentAppendException",
        "ConcurrentWriteException",
        "CommitFailedException",
    )
    for attempt in range(retries):
        try:
            df.write.format("delta").mode("append").save(path_str)
            return
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if any(token in str(exc) for token in retry_tokens):
                logger.warning(
                    "Registry append conflict; retrying (%s/%s).",
                    attempt + 1,
                    retries,
                )
                time.sleep(1 + attempt)
                continue
            raise
    if last_exc:
        raise last_exc


def build_registry_payload(
    manifest: dict,
    batch: dict,
    source_system: str,
    source_schema: str,
    source_table: str,
    apply_ts: str,
    status: str,
    details: str,
    watermark_from: str | None = None,
    watermark_to: str | None = None,
) -> dict:
    """Build a standard registry row mapping for a single batch.

    Args:
        manifest: Manifest JSON dict, e.g. {"run_id": "...", "schema_hash": "..."}.
        batch: Batch context with manifest_path/partition info, e.g. {"partition_value": "2026-01-24"}.
        source_system: Source system id, e.g. "postgres-pre-raw".
        source_schema: Source schema name, e.g. "source".
        source_table: Source table name, e.g. "orders".
        apply_ts: Apply timestamp in UTC ISO-8601, e.g. "2026-01-24T12:00:00+00:00".
        status: Row status, e.g. "applied", "skipped", "failed".
        details: Short reason string, e.g. "ok" or "schema_hash mismatch".
        watermark_from: Optional watermark start, e.g. "2026-01-23T00:00:00Z".
        watermark_to: Optional watermark end, e.g. "2026-01-24T00:00:00Z".

    Examples:
        build_registry_payload(manifest, batch, "postgres-pre-raw", "source", "orders",
                              apply_ts="2026-01-24T12:00:00+00:00", status="applied", details="ok")
    """
    return {
        "source_system": manifest.get("source_system", source_system),
        "source_schema": manifest.get("source_schema", source_schema),
        "source_table": manifest.get("source_table", source_table),
        "run_id": manifest.get("run_id", batch.get("run_id")),
        "manifest_path": batch["manifest_path"],
        "batch_type": manifest.get("batch_type"),
        "partition_kind": batch.get("partition_kind"),
        "partition_value": batch.get("partition_value"),
        "ingest_ts_utc": manifest.get("ingest_ts_utc"),
        "schema_hash": manifest.get("schema_hash"),
        "contract_version": manifest.get("contract_version"),
        "apply_ts_utc": apply_ts,
        "status": status,
        "details": details,
        "watermark_from": watermark_from,
        "watermark_to": watermark_to,
        "lookback_days": manifest.get("lookback_days"),
        "window_from": (manifest.get("window") or {}).get("from"),
        "window_to": (manifest.get("window") or {}).get("to"),
        "row_count": manifest.get("row_count"),
        "file_count": manifest.get("file_count"),
    }


def merge_to_delta(
    spark: SparkSession, df, target_path: str, merge_keys: list[str]
) -> None:
    """Merge or overwrite a Delta table using stable business keys.

    Args:
        spark: Active SparkSession, e.g. SparkSession.builder.getOrCreate().
        df: Source DataFrame, e.g. spark.read.parquet("s3a://...").
        target_path: Delta table path, e.g. "s3a://ampere-bronze/bronze/source/orders".
        merge_keys: Business keys, e.g. ["order_id"].

    Examples:
        merge_to_delta(spark, df, "s3a://ampere-bronze/bronze/source/orders", ["order_id"])
    """
    try:
        from delta.tables import DeltaTable
    except ImportError as exc:
        raise ImportError(
            "delta-spark is required for bronze writes. Ensure Delta jars are on the classpath."
        ) from exc

    if DeltaTable.isDeltaTable(spark, target_path):
        delta_table = DeltaTable.forPath(spark, target_path)
        conditions = " AND ".join([f"t.{k} = s.{k}" for k in merge_keys])
        (
            delta_table.alias("t")
            .merge(df.alias("s"), conditions)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        df.write.format("delta").mode("overwrite").save(target_path)

