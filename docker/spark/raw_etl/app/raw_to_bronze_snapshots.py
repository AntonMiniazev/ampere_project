"""Snapshot batch writer for Bronze Delta tables."""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType

from etl_utils import manifest_ok
from raw_to_bronze_apply_utils import build_registry_payload


def apply_snapshot_batches(
    spark: SparkSession,
    table: str,
    bronze_path: str,
    registry_path: str,
    registry_schema: StructType,
    registry_rows: list[dict],
    source_system: str,
    source_schema: str,
    sorted_batches: list[dict],
    expected_schema_hash: str | None,
    expected_contract_version: str | None,
    logger: logging.Logger,
) -> None:
    """Apply snapshot batches sequentially into a partitioned Delta table.

    Each batch overwrites the target snapshot_date partition to keep a stable
    point-in-time snapshot history while maintaining idempotency on retries.

    Args:
        spark: Active SparkSession, e.g. SparkSession.builder.getOrCreate().
        table: Source table name, e.g. "orders".
        bronze_path: Delta target path, e.g. "s3a://ampere-bronze/bronze/source/orders".
        registry_path: Registry Delta path, e.g. "s3a://ampere-bronze/bronze/ops/bronze_apply_registry".
        registry_schema: Registry schema StructType, e.g. StructType([...]).
        registry_rows: Output list to collect registry rows for a single write.
        source_system: Source system id, e.g. "postgres-pre-raw".
        source_schema: Source schema name, e.g. "source".
        sorted_batches: Ordered batch list with manifest metadata.
        expected_schema_hash: Schema hash to enforce, e.g. "abc123" or None.
        expected_contract_version: Contract version to enforce, e.g. "v2" or None.
        logger: Logger for run output, e.g. logging.getLogger("raw-to-bronze-etl").

    Examples:
        apply_snapshot_batches(
            spark=spark,
            table="orders",
            bronze_path="s3a://ampere-bronze/bronze/source/orders",
            registry_path="s3a://ampere-bronze/bronze/ops/bronze_apply_registry",
            registry_schema=registry_schema,
            registry_rows=[],
            source_system="postgres-pre-raw",
            source_schema="source",
            sorted_batches=sorted_queue,
            expected_schema_hash=None,
            expected_contract_version=None,
            logger=logging.getLogger("raw-to-bronze-etl"),
        )
    """
    try:
        from delta.tables import DeltaTable
    except ImportError as exc:
        raise ImportError(
            "delta-spark is required for bronze writes. Ensure Delta jars are on the classpath."
        ) from exc

    for batch in sorted_batches:
        # Step A: Validate the manifest and decide whether to apply or skip.
        # This ensures only complete, compatible batches are written.
        # The expected outcome is either a write attempt or a registry skip row.
        manifest = batch["manifest"]
        batch_apply_ts = datetime.now(timezone.utc).isoformat()
        partition_kind = batch.get("partition_kind")
        partition_value = batch.get("partition_value")

        ok, reason = manifest_ok(manifest)
        if not ok:
            logger.warning(
                "Manifest validation failed for %s run_id=%s %s=%s reason=%s",
                table,
                manifest.get("run_id", batch.get("run_id")),
                partition_kind,
                partition_value,
                reason,
            )
            registry_rows.append(
                build_registry_payload(
                    manifest,
                    batch,
                    source_system,
                    source_schema,
                    table,
                    batch_apply_ts,
                    "failed",
                    reason,
                )
            )
            continue

        if manifest.get("row_count", 0) == 0 or manifest.get("file_count", 0) == 0:
            logger.info(
                "Skipping empty batch for %s run_id=%s %s=%s",
                table,
                manifest.get("run_id", batch.get("run_id")),
                partition_kind,
                partition_value,
            )
            watermark_from = None
            watermark_to = None
            if manifest.get("watermark"):
                watermark_from = manifest["watermark"].get("from")
                watermark_to = manifest["watermark"].get("to")
            registry_rows.append(
                build_registry_payload(
                    manifest,
                    batch,
                    source_system,
                    source_schema,
                    table,
                    batch_apply_ts,
                    "skipped",
                    "empty batch",
                    watermark_from,
                    watermark_to,
                )
            )
            continue

        if expected_schema_hash and manifest.get("schema_hash") != expected_schema_hash:
            logger.warning(
                "Schema hash mismatch for %s run_id=%s expected=%s actual=%s",
                table,
                manifest.get("run_id"),
                expected_schema_hash,
                manifest.get("schema_hash"),
            )
            registry_rows.append(
                build_registry_payload(
                    manifest,
                    batch,
                    source_system,
                    source_schema,
                    table,
                    batch_apply_ts,
                    "skipped",
                    "schema_hash mismatch",
                )
            )
            continue

        if (
            expected_contract_version
            and manifest.get("contract_version") != expected_contract_version
        ):
            logger.warning(
                "Contract version mismatch for %s run_id=%s expected=%s actual=%s",
                table,
                manifest.get("run_id"),
                expected_contract_version,
                manifest.get("contract_version"),
            )
            registry_rows.append(
                build_registry_payload(
                    manifest,
                    batch,
                    source_system,
                    source_schema,
                    table,
                    batch_apply_ts,
                    "skipped",
                    "contract_version mismatch",
                )
            )
            continue

        if not partition_kind or not partition_value:
            logger.warning(
                "Missing partition info for %s run_id=%s",
                table,
                manifest.get("run_id"),
            )
            registry_rows.append(
                build_registry_payload(
                    manifest,
                    batch,
                    source_system,
                    source_schema,
                    table,
                    batch_apply_ts,
                    "failed",
                    "missing partition info",
                )
            )
            continue

        file_paths = [
            f["path"] for f in manifest.get("files", []) if f.get("path")
        ]
        if not file_paths:
            logger.warning(
                "No file paths in manifest for %s run_id=%s",
                table,
                manifest.get("run_id"),
            )
            registry_rows.append(
                build_registry_payload(
                    manifest,
                    batch,
                    source_system,
                    source_schema,
                    table,
                    batch_apply_ts,
                    "failed",
                    "no files in manifest",
                )
            )
            continue

        # Step B: Read the batch data and add lineage fields.
        # This keeps the "last applied batch" columns aligned to the current run.
        # The expected outcome is a DataFrame ready for snapshot overwrite.
        try:
            df = spark.read.parquet(*file_paths)
            df = df.withColumn("_bronze_last_run_id", F.lit(manifest.get("run_id")))
            df = df.withColumn("_bronze_last_apply_ts", F.lit(batch_apply_ts))
            df = df.withColumn(
                "_bronze_last_manifest_path", F.lit(batch["manifest_path"])
            )

            df = df.withColumn("snapshot_date", F.lit(partition_value))
            if DeltaTable.isDeltaTable(spark, bronze_path):
                (
                    df.write.format("delta")
                    .mode("overwrite")
                    .option(
                        "replaceWhere",
                        f"snapshot_date = '{partition_value}'",
                    )
                    .save(bronze_path)
                )
            else:
                (
                    df.write.format("delta")
                    .mode("overwrite")
                    .partitionBy("snapshot_date")
                    .save(bronze_path)
                )

            # Step C: Record the applied batch in the registry.
            # This keeps idempotency and traceability for future runs.
            # The expected outcome is one applied registry row per batch.
            watermark_from = None
            watermark_to = None
            if manifest.get("watermark"):
                watermark_from = manifest["watermark"].get("from")
                watermark_to = manifest["watermark"].get("to")

            registry_rows.append(
                build_registry_payload(
                    manifest,
                    batch,
                    source_system,
                    source_schema,
                    table,
                    batch_apply_ts,
                    "applied",
                    "ok",
                    watermark_from,
                    watermark_to,
                )
            )
            logger.info(
                "Applied batch run_id=%s %s=%s for %s manifest=%s",
                manifest.get("run_id"),
                partition_kind,
                partition_value,
                table,
                batch["manifest_path"],
            )
        except Exception as exc:  # noqa: BLE001
            registry_rows.append(
                build_registry_payload(
                    manifest,
                    batch,
                    source_system,
                    source_schema,
                    table,
                    batch_apply_ts,
                    "failed",
                    f"bronze apply failed: {exc}",
                )
            )
            logger.exception(
                "Failed applying batch run_id=%s %s=%s for %s",
                manifest.get("run_id"),
                partition_kind,
                partition_value,
                table,
            )
