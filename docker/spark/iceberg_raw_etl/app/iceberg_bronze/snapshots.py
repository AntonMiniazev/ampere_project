"""Snapshot batch writer for Bronze Iceberg tables."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Callable

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType

from etl_utils import manifest_ok, parse_optional_datetime
from iceberg_bronze.apply_utils import build_registry_payload


def apply_snapshot_batches(
    spark: SparkSession,
    table: str,
    bronze_table_name: str,
    registry_schema: StructType,
    registry_rows: list[dict],
    source_system: str,
    source_schema: str,
    sorted_batches: list[dict],
    expected_schema_hash: str | None,
    expected_contract_version: str | None,
    logger: logging.Logger,
    align_to_target_schema: Callable | None = None,
) -> None:
    """Apply snapshot batches sequentially into a partitioned Iceberg table.

    The newest batch for each snapshot_date overwrites that target partition,
    preserving point-in-time history while making retries idempotent.

    Args:
        spark: Active SparkSession, e.g. SparkSession.builder.getOrCreate().
        table: Source table name, e.g. "orders".
        bronze_table_name: Iceberg Bronze table name, e.g. "`iceberg_bronze`.`bronze`.`orders`".
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
            bronze_table_name="`ampere`.`bronze`.`orders`",
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
    if not sorted_batches:
        logger.info("No snapshot batches to apply for %s", table)
        return

    partition_values = [
        batch.get("partition_value")
        for batch in sorted_batches
        if batch.get("partition_value")
    ]
    if not partition_values:
        logger.warning("Missing snapshot partition values for %s", table)
        return

    def _latest_batch_key(batch: dict) -> tuple:
        """Build an ordering key so the newest snapshot batch wins deterministically.

        Snapshot groups may contain multiple raw runs for the same business date
        when reruns happen. Comparing ingest timestamp first and run id second
        gives Bronze a stable rule for deciding which batch overwrites the
        snapshot partition and which ones are marked as superseded.
        """
        ingest_dt = parse_optional_datetime(batch.get("ingest_ts_utc") or "")
        if ingest_dt and ingest_dt.tzinfo is None:
            ingest_dt = ingest_dt.replace(tzinfo=timezone.utc)
        ingest_key = ingest_dt or datetime(1, 1, 1, tzinfo=timezone.utc)
        return (ingest_key, batch.get("run_id", ""))

    latest_by_partition: dict[str, dict] = {}
    superseded_batches = []
    for batch in sorted_batches:
        partition_value = batch.get("partition_value")
        if not partition_value:
            continue
        current = latest_by_partition.get(partition_value)
        if current is None:
            latest_by_partition[partition_value] = batch
        elif _latest_batch_key(batch) > _latest_batch_key(current):
            superseded_batches.append(current)
            latest_by_partition[partition_value] = batch
        else:
            superseded_batches.append(batch)

    latest_batches = [latest_by_partition[value] for value in sorted(latest_by_partition)]
    if not latest_batches:
        logger.warning("No snapshot batch has a partition value for %s", table)
        return

    for batch in superseded_batches:
        manifest = batch["manifest"]
        batch_apply_ts = datetime.now(timezone.utc).isoformat()
        registry_rows.append(
            build_registry_payload(
                manifest,
                batch,
                source_system,
                source_schema,
                table,
                batch_apply_ts,
                "skipped",
                "superseded by latest snapshot",
            )
        )
        logger.info(
            "Skipping superseded snapshot run_id=%s %s=%s for %s",
            manifest.get("run_id", batch.get("run_id")),
            batch.get("partition_kind"),
            batch.get("partition_value"),
            table,
        )

    for batch in latest_batches:
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
        # The expected outcome is a DataFrame ready for snapshot replacement.
        try:
            df = spark.read.parquet(*file_paths)
            df = df.withColumn("_bronze_last_run_id", F.lit(manifest.get("run_id")))
            df = df.withColumn("_bronze_last_apply_ts", F.lit(batch_apply_ts))
            df = df.withColumn(
                "_bronze_last_manifest_path", F.lit(batch["manifest_path"])
            )

            df = df.withColumn("snapshot_date", F.lit(partition_value))
            if align_to_target_schema is not None:
                df = align_to_target_schema(table, df)
            # Commit the replacement as one Iceberg snapshot so a failed task
            # cannot leave this date empty between a delete and an append.
            df.writeTo(bronze_table_name).overwritePartitions()

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
