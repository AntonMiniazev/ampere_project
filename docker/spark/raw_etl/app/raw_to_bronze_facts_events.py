"""Facts/events batch writer for Bronze Delta tables."""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType

from etl_utils import manifest_ok
from raw_to_bronze_apply_utils import build_registry_payload, merge_to_delta


def apply_facts_events_batches(
    spark: SparkSession,
    table: str,
    bronze_path: str,
    merge_keys: list[str],
    registry_path: str,
    registry_schema: StructType,
    registry_rows: list[dict],
    source_system: str,
    source_schema: str,
    sorted_batches: list[dict],
    lookback_days: int,
    append_only_override: bool | None,
    expected_schema_hash: str | None,
    expected_contract_version: str | None,
    logger: logging.Logger,
) -> None:
    """Apply fact/event batches grouped by event_date.

    All eligible batches are combined into one write per table to reduce
    per-batch scheduling and memory pressure on small files.

    Args:
        spark: Active SparkSession, e.g. SparkSession.builder.getOrCreate().
        table: Source table name, e.g. "orders".
        bronze_path: Delta target path, e.g. "s3a://ampere-bronze/bronze/source/orders".
        merge_keys: Business keys, e.g. ["order_id"] or [] for pure append.
        registry_path: Registry Delta path, e.g. "s3a://ampere-bronze/bronze/ops/bronze_apply_registry".
        registry_schema: Registry schema StructType, e.g. StructType([...]).
        registry_rows: Output list to collect registry rows for a single write.
        source_system: Source system id, e.g. "postgres-pre-raw".
        source_schema: Source schema name, e.g. "source".
        sorted_batches: Ordered batch list with manifest metadata.
        lookback_days: Lookback window for events, e.g. 0 for append-only facts.
        append_only_override: Optional flag to force append-only writes.
        expected_schema_hash: Schema hash to enforce, e.g. "abc123" or None.
        expected_contract_version: Contract version to enforce, e.g. "v2" or None.
        logger: Logger for run output, e.g. logging.getLogger("raw-to-bronze-etl").

    Examples:
        apply_facts_events_batches(
            spark=spark,
            table="orders",
            bronze_path="s3a://ampere-bronze/bronze/source/orders",
            merge_keys=["order_id"],
            registry_path="s3a://ampere-bronze/bronze/ops/bronze_apply_registry",
            registry_schema=registry_schema,
            registry_rows=[],
            source_system="postgres-pre-raw",
            source_schema="source",
            sorted_batches=sorted_queue,
            lookback_days=0,
            append_only_override=None,
            expected_schema_hash=None,
            expected_contract_version=None,
            logger=logging.getLogger("raw-to-bronze-etl"),
        )
    """
    # Avoid local-checkpoint materialization for Delta MERGE to reduce
    # checkpoint block loss when executors churn under tight memory.
    spark.conf.set("spark.databricks.delta.merge.materializeSource", "none")
    logger.info(
        "Set spark.databricks.delta.merge.materializeSource=false for facts/events."
    )
    append_only = append_only_override
    if append_only is None:
        append_only = lookback_days <= 0
    if append_only:
        logger.info(
            "Using append-only writes for %s (lookback_days=%s).",
            table,
            lookback_days,
        )

    valid_batches = []
    for batch in sorted_batches:
        # Step A: Validate each manifest and build a per-batch apply plan.
        # This keeps registry rows accurate even when a batch is skipped.
        # The expected outcome is a list of validated batches for this table.
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

        file_paths = [f["path"] for f in manifest.get("files", []) if f.get("path")]
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

        valid_batches.append(
            {
                "batch": batch,
                "manifest": manifest,
                "apply_ts": batch_apply_ts,
                "file_paths": file_paths,
            }
        )

    if not valid_batches:
        return

    partition_kinds = {
        info["batch"].get("partition_kind") for info in valid_batches
    }
    partition_kind = partition_kinds.pop() if len(partition_kinds) == 1 else "event_date"
    if partition_kinds:
        logger.warning(
            "Mixed partition kinds for %s; defaulting to %s",
            table,
            partition_kind,
        )

    file_meta_rows = []
    all_paths = []
    for info in valid_batches:
        batch = info["batch"]
        manifest = info["manifest"]
        for path in info["file_paths"]:
            all_paths.append(path)
            file_meta_rows.append(
                {
                    "file_path": path,
                    "run_id": manifest.get("run_id"),
                    "apply_ts": info["apply_ts"],
                    "manifest_path": batch.get("manifest_path"),
                    "partition_value": batch.get("partition_value"),
                }
            )

    meta_df = spark.createDataFrame(file_meta_rows)
    do_merge = bool(merge_keys) and not append_only

    # Step B: Read all batch files once and write in a single job.
    # This reduces per-batch scheduling overhead while preserving lineage.
    # The expected outcome is one write per table per run.
    try:
        df = spark.read.parquet(*all_paths)
        df = df.withColumn("_input_file", F.input_file_name())
        df = df.join(
            meta_df,
            df["_input_file"] == meta_df["file_path"],
            "inner",
        )
        df = df.drop("_input_file", "file_path")
        df = df.withColumn("_bronze_last_run_id", F.col("run_id"))
        df = df.withColumn("_bronze_last_apply_ts", F.col("apply_ts"))
        df = df.withColumn(
            "_bronze_last_manifest_path", F.col("manifest_path")
        )
        df = df.withColumn(partition_kind, F.col("partition_value"))
        df = df.drop("run_id", "apply_ts", "manifest_path", "partition_value")

        if merge_keys:
            df = df.dropDuplicates(merge_keys)

        if do_merge:
            merge_to_delta(
                spark,
                df,
                bronze_path,
                merge_keys,
                partition_column=partition_kind,
                partition_value=None,
            )
        else:
            df.write.format("delta").mode("append").save(bronze_path)

        # Step C: Emit registry rows for every batch we included.
        # This keeps the registry granular while the write is consolidated.
        # The expected outcome is one applied row per run_id+partition.
        for info in valid_batches:
            manifest = info["manifest"]
            watermark_from = None
            watermark_to = None
            if manifest.get("watermark"):
                watermark_from = manifest["watermark"].get("from")
                watermark_to = manifest["watermark"].get("to")
            registry_rows.append(
                build_registry_payload(
                    manifest,
                    info["batch"],
                    source_system,
                    source_schema,
                    table,
                    info["apply_ts"],
                    "applied",
                    "ok",
                    watermark_from,
                    watermark_to,
                )
            )
            logger.info(
                "Applied batch run_id=%s %s=%s for %s manifest=%s",
                manifest.get("run_id"),
                info["batch"].get("partition_kind"),
                info["batch"].get("partition_value"),
                table,
                info["batch"]["manifest_path"],
            )
    except Exception as exc:  # noqa: BLE001
        for info in valid_batches:
            manifest = info["manifest"]
            registry_rows.append(
                build_registry_payload(
                    manifest,
                    info["batch"],
                    source_system,
                    source_schema,
                    table,
                    info["apply_ts"],
                    "failed",
                    f"bronze apply failed: {exc}",
                )
            )
        logger.exception("Failed applying batches for %s", table)
