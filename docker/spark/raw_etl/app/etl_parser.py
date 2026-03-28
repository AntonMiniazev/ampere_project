"""Shared CLI and group-config parsing for Spark ETL entrypoints."""

from __future__ import annotations

import argparse
import json
from typing import Optional

from etl_utils import parse_date, parse_table_list

RAW_APP_NAME = "source-to-raw-etl"
BRONZE_APP_NAME = "raw-to-bronze-etl"


def parse_raw_args() -> argparse.Namespace:
    """Parse CLI args for source-to-raw extraction.

    Available args:
    - --table: Single source table name used when `--tables` is omitted. Default `""`.
    - --tables: Comma-separated source table list for group extraction. Default `""`.
    - --groups-config: JSON list with group settings for multi-group execution. Default `""`.
    - --table-config: JSON mapping with table-specific overrides. Default `""`.
    - --schema: Source PostgreSQL schema name. Default `source`.
    - --run-date: Business date in `YYYY-MM-DD` format. Default `None`.
    - --mode: Extraction mode for fallback single-group runs. Default `snapshot`.
    - --partition-key: Raw landing partition key for fallback single-group runs. Default `snapshot_date`.
    - --snapshot-partitioned: Whether snapshot output is partitioned by `snapshot_date`. Default `true`.
    - --event-date-column: Date or timestamp column used for event/fact extraction. Default `""`.
    - --watermark-column: Update timestamp column used for mutable-dimension extraction. Default `""`.
    - --watermark-from: Lower ISO-8601 watermark boundary for mutable-dimension extraction. Default `""`.
    - --watermark-to: Upper ISO-8601 watermark boundary for mutable-dimension extraction. Default `""`.
    - --lookback-days: Lookback window in days for event extraction. Default `0`.
    - --shuffle-partitions: Override for `spark.sql.shuffle.partitions`; `0` keeps Spark default. Default `0`.
    - --run-id: Explicit run identifier written into raw metadata and paths. Default `""`.
    - --source-system: Source-system identifier written into manifests and paths. Default `postgres-pre-raw`.
    - --bucket: Target raw landing bucket name. Default `ampere-raw`.
    - --output-prefix: Prefix inside the raw bucket. Default `postgres-pre-raw`.
    - --app-name: Spark application name. Default `source-to-raw-etl`.
    - --image: Container image reference written into the manifest producer block. Default `""`.
    """
    parser = argparse.ArgumentParser(
        description="Extract source tables to MinIO-backed parquet."
    )
    parser.add_argument("--table", default="", help="Source table name")
    parser.add_argument(
        "--tables",
        default="",
        help="Comma-separated table list for group extraction",
    )
    parser.add_argument(
        "--groups-config",
        default="",
        help="JSON list with group settings for multi-group execution",
    )
    parser.add_argument(
        "--table-config",
        default="",
        help="JSON mapping of table-specific overrides",
    )
    parser.add_argument("--schema", default="source", help="Source schema name")
    parser.add_argument("--run-date", type=parse_date, help="YYYY-MM-DD")
    parser.add_argument(
        "--mode",
        choices=("snapshot", "incremental"),
        default="snapshot",
        help="Extraction mode",
    )
    parser.add_argument(
        "--partition-key",
        choices=("snapshot_date", "extract_date", "event_date"),
        default="snapshot_date",
        help="Partition key for raw landing layout",
    )
    parser.add_argument(
        "--snapshot-partitioned",
        default="true",
        help="Whether snapshot output is partitioned by snapshot_date",
    )
    parser.add_argument(
        "--event-date-column",
        default="",
        help="Date/timestamp column for event/fact extractions",
    )
    parser.add_argument(
        "--watermark-column",
        default="",
        help="Updated-at column for mutable dimensions",
    )
    parser.add_argument(
        "--watermark-from",
        default="",
        help="Lower watermark boundary (ISO-8601 timestamp, exclusive)",
    )
    parser.add_argument(
        "--watermark-to",
        default="",
        help="Upper watermark boundary (ISO-8601 timestamp, inclusive)",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=0,
        help="Lookback window for event extraction (days)",
    )
    parser.add_argument(
        "--shuffle-partitions",
        type=int,
        default=0,
        help="Override spark.sql.shuffle.partitions (0 keeps Spark default)",
    )
    parser.add_argument("--run-id", default="", help="Unique run identifier")
    parser.add_argument(
        "--source-system",
        default="postgres-pre-raw",
        help="Source system identifier",
    )
    parser.add_argument("--bucket", default="ampere-raw", help="MinIO bucket name")
    parser.add_argument(
        "--output-prefix",
        default="postgres-pre-raw",
        help="Output prefix in the bucket",
    )
    parser.add_argument("--app-name", default=RAW_APP_NAME, help="Spark app name")
    parser.add_argument("--image", default="", help="Container image reference")
    return parser.parse_args()


def parse_bronze_args() -> argparse.Namespace:
    """Parse CLI args for raw-to-bronze processing.

    Available args:
    - --tables: Comma-separated source table list for group execution. Default `""`.
    - --table-config: JSON mapping with table-specific bronze overrides. Default `""`.
    - --groups-config: JSON list with group settings for multi-group execution. Default `""`.
    - --schema: Source schema name used in raw paths and bronze metadata. Default `source`.
    - --run-date: Business date in `YYYY-MM-DD` format. Default `None`.
    - --mode: Storage mode for fallback single-group runs. Default `snapshot`.
    - --partition-key: Raw landing partition key for fallback single-group runs. Default `snapshot_date`.
    - --event-date-column: Date or timestamp column used for fallback fact/event runs. Default `""`.
    - --lookback-days: Lookback window in days for fallback fact/event runs. Default `0`.
    - --raw-bucket: Raw landing bucket name. Default `ampere-raw`.
    - --raw-prefix: Prefix inside the raw landing bucket. Default `postgres-pre-raw`.
    - --source-system: Source-system identifier used in manifests and registry rows. Default `postgres-pre-raw`.
    - --shuffle-partitions: Override for `spark.sql.shuffle.partitions`; `0` keeps Spark default. Default `0`.
    - --uc-enabled: Enable the required Unity Catalog mode; must stay `true`. Default `true`.
    - --uc-catalog: Unity Catalog catalog name. Default `""`.
    - --uc-bronze-schema: Unity Catalog schema for bronze tables. Default `bronze`.
    - --uc-ops-schema: Unity Catalog schema for operational tables. Default `ops`.
    - --app-name: Spark application name. Default `raw-to-bronze-etl`.
    - --image: Container image reference for logging/metadata. Default `""`.
    """
    parser = argparse.ArgumentParser(
        description="Transform raw landing data to bronze Delta tables."
    )
    parser.add_argument(
        "--tables",
        default="",
        help="Comma-separated table list for group extraction",
    )
    parser.add_argument(
        "--table-config",
        default="",
        help="JSON mapping of table-specific overrides",
    )
    parser.add_argument(
        "--groups-config",
        default="",
        help="JSON list with group settings for multi-group execution",
    )
    parser.add_argument("--schema", default="source", help="Source schema name")
    parser.add_argument("--run-date", type=parse_date, help="YYYY-MM-DD")
    parser.add_argument(
        "--mode",
        choices=("snapshot", "incremental"),
        default="snapshot",
        help="Storage mode for bronze tables",
    )
    parser.add_argument(
        "--partition-key",
        choices=("snapshot_date", "extract_date", "event_date"),
        default="snapshot_date",
        help="Partition key for raw landing layout",
    )
    parser.add_argument(
        "--event-date-column",
        default="",
        help="Date/timestamp column for event/fact extractions",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=0,
        help="Lookback window for event extraction (days)",
    )
    parser.add_argument(
        "--raw-bucket",
        default="ampere-raw",
        help="Raw landing bucket",
    )
    parser.add_argument(
        "--raw-prefix",
        default="postgres-pre-raw",
        help="Raw landing prefix",
    )
    parser.add_argument(
        "--source-system",
        default="postgres-pre-raw",
        help="Source system identifier",
    )
    parser.add_argument(
        "--shuffle-partitions",
        type=int,
        default=0,
        help="Override spark.sql.shuffle.partitions (0 keeps Spark default)",
    )
    parser.add_argument(
        "--uc-enabled",
        default="true",
        help="Enable the required Unity Catalog mode for bronze tables (true/false).",
    )
    parser.add_argument(
        "--uc-catalog",
        default="",
        help="Unity Catalog catalog name, e.g. ampere.",
    )
    parser.add_argument(
        "--uc-bronze-schema",
        default="bronze",
        help="Unity Catalog schema for bronze tables.",
    )
    parser.add_argument(
        "--uc-ops-schema",
        default="ops",
        help="Unity Catalog schema for operational tables.",
    )
    parser.add_argument("--app-name", default=BRONZE_APP_NAME, help="Spark app name")
    parser.add_argument("--image", default="", help="Container image reference")
    return parser.parse_args()


def parse_raw_groups_config(
    raw: str,
    default_shuffle_partitions: Optional[int],
) -> list[dict]:
    """Parse raw layer group config JSON into normalized group dictionaries."""
    if not raw:
        return []
    data = json.loads(raw)
    if not isinstance(data, list):
        raise ValueError("groups-config must be a JSON list")
    groups = []
    for item in data:
        if not isinstance(item, dict):
            raise ValueError("groups-config entries must be objects")
        tables = item.get("tables", [])
        if isinstance(tables, str):
            tables = parse_table_list(tables)
        shuffle_partitions = item.get("shuffle_partitions", default_shuffle_partitions)
        if shuffle_partitions in ("", None):
            shuffle_partitions = None
        else:
            shuffle_partitions = int(shuffle_partitions)
            if shuffle_partitions <= 0:
                shuffle_partitions = None
        groups.append(
            {
                "group": item.get("group", "group"),
                "mode": item.get("mode", "snapshot"),
                "partition_key": item.get("partition_key", "snapshot_date"),
                "event_date_column": item.get("event_date_column", ""),
                "watermark_column": item.get("watermark_column", ""),
                "lookback_days": int(item.get("lookback_days", 0) or 0),
                "tables": tables,
                "table_config": item.get("table_config", {}) or {},
                "snapshot_partitioned": item.get("snapshot_partitioned", "true"),
                "shuffle_partitions": shuffle_partitions,
            }
        )
    return groups


def parse_bronze_groups_config(
    raw: str,
    default_shuffle_partitions: Optional[int],
) -> list[dict]:
    """Parse bronze layer group config JSON into normalized group dictionaries."""
    if not raw:
        return []
    data = json.loads(raw)
    if not isinstance(data, list):
        raise ValueError("groups-config must be a JSON list")
    groups = []
    for item in data:
        if not isinstance(item, dict):
            raise ValueError("groups-config entries must be objects")
        tables = item.get("tables", [])
        if isinstance(tables, str):
            tables = parse_table_list(tables)
        shuffle_partitions = item.get("shuffle_partitions", default_shuffle_partitions)
        if shuffle_partitions in ("", None):
            shuffle_partitions = None
        else:
            shuffle_partitions = int(shuffle_partitions)
            if shuffle_partitions <= 0:
                shuffle_partitions = None
        max_partition_bytes = item.get("files_max_partition_bytes")
        if max_partition_bytes in ("", None):
            max_partition_bytes = None
        else:
            max_partition_bytes = str(max_partition_bytes)
        open_cost_bytes = item.get("files_open_cost_bytes")
        if open_cost_bytes in ("", None):
            open_cost_bytes = None
        else:
            open_cost_bytes = str(open_cost_bytes)
        adaptive_coalesce = item.get("adaptive_coalesce")
        if isinstance(adaptive_coalesce, str):
            adaptive_coalesce = adaptive_coalesce.strip().lower()
            if adaptive_coalesce in ("true", "1", "yes"):
                adaptive_coalesce = True
            elif adaptive_coalesce in ("false", "0", "no"):
                adaptive_coalesce = False
            else:
                adaptive_coalesce = None
        groups.append(
            {
                "group": item.get("group", "group"),
                "mode": item.get("mode", "snapshot"),
                "partition_key": item.get("partition_key", "snapshot_date"),
                "event_date_column": item.get("event_date_column", ""),
                "lookback_days": int(item.get("lookback_days", 0) or 0),
                "tables": tables,
                "table_config": item.get("table_config", {}) or {},
                "shuffle_partitions": shuffle_partitions,
                "files_max_partition_bytes": max_partition_bytes,
                "files_open_cost_bytes": open_cost_bytes,
                "adaptive_coalesce": adaptive_coalesce,
            }
        )
    return groups


def resolve_raw_groups(
    args,
    *,
    event_date_column: str | None,
    watermark_column: str | None,
    lookback_days: int,
) -> tuple[list[dict], bool]:
    """Build the normalized raw group list for the current extraction run."""
    default_shuffle_partitions = (
        args.shuffle_partitions if args.shuffle_partitions > 0 else None
    )
    groups_config = parse_raw_groups_config(args.groups_config, default_shuffle_partitions)
    if groups_config:
        return groups_config, True

    table_list = parse_table_list(args.tables)
    if not table_list and args.table:
        table_list = [args.table]
    if not table_list:
        raise ValueError("No tables provided. Use --table or --tables.")

    table_config = json.loads(args.table_config) if args.table_config else {}
    return (
        [
            {
                "group": "default",
                "mode": args.mode,
                "partition_key": args.partition_key,
                "event_date_column": event_date_column or "",
                "watermark_column": watermark_column or "",
                "lookback_days": lookback_days,
                "tables": table_list,
                "table_config": table_config,
                "snapshot_partitioned": args.snapshot_partitioned,
                "shuffle_partitions": default_shuffle_partitions,
            }
        ],
        False,
    )


def resolve_bronze_groups(args) -> tuple[list[dict], list[str] | None, bool]:
    """Build the normalized bronze group list for the current apply run."""
    default_shuffle_partitions = (
        args.shuffle_partitions if args.shuffle_partitions > 0 else None
    )
    groups_config = parse_bronze_groups_config(
        args.groups_config, default_shuffle_partitions
    )
    if groups_config:
        return groups_config, None, True

    default_tables = parse_table_list(args.tables)
    if not default_tables:
        raise ValueError("No tables provided. Use --tables or --groups-config.")
    table_config = json.loads(args.table_config) if args.table_config else {}
    return (
        [
            {
                "group": "default",
                "mode": args.mode,
                "partition_key": args.partition_key,
                "event_date_column": args.event_date_column,
                "lookback_days": args.lookback_days,
                "tables": default_tables,
                "table_config": table_config,
                "shuffle_partitions": default_shuffle_partitions,
            }
        ],
        default_tables,
        False,
    )
