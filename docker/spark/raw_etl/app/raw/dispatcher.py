"""Dispatch raw extraction planning and writing to group-specific modules."""

from __future__ import annotations

from etl_utils import parse_bool_flag
from raw.common import RawTablePlan
from raw.events import write_event_table, build_event_plan
from raw.facts import write_fact_table, build_fact_plan
from raw.mutable_dims import build_mutable_dim_plan, write_mutable_dim_table
from raw.snapshots import build_snapshot_plan, write_snapshot_table


def build_table_plan(
    *,
    spark,
    logger,
    group: dict,
    group_name: str,
    table: str,
    args,
    run_date,
    output_base: str,
):
    """Build a group-specific table extraction plan.

    This function resolves the generic DAG/group config into one normalized
    `RawTablePlan` for a single table. The returned plan tells the caller which
    group module owns the table semantics, whether the extract is bootstrap or
    incremental, which JDBC filter should be applied, and which landing path
    and metadata settings should be used during the write step.

    Args:
    - spark: Active SparkSession used for landing-state checks and path existence checks. No default.
    - logger: Logger used for bootstrap and initial-load diagnostic messages. No default.
    - group: Normalized raw group config dictionary. Expected keys include:
      - `group`: logical group name. Default in normalized config: `group`.
      - `mode`: extraction mode. Default in normalized config: `snapshot`.
      - `partition_key`: landing partition strategy. Default in normalized config: `snapshot_date`.
      - `event_date_column`: group-level event column override. Default in normalized config: `""`.
      - `watermark_column`: group-level watermark column override. Default in normalized config: `""`.
      - `lookback_days`: group-level lookback window. Default in normalized config: `0`.
      - `snapshot_partitioned`: snapshot partitioning flag. Default in normalized config: `true`.
      - `table_config`: per-table override mapping. Default in normalized config: `{}`.
    - group_name: Logical group name already resolved by the caller, such as `snapshots`, `mutable_dims`, `facts`, or `events`. No default.
    - table: Source table name to plan, such as `clients` or `orders`. No default.
    - args: Parsed raw CLI args namespace. Relevant defaults come from `parse_raw_args()`, for example:
      - `schema`: default `source`
      - `bucket`: default `ampere-raw`
      - `source_system`: default `postgres-pre-raw`
      - `watermark_from`: default `""`
      - `watermark_to`: default `""`
      - `run_date`: default `None`
    - run_date: Business date already normalized by the caller into `datetime.date`. No default.
    - output_base: Landing base path for the current table, for example `s3a://ampere-raw/postgres-pre-raw/source/orders`. No default.

    Returns:
    - RawTablePlan: normalized per-table extraction plan consumed by `write_table()`.

    Example:
    ```python
    plan = build_table_plan(
        spark=SparkSession.builder.appName("source-to-raw-etl").getOrCreate(),
        logger=logging.getLogger("source-to-raw-etl"),
        group={
            "group": "mutable_dims",
            "mode": "incremental",
            "partition_key": "extract_date",
            "event_date_column": "",
            "watermark_column": "updated_at",
            "lookback_days": 0,
            "tables": ["clients"],
            "table_config": {
                "clients": {
                    "watermark_column": "updated_at",
                    "created_column": "created_at",
                }
            },
            "snapshot_partitioned": "true",
            "shuffle_partitions": 1,
        },
        group_name="mutable_dims",
        table="clients",
        args=SimpleNamespace(
            schema="source",
            bucket="ampere-raw",
            source_system="postgres-pre-raw",
            watermark_from="",
            watermark_to="",
            run_date="2026-03-28",
        ),
        run_date=date(2026, 3, 28),
        output_base="s3a://ampere-raw/postgres-pre-raw/source/clients",
    )
    ```
    """
    table_config = group.get("table_config", {}) or {}
    table_meta = table_config.get(table, {})
    group_mode = group.get("mode", "snapshot")
    group_partition_key = group.get("partition_key", "snapshot_date")
    group_event_col = group.get("event_date_column") or None
    group_watermark_col = group.get("watermark_column") or None
    group_lookback_days = int(group.get("lookback_days", 0) or 0)
    snapshot_partitioned = parse_bool_flag(
        group.get("snapshot_partitioned", "true"),
        default=True,
    )

    if group_mode == "snapshot":
        return build_snapshot_plan(
            group_name=group_name,
            group_mode=group_mode,
            group_partition_key=group_partition_key,
            snapshot_partitioned=snapshot_partitioned,
            output_base=output_base,
            table=table,
            table_meta=table_meta,
            group_event_col=group_event_col,
            group_lookback_days=group_lookback_days,
            group_watermark_col=group_watermark_col,
            args=args,
        )

    if group_partition_key == "extract_date":
        return build_mutable_dim_plan(
            spark=spark,
            logger=logger,
            group_name=group_name,
            group_mode=group_mode,
            group_partition_key=group_partition_key,
            snapshot_partitioned=snapshot_partitioned,
            output_base=output_base,
            table=table,
            table_meta=table_meta,
            group_event_col=group_event_col,
            group_lookback_days=group_lookback_days,
            group_watermark_col=group_watermark_col,
            args=args,
            run_date=run_date,
        )

    if group_name == "facts":
        return build_fact_plan(
            spark=spark,
            logger=logger,
            group_name=group_name,
            group_mode=group_mode,
            group_partition_key=group_partition_key,
            snapshot_partitioned=snapshot_partitioned,
            output_base=output_base,
            table=table,
            table_meta=table_meta,
            group_event_col=group_event_col,
            group_lookback_days=group_lookback_days,
            group_watermark_col=group_watermark_col,
            args=args,
            run_date=run_date,
        )

    return build_event_plan(
        spark=spark,
        logger=logger,
        group_name=group_name,
        group_mode=group_mode,
        group_partition_key=group_partition_key,
        snapshot_partitioned=snapshot_partitioned,
        output_base=output_base,
        table=table,
        table_meta=table_meta,
        group_event_col=group_event_col,
        group_lookback_days=group_lookback_days,
        group_watermark_col=group_watermark_col,
        args=args,
        run_date=run_date,
    )


def write_table(
    *,
    spark,
    logger,
    plan: RawTablePlan,
    df,
    run_date,
    run_date_str: str,
    run_id: str,
    manifest_base: dict,
    base_columns: list[str],
    args,
    table: str,
    ingest_ts_utc: str,
) -> None:
    """Write a raw table using the module that owns the group semantics.

    This dispatcher takes a previously prepared `RawTablePlan` and routes the
    actual landing write to the module that owns the table behavior:
    snapshots, mutable dimensions, facts, or events. The caller does not need
    to branch on group semantics again after planning; it only passes the
    shared write-time inputs and this function delegates to the correct writer.

    Args:
    - spark: Active SparkSession used by the concrete writer for parquet, manifest, and state IO. No default.
    - logger: Logger used for write-phase diagnostic messages. No default.
    - plan: `RawTablePlan` returned by `build_table_plan()`. No default.
    - df: Source Spark DataFrame already read from JDBC and enriched with technical columns. No default.
    - run_date: Business date as `datetime.date`, used by fact/event writers when deriving partition windows. No default.
    - run_date_str: Business date string in `YYYY-MM-DD` format, used in landing paths and manifests. No default.
    - run_id: Ingestion run id written into landing paths and metadata. No default.
    - manifest_base: Shared manifest payload produced before the group-specific writer adds partition metadata. No default.
    - base_columns: Original source column list used for profiling and manifest statistics. No default.
    - args: Parsed raw CLI args namespace. Relevant values are read by downstream writers:
      - `source_system`: default `postgres-pre-raw`
      - `schema`: default `source`
    - table: Source table name, such as `clients` or `orders`. No default.
    - ingest_ts_utc: UTC ingest timestamp string written into mutable-dimension state. No default.

    Returns:
    - None: the function writes raw landing artifacts through the selected group writer.

    Example:
    ```python
    write_table(
        spark=SparkSession.builder.appName("source-to-raw-etl").getOrCreate(),
        logger=logging.getLogger("source-to-raw-etl"),
        plan=RawTablePlan(
            group_name="mutable_dims",
            group_mode="incremental",
            group_partition_key="extract_date",
            snapshot_partitioned=True,
            output_base="s3a://ampere-raw/postgres-pre-raw/source/clients",
            table_event_col=None,
            table_lookback_days=0,
            table_watermark_col="updated_at",
            table_created_col="created_at",
            table_cursor_granularity="date",
            state_path_value="s3a://ampere-raw/postgres-pre-raw/source/_state/_state_clients.json",
            table_watermark_from=datetime(2026, 3, 27, 0, 0, tzinfo=timezone.utc),
            table_created_from=datetime(2026, 3, 27, 0, 0, tzinfo=timezone.utc),
            table_watermark_to=datetime(2026, 3, 28, 0, 0, tzinfo=timezone.utc),
            bootstrap_full_extract=False,
            initial_event_load=False,
            where_clause=(
                "((updated_at IS NOT NULL AND updated_at > TIMESTAMPTZ "
                "'2026-03-27T00:00:00+00:00' AND updated_at <= TIMESTAMPTZ "
                "'2026-03-28T00:00:00+00:00') OR (updated_at IS NULL AND "
                "created_at > TIMESTAMPTZ '2026-03-27T00:00:00+00:00' AND "
                "created_at <= TIMESTAMPTZ '2026-03-28T00:00:00+00:00'))"
            ),
            dbtable='(SELECT * FROM "source"."clients" WHERE ... ) AS src',
        ),
        df=clients_df,
        run_date=date(2026, 3, 28),
        run_date_str="2026-03-28",
        run_id="manual__2026-03-28T04_00_00_00_00",
        manifest_base={
            "manifest_version": "1",
            "source_system": "postgres-pre-raw",
            "source_schema": "source",
            "source_table": "clients",
            "contract_name": "source.clients",
            "contract_version": "1",
            "run_id": "manual__2026-03-28T04_00_00_00_00",
            "ingest_ts_utc": "2026-03-28T04:00:05+00:00",
            "batch_type": "incremental",
            "storage_format": "parquet",
            "schema_hash": "example_schema_hash",
            "producer": {
                "tool": "spark",
                "image": "ghcr.io/antonminiazev/ampere-spark:latest",
                "app_name": "source-to-raw-etl",
                "git_sha": "unknown",
            },
            "source_extract": {
                "query": '(SELECT * FROM "source"."clients" WHERE ... ) AS src',
                "read_mode": "jdbc",
                "partitioning": None,
            },
        },
        base_columns=["client_id", "created_at", "updated_at", "name"],
        args=SimpleNamespace(
            source_system="postgres-pre-raw",
            schema="source",
        ),
        table="clients",
        ingest_ts_utc="2026-03-28T04:00:05+00:00",
    )
    ```
    """
    if plan.group_mode == "snapshot":
        write_snapshot_table(
            spark=spark,
            df=df,
            plan=plan,
            run_date_str=run_date_str,
            run_id=run_id,
            manifest_base=manifest_base,
            base_columns=base_columns,
            logger=logger,
        )
        return

    if plan.group_partition_key == "extract_date":
        write_mutable_dim_table(
            spark=spark,
            df=df,
            plan=plan,
            run_date_str=run_date_str,
            run_id=run_id,
            manifest_base=manifest_base,
            base_columns=base_columns,
            source_system=args.source_system,
            schema=args.schema,
            table=table,
            ingest_ts_utc=ingest_ts_utc,
            logger=logger,
        )
        return

    if not plan.table_event_col:
        raise ValueError("event_date_column is required for event_date batches.")
    writer = write_fact_table if plan.group_name == "facts" else write_event_table
    writer(
        spark=spark,
        df=df,
        plan=plan,
        run_date=run_date,
        run_id=run_id,
        manifest_base=manifest_base,
        base_columns=base_columns,
        logger=logger,
    )
