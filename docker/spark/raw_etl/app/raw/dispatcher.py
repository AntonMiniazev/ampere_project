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
    """Build a group-specific table extraction plan."""
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
    """Write a raw table using the module that owns the group semantics."""
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
