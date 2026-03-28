"""Snapshot writers for source-to-raw extraction."""

from __future__ import annotations

import logging

from raw.common import RawTablePlan, build_dbtable, write_batch


def build_snapshot_plan(
    *,
    group_name: str,
    group_mode: str,
    group_partition_key: str,
    snapshot_partitioned: bool,
    output_base: str,
    table: str,
    table_meta: dict,
    group_event_col: str | None,
    group_lookback_days: int,
    group_watermark_col: str | None,
    args,
) -> RawTablePlan:
    """Build the extraction plan for snapshot tables."""
    table_event_col = table_meta.get("event_date_column") or group_event_col
    table_lookback_days = table_meta.get("lookback_days")
    if table_lookback_days is None:
        table_lookback_days = group_lookback_days
    table_watermark_col = table_meta.get("watermark_column") or group_watermark_col
    table_created_col = table_meta.get("created_column") or None
    dbtable = build_dbtable(args.schema, table, None)
    return RawTablePlan(
        group_name=group_name,
        group_mode=group_mode,
        group_partition_key=group_partition_key,
        snapshot_partitioned=snapshot_partitioned,
        output_base=output_base,
        table_event_col=table_event_col,
        table_lookback_days=int(table_lookback_days or 0),
        table_watermark_col=table_watermark_col,
        table_created_col=table_created_col,
        table_cursor_granularity="timestamp",
        state_path_value=None,
        table_watermark_from=None,
        table_created_from=None,
        table_watermark_to=None,
        bootstrap_full_extract=False,
        initial_event_load=False,
        where_clause=None,
        dbtable=dbtable,
    )


def write_snapshot_table(
    *,
    spark,
    df,
    plan: RawTablePlan,
    run_date_str: str,
    run_id: str,
    manifest_base: dict,
    base_columns: list[str],
    logger: logging.Logger,
) -> None:
    """Write one snapshot batch for the logical run date."""
    partition_value = run_date_str
    if plan.snapshot_partitioned:
        output_path = (
            f"{plan.output_base}/mode=snapshot/snapshot_date={partition_value}/run_id={run_id}/"
        )
    else:
        output_path = f"{plan.output_base}/mode=snapshot/run_id={run_id}/"
    manifest_context = {
        **manifest_base,
        "snapshot_date": partition_value,
    }
    write_batch(
        spark,
        df,
        output_path,
        manifest_context,
        base_columns,
        logger,
    )
