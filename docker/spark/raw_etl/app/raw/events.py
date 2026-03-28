"""Event writers for source-to-raw extraction."""

from __future__ import annotations

import logging

from raw.common import (
    RawTablePlan,
    build_dbtable,
    build_where_clause,
    has_incremental_output,
    write_event_partition_batches,
)


def build_event_plan(
    *,
    spark,
    logger: logging.Logger,
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
    run_date,
) -> RawTablePlan:
    """Build the extraction plan for event-style tables."""
    table_event_col = table_meta.get("event_date_column") or group_event_col
    table_lookback_days = table_meta.get("lookback_days")
    if table_lookback_days is None:
        table_lookback_days = group_lookback_days
    table_watermark_col = table_meta.get("watermark_column") or group_watermark_col
    table_created_col = table_meta.get("created_column") or None

    initial_event_load = False
    if group_mode == "incremental" and group_partition_key == "event_date":
        if not has_incremental_output(spark, output_base):
            initial_event_load = True
            logger.info(
                "Initial event load detected; extracting full table for %s",
                table,
            )

    where_clause = None
    if not initial_event_load:
        where_clause = build_where_clause(
            group_mode,
            group_partition_key,
            run_date,
            table_event_col,
            table_watermark_col,
            table_created_col,
            int(table_lookback_days or 0),
            None,
            None,
            None,
        )

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
        state_path_value=None,
        table_watermark_from=None,
        table_created_from=None,
        table_watermark_to=None,
        bootstrap_full_extract=False,
        initial_event_load=initial_event_load,
        where_clause=where_clause,
        dbtable=build_dbtable(args.schema, table, where_clause),
    )


def write_event_table(
    *,
    spark,
    df,
    plan: RawTablePlan,
    run_date,
    run_id: str,
    manifest_base: dict,
    base_columns: list[str],
    logger: logging.Logger,
) -> None:
    """Write raw event batches partitioned by event_date."""
    write_event_partition_batches(
        spark=spark,
        df=df,
        output_base=plan.output_base,
        event_col=plan.table_event_col or "",
        run_date=run_date,
        run_id=run_id,
        manifest_base=manifest_base,
        base_columns=base_columns,
        lookback_days=plan.table_lookback_days,
        initial_event_load=plan.initial_event_load,
        logger=logger,
    )
