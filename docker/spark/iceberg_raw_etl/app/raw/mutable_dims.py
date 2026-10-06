"""Mutable-dimension writers for source-to-raw extraction."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone

from etl_utils import format_ts, parse_optional_datetime, read_json, state_path, write_bytes
from raw.common import RawTablePlan, build_dbtable, build_where_clause, write_batch


def build_mutable_dim_plan(
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
    """Build the extraction plan for mutable dimensions."""
    table_event_col = table_meta.get("event_date_column") or group_event_col
    table_lookback_days = table_meta.get("lookback_days")
    if table_lookback_days is None:
        table_lookback_days = group_lookback_days
    table_watermark_col = table_meta.get("watermark_column") or group_watermark_col
    table_created_col = table_meta.get("created_column") or None
    table_cursor_granularity = (
        table_meta.get("cursor_granularity") or "timestamp"
    ).strip().lower()
    if table_cursor_granularity not in {"date", "timestamp"}:
        raise ValueError(
            f"Unsupported cursor_granularity={table_cursor_granularity!r} for {table}."
        )
    if not table_watermark_col:
        raise ValueError("watermark_column is required for extract_date batches.")

    state_path_value = state_path(args.bucket, args.source_system, args.schema, table)
    table_watermark_from = parse_optional_datetime(args.watermark_from.strip())
    table_created_from = None
    table_watermark_to = parse_optional_datetime(args.watermark_to.strip())
    bootstrap_full_extract = False

    if table_watermark_from is None:
        state = read_json(spark, state_path_value, logger)
        if state:
            if state.get("last_updated_at"):
                table_watermark_from = parse_optional_datetime(state["last_updated_at"])
            elif state.get("last_watermark"):
                table_watermark_from = parse_optional_datetime(state["last_watermark"])
            if state.get("last_created_at"):
                table_created_from = parse_optional_datetime(state["last_created_at"])
        else:
            bootstrap_full_extract = True
    if table_watermark_to is None:
        table_watermark_to = datetime.now(timezone.utc)
    if table_watermark_from is None and not bootstrap_full_extract:
        table_watermark_from = datetime(1900, 1, 1, tzinfo=timezone.utc)
    if table_created_from is None and not bootstrap_full_extract:
        table_created_from = datetime(1900, 1, 1, tzinfo=timezone.utc)

    where_clause = None
    if not bootstrap_full_extract:
        where_clause = build_where_clause(
            group_mode,
            group_partition_key,
            run_date,
            table_event_col,
            table_watermark_col,
            table_created_col,
            table_cursor_granularity,
            int(table_lookback_days or 0),
            table_watermark_from,
            table_created_from,
            table_watermark_to,
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
        table_cursor_granularity=table_cursor_granularity,
        state_path_value=state_path_value,
        table_watermark_from=table_watermark_from,
        table_created_from=table_created_from,
        table_watermark_to=table_watermark_to,
        bootstrap_full_extract=bootstrap_full_extract,
        initial_event_load=False,
        where_clause=where_clause,
        dbtable=build_dbtable(args.schema, table, where_clause),
    )


def write_mutable_dim_table(
    *,
    spark,
    df,
    plan: RawTablePlan,
    run_date_str: str,
    run_id: str,
    manifest_base: dict,
    base_columns: list[str],
    source_system: str,
    schema: str,
    table: str,
    ingest_ts_utc: str,
    logger: logging.Logger,
) -> None:
    """Write one mutable-dimension batch and update the landing state file."""
    output_path = (
        f"{plan.output_base}/mode=incremental/extract_date={run_date_str}/run_id={run_id}/"
    )
    manifest_context = {
        **manifest_base,
        "extract_date": run_date_str,
    }
    upper = plan.table_watermark_to or datetime.now(timezone.utc)
    if plan.bootstrap_full_extract:
        lower = datetime(1900, 1, 1, tzinfo=timezone.utc)
    else:
        if plan.table_lookback_days > 0:
            lower = upper - timedelta(days=plan.table_lookback_days)
        else:
            lower = plan.table_watermark_from or (upper - timedelta(days=1))
    if plan.table_watermark_col:
        window_from = format_ts(lower)
        window_to = format_ts(upper)
        manifest_context["watermark"] = {
            "column": plan.table_watermark_col,
            "from": window_from,
            "to": window_to,
        }
        if plan.table_lookback_days > 0:
            manifest_context["lookback_days"] = plan.table_lookback_days
            manifest_context["window"] = {
                "from": window_from,
                "to": window_to,
            }

    batch_result = write_batch(
        spark,
        df,
        output_path,
        manifest_context,
        base_columns,
        logger,
    )
    if plan.state_path_value and batch_result["success"]:
        if plan.table_cursor_granularity == "date":
            last_cursor_value = upper.date().isoformat()
        else:
            last_cursor_value = format_ts(upper)
        state_payload = {
            "source_system": source_system,
            "source_schema": schema,
            "source_table": table,
            "watermark_column": plan.table_watermark_col,
            "created_column": plan.table_created_col,
            "cursor_granularity": plan.table_cursor_granularity,
            "last_watermark": last_cursor_value,
            "last_updated_at": last_cursor_value,
            "last_created_at": last_cursor_value,
            "last_successful_run_id": run_id,
            "last_successful_ingest_ts_utc": ingest_ts_utc,
            "last_manifest_path": batch_result["manifest_path"],
        }
        write_bytes(
            spark,
            plan.state_path_value,
            json.dumps(state_payload, indent=2, sort_keys=True).encode("utf-8"),
        )
