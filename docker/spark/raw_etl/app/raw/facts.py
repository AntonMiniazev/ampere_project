"""Fact writers for source-to-raw extraction."""

from __future__ import annotations

import logging

from raw.common import RawTablePlan
from raw.events import build_event_plan, write_event_table


def build_fact_plan(**kwargs) -> RawTablePlan:
    """Build the extraction plan for fact tables."""
    return build_event_plan(**kwargs)


def write_fact_table(
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
    """Write raw fact batches partitioned by event_date."""
    write_event_table(
        spark=spark,
        df=df,
        plan=plan,
        run_date=run_date,
        run_id=run_id,
        manifest_base=manifest_base,
        base_columns=base_columns,
        logger=logger,
    )
