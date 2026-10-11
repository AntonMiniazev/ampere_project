from __future__ import annotations

import logging
import hashlib
import json
import os
import random
from dataclasses import asdict
from datetime import date, timedelta

import numpy as np
import polars as pl
from faker import Faker
from sqlalchemy import text

from order_data_generator.config import GeneratorConfig
from order_data_generator.db import (
    complete_generation_batch,
    get_engine,
    start_generation_batch,
    upload_new_data,
)
from order_data_generator.generators.clients_gen import (
    prepare_clients_update_and_generation,
    update_churned,
)
from order_data_generator.generators.orders_gen import prepare_orders_statuses
from order_data_generator.logging_utils import APP_NAME

logger = logging.getLogger(APP_NAME)


def set_seed(seed: int) -> None:
    """Seed all random libraries used by the generator.

    Synthetic pipelines are much easier to explain and test when a run can be
    reproduced exactly. Seeding Python, NumPy, and Faker together ensures that
    names, churn decisions, order counts, and status timings all follow the same
    deterministic scenario when needed.
    """
    random.seed(seed)
    np.random.seed(seed)
    Faker.seed(seed)


def _run_generation_in_transaction(
    run_date: date,
    config: GeneratorConfig,
    connection,
    *,
    scenario_seed: int | None = None,
) -> bool:
    """Generate one logical date using the caller's active PostgreSQL transaction."""
    yesterday = run_date - timedelta(days=1)
    fingerprint = hashlib.sha256(
        json.dumps(asdict(config), sort_keys=True, default=list).encode("utf-8")
    ).hexdigest()
    code_version = os.getenv("AMPERE_SOURCE_CODE_VERSION", "unknown")

    logger.info("Starting atomic generation workflow for %s", run_date)
    # Serialize source progression across init/daily/backfill callers. The
    # advisory lock is held through commit or rollback for this business date.
    connection.execute(
        text("SELECT pg_advisory_xact_lock(:lock_key)"),
        {"lock_key": 724614902631},
    )
    batch = start_generation_batch(
        connection,
        run_date,
        scenario_seed=scenario_seed,
        config_fingerprint=fingerprint,
        source_code_version=code_version,
    )
    if batch is None:
        return False
    logger.info("Preparing client churn and new client generation")
    to_churn_ids, clients_for_upload = prepare_clients_update_and_generation(
        run_date, config, connection=connection
    )
    update_churned(to_churn_ids, config.schema, run_date, connection=connection)

    if clients_for_upload:
        upload_new_data(
            pl.DataFrame(clients_for_upload),
            "clients",
            config.schema,
            connection=connection,
        )

    logger.info("Preparing orders, statuses, delivery tracking, and payments")
    prepare_orders_statuses(run_date, yesterday, config, connection=connection)
    latest_generated = connection.execute(
        text(f'SELECT MAX(order_date) FROM "{config.schema}"."orders"')
    ).scalar_one_or_none()
    if latest_generated != run_date:
        raise RuntimeError(
            f"Generation for {run_date} committed no orders; refusing to advance"
        )
    change_counts = complete_generation_batch(connection, batch)
    changed_tables = sum(count > 0 for count in change_counts.values())
    logger.info(
        "Completed source batch dataset=%s batch=%s sequence=%s tables_changed=%s rows_changed=%s",
        batch["dataset_id"],
        batch["batch_id"],
        batch["batch_sequence"],
        changed_tables,
        sum(change_counts.values()),
    )
    logger.info("Daily generation workflow completed for %s", run_date)
    return True


def run_generation(
    run_date: date, config: GeneratorConfig, *, scenario_seed: int | None = None
) -> bool:
    """Generate one logical date and commit its source changes atomically."""
    with get_engine().begin() as connection:
        return _run_generation_in_transaction(
            run_date, config, connection, scenario_seed=scenario_seed
        )


def run_generation_range(
    start_date: date,
    end_date: date,
    config: GeneratorConfig,
    *,
    seed: int | None = None,
) -> int:
    """Generate sequentially in calendar-month transactions.

    Days remain ordered and use bounded daily working frames, but the journal
    and source writes for a month commit together. A retry resumes from the
    prior committed month; an interrupted month rolls back and can be rerun.
    """
    if end_date < start_date:
        raise ValueError("end_date must be on or after start_date")
    day_count = (end_date - start_date).days + 1

    completed = 0
    total = 0
    run_date = start_date
    engine = get_engine()
    while run_date <= end_date:
        month_start = run_date.replace(day=1)
        next_month = (month_start.replace(day=28) + timedelta(days=4)).replace(day=1)
        month_end = min(end_date, next_month - timedelta(days=1))
        month_completed = 0
        logger.info(
            "Starting monthly source transaction for %s..%s",
            run_date,
            month_end,
        )
        with engine.begin() as connection:
            while run_date <= month_end:
                total += 1
                if seed is not None:
                    # Keep each date reproducible across retries and releases.
                    day_seed = (seed + run_date.toordinal()) % (2**32)
                    set_seed(day_seed)
                else:
                    day_seed = None
                logger.info("Backfill day %s/%s: %s", total, day_count, run_date)
                if _run_generation_in_transaction(
                    run_date,
                    config,
                    connection,
                    scenario_seed=day_seed,
                ):
                    completed += 1
                    month_completed += 1
                run_date += timedelta(days=1)
        logger.info(
            "Committed monthly source transaction through %s: generated=%s",
            month_end,
            month_completed,
        )

    logger.info(
        "Generation range finished: requested=%s..%s generated=%s skipped=%s",
        start_date,
        end_date,
        completed,
        total - completed,
    )
    return completed
