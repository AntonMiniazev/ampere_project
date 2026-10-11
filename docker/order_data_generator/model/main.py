from __future__ import annotations

import logging
import random
from datetime import date, timedelta

import numpy as np
import polars as pl
from faker import Faker
from sqlalchemy import text

from order_data_generator.config import GeneratorConfig
from order_data_generator.db import get_engine, upload_new_data
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


def run_generation(run_date: date, config: GeneratorConfig) -> bool:
    """Execute the full daily source-side generation workflow for one logical date.

    The function first updates the customer base, then inserts new clients, and
    finally generates operational order-related tables for the same business
    day. This is the daily mutating step that makes the downstream raw landing
    and bronze jobs meaningful instead of repeatedly reading static data.
    """
    yesterday = run_date - timedelta(days=1)

    logger.info("Starting atomic generation workflow for %s", run_date)
    # Serialize source progression across init/daily/backfill callers. The
    # advisory lock is held through commit or rollback for this business date.
    with get_engine().begin() as connection:
        connection.execute(
            text("SELECT pg_advisory_xact_lock(:lock_key)"),
            {"lock_key": 724614902631},
        )
        latest_generated = connection.execute(
            text(f'SELECT MAX(order_date) FROM "{config.schema}"."orders"')
        ).scalar_one_or_none()
        if latest_generated is not None:
            if run_date <= latest_generated:
                logger.info(
                    "Skipping already-generated date %s (source is complete through %s)",
                    run_date,
                    latest_generated,
                )
                return False
            expected_date = latest_generated + timedelta(days=1)
            if run_date != expected_date:
                raise ValueError(
                    f"Generation must be chronological: source is complete through "
                    f"{latest_generated}, next date is {expected_date}, requested {run_date}"
                )
        else:
            latest_registration = connection.execute(
                text(
                    f'SELECT MAX(registration_date) FROM "{config.schema}"."clients"'
                )
            ).scalar_one_or_none()
            if latest_registration is not None and run_date < latest_registration:
                raise ValueError(
                    f"Generation date {run_date} precedes the latest initialized client "
                    f"registration date {latest_registration}"
                )
        logger.info("Preparing client churn and new client generation")
        to_churn_ids, clients_for_upload = prepare_clients_update_and_generation(
            run_date, config, connection=connection
        )
        update_churned(to_churn_ids, config.schema, run_date, connection=connection)

        if clients_for_upload:
            upload_new_data(
                pl.DataFrame(clients_for_upload), "clients", config.schema,
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
    logger.info("Daily generation workflow completed for %s", run_date)
    return True


def run_generation_range(
    start_date: date,
    end_date: date,
    config: GeneratorConfig,
    *,
    seed: int | None = None,
    max_days: int = 31,
) -> int:
    """Generate an inclusive date range sequentially in this process.

    Each date commits independently through ``run_generation``. The maximum
    source ``orders.order_date`` acts as a durable checkpoint, so retrying the
    range skips completed dates and resumes after them. A stable date-derived
    seed makes a retry reproduce the same daily random choices.
    """
    if end_date < start_date:
        raise ValueError("end_date must be on or after start_date")
    day_count = (end_date - start_date).days + 1
    if day_count > max_days:
        raise ValueError(f"Range is {day_count} days; maximum supported is {max_days}")

    completed = 0
    total = 0
    run_date = start_date
    while run_date <= end_date:
        total += 1
        if seed is not None:
            # Do not use Python's randomized hash(); the same date always gets
            # the same RNG state across containers and process restarts.
            set_seed((seed + run_date.toordinal()) % (2**32))
        logger.info("Backfill day %s/%s: %s", total, day_count, run_date)
        if run_generation(run_date, config):
            completed += 1
        run_date += timedelta(days=1)

    logger.info(
        "Generation range finished: requested=%s..%s generated=%s skipped=%s",
        start_date,
        end_date,
        completed,
        total - completed,
    )
    return completed

