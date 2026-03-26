from __future__ import annotations

import logging
import random
from datetime import date, timedelta

import numpy as np
import polars as pl
from faker import Faker

from order_data_generator.config import GeneratorConfig
from order_data_generator.db import upload_new_data
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


def run_generation(run_date: date, config: GeneratorConfig) -> None:
    """Execute the full daily source-side generation workflow for one logical date.

    The function first updates the customer base, then inserts new clients, and
    finally generates operational order-related tables for the same business
    day. This is the daily mutating step that makes the downstream raw landing
    and bronze jobs meaningful instead of repeatedly reading static data.
    """
    yesterday = run_date - timedelta(days=1)

    logger.info("Starting daily generation workflow")
    # Step 0 updates the dimension-like part of the source system. Existing
    # clients may churn, and new clients are inserted before orders are created.
    logger.info("Preparing client churn and new client generation")
    to_churn_ids, clients_for_upload = prepare_clients_update_and_generation(
        run_date, config
    )
    update_churned(to_churn_ids, config.schema, run_date)

    if clients_for_upload:
        upload_new_data(pl.DataFrame(clients_for_upload), "clients", config.schema)

    # Step 1 creates the fact/event side of the source system for this day.
    logger.info("Preparing orders, statuses, delivery tracking, and payments")
    prepare_orders_statuses(run_date, yesterday, config)
    logger.info("Daily generation workflow completed")

