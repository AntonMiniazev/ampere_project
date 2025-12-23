from __future__ import annotations

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


def set_seed(seed: int) -> None:
    random.seed(seed)
    np.random.seed(seed)
    Faker.seed(seed)


def run_generation(run_date: date, config: GeneratorConfig) -> None:
    yesterday = run_date - timedelta(days=1)

    to_churn_ids, clients_for_upload = prepare_clients_update_and_generation(
        run_date, config
    )
    update_churned(to_churn_ids, config.schema)

    if clients_for_upload:
        upload_new_data(pl.DataFrame(clients_for_upload), "clients", config.schema)

    prepare_orders_statuses(run_date, yesterday, config)

