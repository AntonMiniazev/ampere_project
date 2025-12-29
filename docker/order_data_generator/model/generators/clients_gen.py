from __future__ import annotations

import logging
from datetime import date

import numpy as np
import polars as pl
from faker import Faker

from order_data_generator.config import GeneratorConfig
from order_data_generator.db import exec_sql, read_sql
from order_data_generator.logging_utils import APP_NAME

fake = Faker()
logger = logging.getLogger(APP_NAME)


def update_churned(ids: list[int], schema: str, today: date) -> None:
    if not ids:
        logger.info("No clients marked for churn update.")
        return

    # Bulk-update churned flag in a single SQL statement.
    id_list_str = ",".join(str(i) for i in ids)
    query = f'''
        UPDATE "{schema}"."clients"
        SET churned = TRUE,
            updated_at = '{today.isoformat()}'
        WHERE id IN ({id_list_str})
    '''
    exec_sql(query)
    logger.info("Updated %s clients (churned = TRUE)", len(ids))


def generate_clients(n: int, store_id: int, today: date) -> list[dict]:
    clients = []
    for _ in range(n):
        # Use Faker to create realistic names and register them today.
        fullname = f"{fake.first_name()} {fake.last_name()}"
        clients.append(
            {
                "fullname": fullname,
                "preferred_store_id": int(store_id),
                "registration_date": today,
                "churned": False,  # New clients are not churned
            }
        )

    return clients


def prepare_clients_update_and_generation(
    today: date, config: GeneratorConfig
) -> tuple[list[int], list[dict]]:
    client_query = f'''
        SELECT id, fullname, preferred_store_id, registration_date, churned
        FROM "{config.schema}"."clients"
    '''
    # Read current clients and keep only active ones for churn/new sampling.
    clients_df = read_sql(client_query)
    active_clients = clients_df.filter(pl.col("churned") == False)

    to_churn_ids: list[int] = []
    clients_for_upload: list[dict] = []

    store_ids = active_clients.get_column("preferred_store_id").unique().to_list()

    for store_id in store_ids:
        # Work per-store to preserve local churn and acquisition rates.
        store_clients = active_clients.filter(
            pl.col("preferred_store_id") == store_id
        )
        n_clients = store_clients.height
        if n_clients == 0:
            continue

        churn_rate = np.random.uniform(config.churn_rates[0], config.churn_rates[1])
        n_churn = int(n_clients * churn_rate)
        if n_churn > 0:
            # Sample churned clients without replacement.
            client_ids = store_clients.get_column("id").to_list()
            churn_ids = np.random.choice(client_ids, size=n_churn, replace=False).tolist()
            to_churn_ids.extend(churn_ids)

        new_clients_rate = np.random.uniform(
            config.new_clients_rates[0], config.new_clients_rates[1]
        )
        n_new_clients = int(n_clients * new_clients_rate)
        if n_new_clients > 0:
            # Create new clients for this store with today's registration date.
            clients_for_upload.extend(generate_clients(n_new_clients, store_id, today))

    return to_churn_ids, clients_for_upload

