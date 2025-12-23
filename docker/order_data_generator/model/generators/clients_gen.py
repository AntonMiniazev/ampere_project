from __future__ import annotations

from datetime import date

import numpy as np
import polars as pl
from faker import Faker

from order_data_generator.config import GeneratorConfig
from order_data_generator.db import exec_sql, read_sql

fake = Faker()


def update_churned(ids: list[int], schema: str) -> None:
    if not ids:
        print("No clients for update.")
        return

    id_list_str = ",".join(str(i) for i in ids)
    query = f'''
        UPDATE "{schema}"."clients"
        SET churned = TRUE
        WHERE id IN ({id_list_str})
    '''
    exec_sql(query)
    print(f"Updated {len(ids)} clients (churned = TRUE)")


def generate_clients(n: int, store_id: int, today: date) -> list[dict]:
    clients = []

    for _ in range(n):
        fullname = f"{fake.first_name()} {fake.last_name()}"
        clients.append(
            {
                "fullname": fullname,
                "preferred_store_id": int(store_id),
                "registration_date": today,
                "churned": False,
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
    clients_df = read_sql(client_query)
    active_clients = clients_df.filter(pl.col("churned") == False)

    to_churn_ids: list[int] = []
    clients_for_upload: list[dict] = []

    store_ids = active_clients.get_column("preferred_store_id").unique().to_list()

    for store_id in store_ids:
        store_clients = active_clients.filter(
            pl.col("preferred_store_id") == store_id
        )
        n_clients = store_clients.height
        if n_clients == 0:
            continue

        churn_rate = np.random.uniform(config.churn_rates[0], config.churn_rates[1])
        n_churn = int(n_clients * churn_rate)
        if n_churn > 0:
            client_ids = store_clients.get_column("id").to_list()
            churn_ids = np.random.choice(client_ids, size=n_churn, replace=False).tolist()
            to_churn_ids.extend(churn_ids)

        new_clients_rate = np.random.uniform(
            config.new_clients_rates[0], config.new_clients_rates[1]
        )
        n_new_clients = int(n_clients * new_clients_rate)
        if n_new_clients > 0:
            clients_for_upload.extend(generate_clients(n_new_clients, store_id, today))

    return to_churn_ids, clients_for_upload

