import numpy as np
from config import churn_rates, new_clients_rates
from db.mssql import engine
from dotenv import load_dotenv
from faker import Faker
from generators.utils import today
from sqlalchemy import text

from data_source.db.db_io import exec_sql

# Load environment variables
load_dotenv()
# Initialize Faker
fake = Faker()

client_query = """
    SELECT 
        id,
        fullname,
        preferred_store_id,
        registration_date,
        churned
    FROM [core].[clients]
"""


def update_churned(ids: list):
    if not ids:
        print("No clients for update.")
        return

    id_list_str = ",".join(str(i) for i in ids)

    query = f"""
        UPDATE [core].[clients]
        SET churned = 1
        WHERE id IN ({id_list_str})
    """

    with engine.connect() as conn:
        conn.execute(text(query))
        conn.commit()
        print(f"Updated {len(ids)} clients (churned = 1)")


def generate_clients(n: int, store_id: int):
    clients = []

    for i in range(1, n + 1):
        fullname = f"{fake.first_name()} {fake.last_name()}"

        clients.append(
            {
                "fullname": fullname,
                "preferred_store_id": int(store_id),
                "registration_date": today.strftime("%Y-%m-%d"),
                "churned": 0,  # New clients are not churned
            }
        )

    return clients


def prepare_clients_update_and_generation():
    clients_df = exec_sql(client_query)
    active_clients = clients_df[clients_df["churned"] == 0].copy()

    # Define churn logic
    to_churn_ids = []
    clients_for_upload = []

    for store_id in active_clients["preferred_store_id"].unique():
        store_clients = active_clients[active_clients["preferred_store_id"] == store_id]
        churn_rate = np.random.uniform(
            churn_rates[0], churn_rates[1]
        )  # between 0.4% and 0.7%
        n_churn = int(len(store_clients) * churn_rate)
        churn_ids = store_clients.sample(n=n_churn)["id"].tolist()
        to_churn_ids.extend(churn_ids)

        # Generate new clients for the store
        new_clients_rate = np.random.uniform(
            new_clients_rates[0], new_clients_rates[1]
        )  # between 0.6% and 1%
        n_new_clients = int(len(store_clients) * new_clients_rate)
        clients_for_upload.extend(generate_clients(n_new_clients, store_id))

    return to_churn_ids, clients_for_upload
