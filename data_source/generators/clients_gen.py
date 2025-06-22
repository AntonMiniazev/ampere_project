import atexit
import os
import time
from datetime import datetime

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from faker import Faker
from sqlalchemy import create_engine, text

# Load environment variables
load_dotenv()
# Initialize Faker
fake = Faker()

# Build connection URL
server = os.getenv("SERVER_ADDRESS", "localhost")
db_source = "source"
username = os.getenv("MSSQL_USER")
password = os.getenv("MSSQL_PASSWORD")
port = os.getenv("MSSQL_NODE_PORT")
driver = "ODBC Driver 17 for SQL Server"

connection_url = (
    f"mssql+pyodbc://{username}:{password}@{server},{port}/{db_source}"
    f"?driver={driver.replace(' ', '+')}&TrustServerCertificate=yes"
)

engine = create_engine(connection_url)


@atexit.register
def cleanup_engine():
    print("Disposing engine")
    engine.dispose()


def select_sql(query: str) -> pd.DataFrame:
    start = time.perf_counter()
    with engine.connect() as conn:
        result = conn.execute(text(query))
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
    elapsed = time.perf_counter() - start
    print(f"Query executed in {elapsed:.3f} seconds")
    return df


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
                "registration_date": datetime.today().strftime("%Y-%m-%d"),
                "churned": 0,  # New clients are not churned
            }
        )

    return clients


def upload_new_clients(clients: list[dict]):
    if not clients:
        print("No clients to upload.")
        return

    query = text("""
        INSERT INTO core.clients (fullname, preferred_store_id, registration_date, churned)
        VALUES (:fullname, :preferred_store_id, :registration_date, :churned)
    """)

    with engine.begin() as conn:
        conn.execute(query, clients)
        print(f"Uploaded {len(clients)} new clients.")


client_query = """
    SELECT 
        id,
        fullname,
        preferred_store_id,
        registration_date,
        churned
    FROM [core].[clients]
"""

clients_df = select_sql(client_query)
active_clients = clients_df[clients_df["churned"] == 0].copy()

# Define churn logic
to_churn_ids = []
clients_for_upload = []

for store_id in active_clients["preferred_store_id"].unique():
    store_clients = active_clients[active_clients["preferred_store_id"] == store_id]
    churn_rate = np.random.uniform(0.004, 0.007)  # between 0.4% and 0.7%
    n_churn = int(len(store_clients) * churn_rate)
    churn_ids = store_clients.sample(n=n_churn)["id"].tolist()
    to_churn_ids.extend(churn_ids)

    # Generate new clients for the store
    new_clients_rate = np.random.uniform(0.006, 0.01)  # between 0.6% and 1%
    n_new_clients = int(len(store_clients) * new_clients_rate)
    clients_for_upload.extend(generate_clients(n_new_clients, store_id))

update_churned(to_churn_ids)
upload_new_clients(clients_for_upload)
