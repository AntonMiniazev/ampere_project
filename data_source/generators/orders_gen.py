import atexit
import os
import time
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from faker import Faker
from sqlalchemy import create_engine, text

# Load environment variables
load_dotenv()
# Initialize Faker
fake = Faker()
today = datetime.today().date() + timedelta(days=3)
yesterday = today - timedelta(days=1)

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

# Constants for order generation
avg_orders = 1.35
min_orders = 1
max_orders = 4

# Constants for product generation
min_products = 5
max_products = 20
avg_products = 10


@atexit.register
def cleanup_engine():
    print("Disposing engine")
    engine.dispose()


def scaled_beta(mean_target, low, high, alpha=2):
    # Convert target mean to normalized [0,1] scale
    mean_norm = (mean_target - low) / (high - low)

    # Fix alpha (controls concentration), calculate beta accordingly
    alpha = 2
    beta_param = alpha * (1 - mean_norm) / mean_norm

    # Generate a single beta sample
    sample = np.random.beta(alpha, beta_param)

    # Scale back to original range
    return low + sample * (high - low)


def exec_sql(query: str) -> pd.DataFrame | None:
    start = time.perf_counter()
    with engine.begin() as conn:
        result = conn.execute(text(query))
        try:
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
            elapsed = time.perf_counter() - start
            print(f"Query (SELECT) executed in {elapsed:.3f} seconds")
            return df
        except Exception:
            elapsed = time.perf_counter() - start
            print(f"Query (DML) executed in {elapsed:.3f} seconds")
            return None


def generate_products_in_order(
    assortment: pd.DataFrame, store_id: int, client_ids: list
):
    prod_in_orders = []
    store_assortment = assortment[assortment["store_id"] == store_id]
    product_ids = store_assortment["product_id"].tolist()
    weights = store_assortment["chance"].astype(float).values

    weights = weights / weights.sum()

    for i in client_ids:
        n_items = int(
            round(scaled_beta(avg_products, min_products, max_products), 0)
        )  # number of products in this order: 5–20
        chosen_products = np.random.choice(
            product_ids, size=n_items, replace=False, p=weights
        )

        for product_id in chosen_products:
            unit_type = store_assortment.loc[
                store_assortment["product_id"] == product_id, "unit_type"
            ].values[0]
            store_id = store_assortment["store_id"].iloc[
                0
            ]  # all rows have same store_id

            if unit_type == "units":
                n_units = np.random.randint(1, 5)
            else:
                n_units = round(np.random.uniform(0.100, 4.000), 3)

            price = store_assortment.loc[
                store_assortment["product_id"] == product_id, "price"
            ].values[0]

            prod_in_orders.append(
                {
                    "client_id": int(i),
                    "store_id": int(store_id),
                    "product_id": int(product_id),
                    "unit_type": unit_type,
                    "quantity": float(n_units),
                    "price": float(price),
                }
            )

    return prod_in_orders


def clients_to_orders(
    avg_orders: float,
    n_stores: int,
    client_base: pd.DataFrame,
    couriers_df: pd.DataFrame,
    assortment_df: pd.DataFrame,
):
    orders = []
    products_in_orders = []

    for store_id in range(1, n_stores + 1):
        store_clients = client_base[client_base["preferred_store_id"] == store_id]
        store_couriers = couriers_df[couriers_df["store_id"] == store_id]

        delivery_type_ids = [1, 2, 3]
        status_ids = [1, 2, 3]

        if store_clients.empty or store_couriers.empty:
            continue

        # Generate number of orders for this store
        n_clients = store_clients.shape[0]
        n_orders = int(
            scaled_beta(mean_target=avg_orders, low=min_orders, high=max_orders)
            / 7
            * n_clients
        )

        # Sample client ids who made an order
        client_ids = store_clients.sample(n=n_orders, replace=False)["id"].tolist()

        # Determine delivery type probabilities
        p1 = np.random.uniform(0.45, 0.55)
        p2 = np.random.uniform(0.30, 0.40)
        p3 = 1.0 - p1 - p2

        delivery_types = np.random.choice(
            delivery_type_ids, size=n_orders, p=[p1, p2, p3]
        )

        # Determine delivery type probabilities
        s1 = np.random.uniform(0.005, 0.01)
        s3 = np.random.uniform(0.97, 0.98)
        s2 = 1.0 - s1 - s3

        statuses = np.random.choice(status_ids, size=n_orders, p=[s1, s2, s3])

        # Get couriers by delivery type
        couriers_by_type = {
            dtype: store_couriers[store_couriers["delivery_type_id"] == dtype][
                "id"
            ].tolist()
            for dtype in delivery_type_ids
        }

        # Generate order source ids
        # Build result
        for client_id, dtype, status in zip(client_ids, delivery_types, statuses):
            # Generate order source id
            order_source_id = round(scaled_beta(2, 1, 3, alpha=1), 0)

            courier_pool = couriers_by_type[dtype]
            if not courier_pool:
                continue  # skip if no courier available for this type
            courier_id = np.random.choice(courier_pool)
            orders.append(
                {
                    "client_id": int(client_id),
                    "courier_id": int(courier_id),
                    "delivery_type_id": int(dtype),
                    "store_id": int(store_id),
                    "order_source_id": int(order_source_id),
                    "status_id": int(status),
                    "order_date": today,
                }
            )

        # Generate orders with products for this store
        products_in_orders.extend(
            generate_products_in_order(
                assortment_df,
                store_id=store_id,
                client_ids=client_ids,
            )
        )

    df_orders = pd.DataFrame(orders)
    df_products = pd.DataFrame(products_in_orders).drop(columns=["store_id"])

    df_order_product = pd.merge(df_orders, df_products, on="client_id", how="inner")

    return df_order_product


def upload_new_data(table: pd.DataFrame, target_table: str):
    if table.empty:
        print("No data to upload.")
        return

    if target_table == "delivery_tracking":
        with engine.begin() as conn:
            conn.execute(
                text(f"""DELETE FROM [core].[delivery_tracking]
                              WHERE status IS NULL AND order_id IN (SELECT order_id FROM [core].[order_status_history] WHERE order_status_id = 3 AND status_datetime >= '{yesterday}')
                              """)
            )

    table.to_sql(
        name=target_table,
        con=engine,
        schema="core",
        if_exists="append",
        index=False,
    )


def generate_order_status_history(
    order_status_history_input: pd.DataFrame, today: datetime.date
) -> pd.DataFrame:
    """
    Generate status history for orders by courier based on their latest status and order date.
    """

    records = []

    for courier_id, group in order_status_history_input.groupby("courier_id"):
        sorted_orders = group.sort_values("order_id")
        # Start between 08:00 and 08:15
        courier_clock = datetime.combine(today, datetime.min.time()) + timedelta(
            hours=8, minutes=np.random.randint(0, 16)
        )

        for _, row in sorted_orders.iterrows():
            order_id = row["order_id"]
            status_id = int(row["status_id"])
            order_date = row["order_date"]

            # Case A+B unified: either full chain (status 3), or yesterday's partial order
            if status_id == 3 or (order_date < today and status_id in (1, 2)):
                total_duration = timedelta(minutes=np.random.randint(45, 55))
                stage_duration = total_duration / 3

                # If yesterday and status_id == 1 → include 2 and 3 only
                if order_date < today and status_id == 1:
                    status2_time = courier_clock
                    status3_time = (
                        status2_time
                        + stage_duration
                        + timedelta(minutes=np.random.randint(-1, 2))
                    )

                    records.append(
                        {
                            "order_id": order_id,
                            "order_status_id": 2,
                            "status_datetime": status2_time,
                        }
                    )
                    records.append(
                        {
                            "order_id": order_id,
                            "order_status_id": 3,
                            "status_datetime": status3_time,
                        }
                    )

                    courier_clock = status3_time + timedelta(
                        minutes=np.random.randint(8, 15)
                    )

                # If yesterday and status_id == 2 → only status 3
                elif order_date < today and status_id == 2:
                    status3_time = courier_clock

                    records.append(
                        {
                            "order_id": order_id,
                            "order_status_id": 3,
                            "status_datetime": status3_time,
                        }
                    )
                    courier_clock = status3_time + timedelta(
                        minutes=np.random.randint(8, 15)
                    )

                # Otherwise full chain 1–2–3
                else:
                    status1_time = courier_clock
                    status2_time = (
                        status1_time
                        + stage_duration
                        + timedelta(minutes=np.random.randint(-1, 2))
                    )
                    status3_time = (
                        status2_time
                        + stage_duration
                        + timedelta(minutes=np.random.randint(-1, 2))
                    )

                    records.append(
                        {
                            "order_id": order_id,
                            "order_status_id": 1,
                            "status_datetime": status1_time,
                        }
                    )
                    records.append(
                        {
                            "order_id": order_id,
                            "order_status_id": 2,
                            "status_datetime": status2_time,
                        }
                    )
                    records.append(
                        {
                            "order_id": order_id,
                            "order_status_id": 3,
                            "status_datetime": status3_time,
                        }
                    )

                    courier_clock = status3_time + timedelta(
                        minutes=np.random.randint(10, 17)
                    )

            # Case C: today, status = 2 → backfill status 1
            elif order_date == today and status_id == 2:
                status2_time = datetime.combine(today, datetime.min.time()) + timedelta(
                    hours=19, minutes=np.random.randint(0, 60)
                )
                status1_time = status2_time - timedelta(
                    minutes=np.random.randint(10, 16)
                )

                records.append(
                    {
                        "order_id": order_id,
                        "order_status_id": 1,
                        "status_datetime": status1_time,
                    }
                )
                records.append(
                    {
                        "order_id": order_id,
                        "order_status_id": 2,
                        "status_datetime": status2_time,
                    }
                )

            # Case D: today, status = 1 → just status 1
            elif order_date == today and status_id == 1:
                status1_time = datetime.combine(today, datetime.min.time()) + timedelta(
                    hours=19, minutes=np.random.randint(0, 60)
                )
                records.append(
                    {
                        "order_id": order_id,
                        "order_status_id": 1,
                        "status_datetime": status1_time,
                    }
                )

    return pd.DataFrame(records)


def generate_payments(
    payments_input: pd.DataFrame, today: datetime.date, yesterday: datetime.date
) -> pd.DataFrame:
    """
    Generate payments for orders based on their status and order date.
    """
    records = []

    for _, row in payments_input.iterrows():
        order_id = row["order_id"]
        amount = row["amount"]
        order_date = row["order_date"]
        latest_status = row["latest_status"]
        method = np.random.choice(
            ["card", "cash", "cash+bonuses", "card+bonuses"], p=[0.6, 0.2, 0.05, 0.15]
        )

        # Define payment date
        if order_date == today and latest_status < 3:  # order not delivered
            payment_date = np.random.choice(
                [today, today + timedelta(days=1)], p=[0.2, 0.8]
            )  # 20% prepayments and 80% after delivery

        elif order_date == today and latest_status == 3:  # order delivered:
            payment_date = today

        elif order_date < today and latest_status == 3:  # yesterday's order delivered
            payment_date = np.random.choice(
                [today, yesterday], p=[0.8, 0.2]
            )  # 20% prepayments and 80% after delivery

        # Define payment status
        if payment_date == today or payment_date == yesterday:
            payment_status = "paid"
        else:
            payment_status = "unpaid"

        records.append(
            {
                "order_id": order_id,
                "amount": amount,
                "method": method,
                "payment_status": payment_status,
                "payment_date": payment_date,
            }
        )

    return pd.DataFrame(records)


def generate_delivery_tracking(delivery_tracking_input: pd.DataFrame) -> pd.DataFrame:
    records = []

    for _, row in delivery_tracking_input.iterrows():
        order_id = row["order_id"]
        courier_id = row["courier_id"]
        delivery_time = row["delivered_time"]
        if pd.isna(row["packing_time"]):
            pickup_time = delivery_time - timedelta(minutes=np.random.randint(5, 15))
        else:
            pickup_time = row["packing_time"]

        records.append(
            {
                "order_id": order_id,
                "courier_id": courier_id,
                "delivery_status_id": 1,
                "status": "pick up",
                "status_time": pickup_time,
            }
        )
        records.append(
            {
                "order_id": order_id,
                "courier_id": courier_id,
                "delivery_status_id": 2,
                "status": "delivery",
                "status_time": delivery_time,
            }
        )

    return pd.DataFrame(records)


client_query = """
    SELECT 
        id,
        fullname,
        preferred_store_id,
        registration_date,
        churned
    FROM [core].[clients]
"""

store_query = """
    SELECT COUNT(*)
    FROM [core].[stores]
"""

couriers_query = """
    SELECT id, delivery_type_id, store_id
    FROM [core].[delivery_resource]
    WHERE active_flag = 1
"""

assortment_query = """
    SELECT a.product_id, a.store_id, p.unit_type, p.chance, p.price
    FROM [core].[assortment] a
    LEFT JOIN [core].[products] p ON a.product_id = p.id
"""

orders_query = f"""
    SELECT id as order_id, client_id
    FROM [core].[orders]
    WHERE order_date = '{today}'
"""

statuses = """
    SELECT id as status_id
    FROM [core].[orders]
    WHERE id < 4
"""

yesterday_orders = f"""
    WITH status_ranked AS (
        SELECT 
            osh.order_id,
            osh.order_status_id,
            ROW_NUMBER() OVER (PARTITION BY osh.order_id ORDER BY osh.order_status_id DESC) AS rn
        FROM [core].[order_status_history] osh
    ),

    latest_status AS (
        SELECT order_id, order_status_id
        FROM status_ranked
        WHERE rn = 1
    )

    SELECT 
        o.id AS order_id,
        dt.courier_id,
        o.order_date,
        ls.order_status_id AS status_id
    FROM [core].[orders] o
    LEFT JOIN latest_status ls ON o.id = ls.order_id
    LEFT JOIN [core].[delivery_tracking] dt ON o.id = dt.order_id
    WHERE o.order_date = '{yesterday}' AND ls.order_status_id < 3
"""

payments_query = f"""
    WITH latest_order_status AS (
        SELECT
            order_id,
            MAX(order_status_id) AS latest_status
        FROM [core].[order_status_history]
        WHERE status_datetime >= '{yesterday}'
        GROUP BY order_id
    )
    
    SELECT
        o.id as order_id,
        o.total_amount as amount,
        o.order_date,
        los.latest_status as latest_status
    FROM [core].[orders] o

    LEFT JOIN latest_order_status los ON o.id = los.order_id

    WHERE (o.order_date >= '{yesterday}')
    AND o.id NOT IN (SELECT order_id FROM [core].[payments] WHERE (payment_date >= '{yesterday}') AND payment_status = 'paid')
"""

delete_unpaid_payments = f"""
    DELETE FROM [core].[payments]
    WHERE payment_date >= '{yesterday}' AND payment_status = 'unpaid'
"""

clients_df = exec_sql(client_query)
n_stores = exec_sql(store_query).iloc[0, 0]
couriers_df = exec_sql(couriers_query)
assortment_df = exec_sql(assortment_query)
statuses_df = exec_sql(statuses)
yesterday_orders_df = exec_sql(yesterday_orders)

order_product = clients_to_orders(
    avg_orders=avg_orders,
    n_stores=n_stores,
    client_base=clients_df,
    couriers_df=couriers_df,
    assortment_df=assortment_df,
)

order_product["total_amount"] = round(
    order_product["quantity"] * order_product["price"], 2
)

orders_upload = order_product.groupby(
    ["client_id", "order_date", "order_source_id"], as_index=False
).agg({"total_amount": "sum"})

upload_new_data(orders_upload, target_table="orders")

today_orders_df = exec_sql(orders_query)
order_product = pd.merge(order_product, today_orders_df, on=["client_id"], how="left")

orders_product_upload = order_product[["order_id", "product_id", "quantity"]]

upload_new_data(orders_product_upload, target_table="order_product")

delivery_tracking_upload_init = order_product[
    ["order_id", "courier_id"]
].drop_duplicates()

upload_new_data(delivery_tracking_upload_init, target_table="delivery_tracking")

order_status_history_input = order_product[
    ["order_id", "courier_id", "order_date", "status_id"]
].drop_duplicates()

order_status_history_input = pd.concat(
    [order_status_history_input, yesterday_orders_df], ignore_index=True
)


order_status_history = generate_order_status_history(order_status_history_input, today)

upload_new_data(order_status_history, target_table="order_status_history")

delivery_tracking_input = pd.merge(
    order_status_history[order_status_history["order_status_id"] == 3]
    .drop(columns=["order_status_id"])
    .rename(columns={"status_datetime": "delivered_time"}),
    order_status_history[order_status_history["order_status_id"] == 2]
    .drop(columns=["order_status_id"])
    .rename(columns={"status_datetime": "packing_time"}),
    on=["order_id"],
    how="left",
)

######################################################################
delivery_tracking_input.to_csv(
    "delivery_tracking_input" + str(today) + ".csv", index=False
)

delivery_tracking_input = pd.merge(
    delivery_tracking_input,
    order_status_history_input[["order_id", "courier_id"]].drop_duplicates(),
    on=["order_id"],
    how="left",
)


delivery_tracking_upload = generate_delivery_tracking(delivery_tracking_input)

upload_new_data(delivery_tracking_upload, target_table="delivery_tracking")

payments_df = exec_sql(payments_query)
exec_sql(delete_unpaid_payments)

payments_upload = generate_payments(
    payments_input=payments_df, today=today, yesterday=yesterday
)

upload_new_data(payments_upload, target_table="payments")
