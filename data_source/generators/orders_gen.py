from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from db.db_io import exec_sql, upload_new_data
from dotenv import load_dotenv
from faker import Faker
from generators.config import (
    avg_orders,
    avg_products,
    courier_clock_delta,
    courier_hours,
    delivery_type_ids,
    dt_p1,
    dt_p2,
    eod_orders_time,
    max_orders,
    max_products,
    max_units,
    max_weight,
    min_orders,
    min_products,
    min_units,
    min_weight,
    ord_source_p,
    order_cycle_minutes,
    os_p1,
    os_p2,
    pickup_timing,
    pmt_type_p,
    prepayment_p,
    status_ids,
    time_between_statuses,
)
from generators.utils import (
    scaled_beta,
)

# Load environment variables
load_dotenv()
# Initialize Faker
fake = Faker()


def generate_products_in_order(
    assortment: pd.DataFrame,
    store_id: int,
    client_ids: list,
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
                n_units = np.random.randint(min_units, max_units)
            else:
                n_units = round(np.random.uniform(min_weight, max_weight), 3)

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
    n_stores: int,
    client_base: pd.DataFrame,
    couriers_df: pd.DataFrame,
    assortment_df: pd.DataFrame,
    today: datetime.date,
):
    orders = []
    products_in_orders = []

    for store_id in range(1, n_stores + 1):
        store_clients = client_base[client_base["preferred_store_id"] == store_id]
        store_couriers = couriers_df[couriers_df["store_id"] == store_id]

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
        p1 = np.random.uniform(dt_p1[0], dt_p1[1])
        p2 = np.random.uniform(dt_p2[0], dt_p2[1])
        p3 = 1.0 - p1 - p2

        delivery_types = np.random.choice(
            delivery_type_ids, size=n_orders, p=[p1, p2, p3]
        )

        # Determine order statuses probabilities
        s1 = np.random.uniform(os_p1[0], os_p1[1])
        s3 = np.random.uniform(os_p2[0], os_p2[1])
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
            order_source_id = round(
                scaled_beta(ord_source_p[0], ord_source_p[1], ord_source_p[2], alpha=2),
                0,
            )

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
    df_order_product["total_amount"] = round(
        df_order_product["quantity"] * df_order_product["price"], 2
    )

    df_orders_upload = df_order_product.groupby(
        ["client_id", "order_date", "order_source_id"], as_index=False
    ).agg({"total_amount": "sum"})

    return df_order_product, df_orders_upload


def generate_order_status_history(
    order_status_history_input: pd.DataFrame, today: datetime.date
) -> pd.DataFrame:
    """
    Generate status history for orders by courier based on their latest status and order date.
    """

    records = []

    for _, group in order_status_history_input.groupby("courier_id"):
        sorted_orders = group.sort_values("order_id")
        # Start between 08:00 and 08:15
        courier_clock = datetime.combine(today, datetime.min.time()) + timedelta(
            hours=courier_hours,
            minutes=np.random.randint(courier_clock_delta[0], courier_clock_delta[1]),
        )

        for _, row in sorted_orders.iterrows():
            order_id = row["order_id"]
            status_id = int(row["status_id"])
            order_date = row["order_date"]

            # Case A+B unified: either full chain (status 3), or yesterday's partial order
            if status_id == 3 or (order_date < today and status_id in (1, 2)):
                total_duration = timedelta(
                    minutes=np.random.randint(
                        order_cycle_minutes[0], order_cycle_minutes[1]
                    )
                )
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
                        minutes=np.random.randint(
                            time_between_statuses[0], time_between_statuses[1]
                        )
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
                        minutes=np.random.randint(
                            time_between_statuses[0], time_between_statuses[1]
                        )
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
                        minutes=np.random.randint(
                            time_between_statuses[0], time_between_statuses[1]
                        )
                    )

            # Case C: today, status = 2 → backfill status 1
            elif order_date == today and status_id == 2:
                status2_time = datetime.combine(today, datetime.min.time()) + timedelta(
                    hours=eod_orders_time, minutes=np.random.randint(0, 60)
                )
                status1_time = status2_time - timedelta(
                    minutes=np.random.randint(
                        time_between_statuses[0], time_between_statuses[1]
                    )
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


def generate_payments(today, yesterday) -> pd.DataFrame:
    """
    Generate payments for orders based on their status and order date.
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

    # Extract only unpaid payments
    payments_input = exec_sql(payments_query)
    # Clean payments that will be updated
    exec_sql(delete_unpaid_payments)

    records = []

    for _, row in payments_input.iterrows():
        order_id = row["order_id"]
        amount = row["amount"]
        order_date = row["order_date"]
        latest_status = row["latest_status"]
        method = np.random.choice(
            ["card", "cash", "cash+bonuses", "card+bonuses"], p=pmt_type_p
        )

        # Define payment date
        if order_date == today and latest_status < 3:  # order not delivered
            payment_date = np.random.choice(
                [today, today + timedelta(days=1)], p=prepayment_p
            )  # 20% prepayments and 80% after delivery

        elif order_date == today and latest_status == 3:  # order delivered:
            payment_date = today

        elif order_date < today and latest_status == 3:  # yesterday's order delivered
            payment_date = np.random.choice(
                [yesterday, today], p=prepayment_p
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
            pickup_time = delivery_time - timedelta(
                minutes=np.random.randint(pickup_timing[0], pickup_timing[1])
            )
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


def prepare_raw_data(yesterday):
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

    clients_df = exec_sql(client_query)
    n_stores = exec_sql(store_query).iloc[0, 0]
    couriers_df = exec_sql(couriers_query)
    assortment_df = exec_sql(assortment_query)
    yesterday_orders_df = exec_sql(yesterday_orders)

    return (
        clients_df,
        n_stores,
        couriers_df,
        assortment_df,
        yesterday_orders_df,
    )


def prepare_orders_statuses(today: datetime.date, yesterday: datetime.date):
    todays_orders_query = f"""
        SELECT id as order_id, client_id
        FROM [core].[orders]
        WHERE order_date = '{today}'
    """

    # Step 0 Load raw data
    (
        clients_df,
        n_stores,
        couriers_df,
        assortment_df,
        yesterday_orders_df,
    ) = prepare_raw_data(yesterday)

    order_product, orders_upload = clients_to_orders(
        n_stores=n_stores,
        client_base=clients_df,
        couriers_df=couriers_df,
        assortment_df=assortment_df,
        today=today,
    )

    # Step 1: Uploading generated orders (ids are assigned using IDENTITY(1,1) in SQL Server)
    upload_new_data(orders_upload, target_table="orders", yesterday=yesterday)

    # Extract only today's orders with ids generated in DB and add these ids to order_product
    today_orders_df = exec_sql(todays_orders_query)
    order_product = pd.merge(
        order_product, today_orders_df, on=["client_id"], how="left"
    )

    # Step 2: Uploading generated products in each order
    orders_product_upload = order_product[["order_id", "product_id", "quantity"]]
    upload_new_data(
        orders_product_upload, target_table="order_product", yesterday=yesterday
    )

    # Step 3: Generating order status history and delivery tracking
    # Temporary extract today's orders and assigned couriers and put in delivery_tracking
    delivery_tracking_upload_init = order_product[
        ["order_id", "courier_id"]
    ].drop_duplicates()

    # Creating order_id and courier_id link in delivery_tracking and delete all finished orders from delivery_tracking without tracking info
    # This delivery tracking info is generated in this step
    upload_new_data(
        delivery_tracking_upload_init,
        target_table="delivery_tracking",
        yesterday=yesterday,
    )

    # Creating inputs for order status history generation
    order_status_history_input = order_product[
        ["order_id", "courier_id", "order_date", "status_id"]
    ].drop_duplicates()

    # Add yestarday's unfinished orders to add today's statuses
    order_status_history_input = pd.concat(
        [order_status_history_input, yesterday_orders_df], ignore_index=True
    )
    order_status_history = generate_order_status_history(
        order_status_history_input, today
    )
    upload_new_data(
        order_status_history, target_table="order_status_history", yesterday=yesterday
    )

    # Delivery tracking data is generated based on delivered orders. Unfinished orders stays in delivery_tracking from step 3 first upload
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

    # Add assigned couriers to delivery tracking data
    delivery_tracking_input = pd.merge(
        delivery_tracking_input,
        order_status_history_input[["order_id", "courier_id"]].drop_duplicates(),
        on=["order_id"],
        how="left",
    )
    # Generation delivery tracking data
    delivery_tracking_upload = generate_delivery_tracking(delivery_tracking_input)

    # Uploading generated data
    upload_new_data(
        delivery_tracking_upload, target_table="delivery_tracking", yesterday=yesterday
    )

    # Step 4: Generating and uploading payments
    payments_upload = generate_payments(today, yesterday)
    upload_new_data(payments_upload, target_table="payments", yesterday=yesterday)
