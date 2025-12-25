from __future__ import annotations

from datetime import date, datetime, timedelta

import numpy as np
import polars as pl

from order_data_generator.config import GeneratorConfig
from order_data_generator.db import (
    exec_sql,
    fetch_delivery_type_ids,
    fetch_order_status_map,
    insert_orders_returning_ids,
    read_sql,
    upload_new_data,
)
from order_data_generator.generators.utils import scaled_beta


def _normalize_date(value) -> date:
    if isinstance(value, datetime):
        return value.date()
    return value


def _clamp_times_to_date(base_date: date, times: list[datetime]) -> list[datetime]:
    end_of_day = datetime.combine(base_date, datetime.max.time()).replace(microsecond=0)
    latest = max(times)
    if latest <= end_of_day:
        return times

    overflow = latest - end_of_day
    return [value - overflow for value in times]


def generate_products_in_order(
    assortment: pl.DataFrame,
    store_id: int,
    store_orders: list[dict],
    config: GeneratorConfig,
) -> list[dict]:
    prod_in_orders: list[dict] = []

    store_assortment = assortment.filter(pl.col("store_id") == store_id)
    if store_assortment.height == 0:
        return prod_in_orders

    # Build weighted product choices per store.
    product_ids = store_assortment.get_column("product_id").to_list()
    weights = store_assortment.get_column("chance").cast(pl.Float64).to_list()
    weights = np.array(weights, dtype=float)
    total_weight = weights.sum()
    if total_weight == 0:
        weights = np.ones_like(weights) / len(weights)
    else:
        weights = weights / total_weight

    product_meta = {
        int(row["product_id"]): row
        for row in store_assortment.select(
            ["product_id", "unit_type", "price"]
        ).iter_rows(named=True)
    }

    for order in store_orders:
        # Number of products in the order bounded by config.
        n_items = int(
            round(
                scaled_beta(
                    config.avg_products, config.min_products, config.max_products
                ),
                0,
            )
        )
        n_items = max(1, min(n_items, len(product_ids)))

        # Sample products without replacement to avoid duplicates in a single order.
        chosen_products = np.random.choice(
            product_ids, size=n_items, replace=False, p=weights
        )

        for product_id in chosen_products:
            meta = product_meta[int(product_id)]
            unit_type = meta["unit_type"]
            if unit_type == "units":
                # Units-based products use integer quantity.
                n_units = int(np.random.randint(config.min_units, config.max_units))
            else:
                # Weight-based products use fractional quantity.
                n_units = round(
                    float(np.random.uniform(config.min_weight, config.max_weight)), 3
                )

            prod_in_orders.append(
                {
                    "order_key": order["order_key"],
                    "client_id": int(order["client_id"]),
                    "product_id": int(product_id),
                    "quantity": float(n_units),
                    "price": float(meta["price"]),
                }
            )

    return prod_in_orders


def clients_to_orders(
    n_stores: int,
    client_base: pl.DataFrame,
    couriers_df: pl.DataFrame,
    assortment_df: pl.DataFrame,
    today: date,
    config: GeneratorConfig,
    delivery_type_ids: list[int],
    created_status_id: int,
    packing_status_id: int,
    delivered_status_id: int,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    orders: list[dict] = []
    products_in_orders: list[dict] = []
    order_key = 0

    for store_id in range(1, n_stores + 1):
        store_clients = client_base.filter(pl.col("preferred_store_id") == store_id)
        store_couriers = couriers_df.filter(pl.col("store_id") == store_id)

        if store_clients.height == 0 or store_couriers.height == 0:
            continue

        n_clients = store_clients.height
        # Scale weekly order rate down to daily volume per active client base.
        n_orders = int(
            scaled_beta(
                mean_target=config.avg_orders,
                low=config.min_orders,
                high=config.max_orders,
            )
            / 7
            * n_clients
        )
        if n_orders <= 0:
            continue

        client_ids = store_clients.get_column("id").to_list()
        if n_orders > len(client_ids):
            n_orders = len(client_ids)
        sampled_clients = np.random.choice(
            client_ids, size=n_orders, replace=False
        ).tolist()

        # Choose delivery types per store based on configured ranges.
        p1 = np.random.uniform(config.dt_p1[0], config.dt_p1[1])
        p2 = np.random.uniform(config.dt_p2[0], config.dt_p2[1])
        p3 = max(0.0, 1.0 - p1 - p2)

        delivery_types = np.random.choice(
            delivery_type_ids, size=n_orders, p=[p1, p2, p3]
        )

        # Choose initial order status; unfinished orders will be completed tomorrow.
        s1 = np.random.uniform(config.os_p1[0], config.os_p1[1])
        s3 = np.random.uniform(config.os_p2[0], config.os_p2[1])
        s2 = max(0.0, 1.0 - s1 - s3)
        status_probs = np.array([s1, s2, s3], dtype=float)
        status_probs = status_probs / status_probs.sum()
        status_choices = [created_status_id, packing_status_id, delivered_status_id]
        statuses = np.random.choice(status_choices, size=n_orders, p=status_probs)

        # Build courier pools by delivery type for assignment.
        couriers_by_type = {
            dtype: store_couriers.filter(pl.col("delivery_type_id") == dtype)
            .get_column("id")
            .to_list()
            for dtype in delivery_type_ids
        }

        store_orders: list[dict] = []
        for client_id, dtype, status in zip(sampled_clients, delivery_types, statuses):
            courier_pool = couriers_by_type.get(int(dtype), [])
            if not courier_pool:
                continue

            # Generate order source id via scaled beta distribution.
            order_source_id = int(
                round(
                    scaled_beta(
                        config.ord_source_p[0],
                        config.ord_source_p[1],
                        config.ord_source_p[2],
                        alpha=2,
                    ),
                    0,
                )
            )
            courier_id = int(np.random.choice(courier_pool))

            order_key += 1
            # Keep order_key for local joins until IDs are inserted into DB.
            order = {
                "order_key": order_key,
                "client_id": int(client_id),
                "courier_id": courier_id,
                "delivery_type_id": int(dtype),
                "store_id": int(store_id),
                "order_source_id": order_source_id,
                "status_id": int(status),
                "order_date": today,
            }
            store_orders.append(order)
            orders.append(order)

        products_in_orders.extend(
            generate_products_in_order(assortment_df, store_id, store_orders, config)
        )

    orders_df = pl.DataFrame(orders)
    products_df = pl.DataFrame(products_in_orders)
    return orders_df, products_df


def generate_order_status_history(
    order_status_history_input: pl.DataFrame,
    today: date,
    status_map: dict[str, int],
    config: GeneratorConfig,
) -> pl.DataFrame:
    records: list[dict] = []

    created_id = status_map["created"]
    packing_id = status_map["packing"]
    delivered_id = status_map["delivered"]

    for _, group in order_status_history_input.group_by("courier_id"):
        # Process orders per courier to keep realistic time progression.
        sorted_orders = group.sort("order_id")
        courier_clock = datetime.combine(today, datetime.min.time()) + timedelta(
            hours=config.courier_hours,
            minutes=np.random.randint(
                config.courier_clock_delta[0], config.courier_clock_delta[1]
            ),
        )

        for row in sorted_orders.iter_rows(named=True):
            order_id = row["order_id"]
            status_id = int(row["status_id"])
            order_date = _normalize_date(row["order_date"])

            # Case A+B: full chain (delivered) or yesterday's partial order.
            # Case A+B: delivered today, or yesterday's unfinished orders to finish today.
            if status_id == delivered_id or (
                order_date < today and status_id in (created_id, packing_id)
            ):
                # Base date is today for normal flow, or the original order date for old delivered.
                base_date = (
                    order_date
                    if order_date < today and status_id == delivered_id
                    else today
                )
                order_clock = courier_clock
                update_courier_clock = True
                if base_date != today:
                    order_clock = datetime.combine(
                        base_date, datetime.min.time()
                    ) + timedelta(
                        hours=config.courier_hours,
                        minutes=np.random.randint(
                            config.courier_clock_delta[0], config.courier_clock_delta[1]
                        ),
                    )
                    update_courier_clock = False
                total_duration = timedelta(
                    minutes=np.random.randint(
                        config.order_cycle_minutes[0],
                        config.order_cycle_minutes[1],
                    )
                )
                stage_duration = total_duration / 3

                # Yesterday + created: include packing and delivered only.
                if order_date < today and status_id == created_id:
                    status2_time = order_clock
                    status3_time = (
                        status2_time
                        + stage_duration
                        + timedelta(minutes=np.random.randint(-1, 2))
                    )

                    # Clamp to base date to avoid spilling into the next day.
                    status2_time, status3_time = _clamp_times_to_date(
                        base_date, [status2_time, status3_time]
                    )

                    records.append(
                        {
                            "order_id": order_id,
                            "order_status_id": packing_id,
                            "status_datetime": status2_time,
                        }
                    )
                    records.append(
                        {
                            "order_id": order_id,
                            "order_status_id": delivered_id,
                            "status_datetime": status3_time,
                        }
                    )

                    if update_courier_clock:
                        courier_clock = status3_time + timedelta(
                            minutes=np.random.randint(
                                config.time_between_statuses[0],
                                config.time_between_statuses[1],
                            )
                        )

                # Yesterday + packing: add delivered only.
                elif order_date < today and status_id == packing_id:
                    status3_time = order_clock

                    status3_time = _clamp_times_to_date(base_date, [status3_time])[0]

                    records.append(
                        {
                            "order_id": order_id,
                            "order_status_id": delivered_id,
                            "status_datetime": status3_time,
                        }
                    )
                    if update_courier_clock:
                        courier_clock = status3_time + timedelta(
                            minutes=np.random.randint(
                                config.time_between_statuses[0],
                                config.time_between_statuses[1],
                            )
                        )

                # Otherwise full chain 1 → 2 → 3 for same-day delivery.
                else:
                    status1_time = order_clock
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

                    # Clamp to base date to keep same-day progression.
                    status1_time, status2_time, status3_time = _clamp_times_to_date(
                        base_date, [status1_time, status2_time, status3_time]
                    )

                    records.append(
                        {
                            "order_id": order_id,
                            "order_status_id": created_id,
                            "status_datetime": status1_time,
                        }
                    )
                    records.append(
                        {
                            "order_id": order_id,
                            "order_status_id": packing_id,
                            "status_datetime": status2_time,
                        }
                    )
                    records.append(
                        {
                            "order_id": order_id,
                            "order_status_id": delivered_id,
                            "status_datetime": status3_time,
                        }
                    )

                    if update_courier_clock:
                        courier_clock = status3_time + timedelta(
                            minutes=np.random.randint(
                                config.time_between_statuses[0],
                                config.time_between_statuses[1],
                            )
                        )

            # Case C: today + packing -> backfill created.
            elif order_date == today and status_id == packing_id:
                status2_time = datetime.combine(today, datetime.min.time()) + timedelta(
                    hours=config.eod_orders_time, minutes=np.random.randint(0, 60)
                )
                status1_time = status2_time - timedelta(
                    minutes=np.random.randint(
                        config.time_between_statuses[0],
                        config.time_between_statuses[1],
                    )
                )

                records.append(
                    {
                        "order_id": order_id,
                        "order_status_id": created_id,
                        "status_datetime": status1_time,
                    }
                )
                records.append(
                    {
                        "order_id": order_id,
                        "order_status_id": packing_id,
                        "status_datetime": status2_time,
                    }
                )

            # Case D: today + created -> only created.
            elif order_date == today and status_id == created_id:
                status1_time = datetime.combine(today, datetime.min.time()) + timedelta(
                    hours=config.eod_orders_time, minutes=np.random.randint(0, 60)
                )
                records.append(
                    {
                        "order_id": order_id,
                        "order_status_id": created_id,
                        "status_datetime": status1_time,
                    }
                )

    return pl.DataFrame(records)


def generate_payments(
    today: date,
    yesterday: date,
    schema: str,
    delivered_status_id: int,
    config: GeneratorConfig,
) -> pl.DataFrame:
    # Use latest status to decide prepayment vs after-payment behavior.
    payments_query = f'''
        WITH latest_order_status AS (
            SELECT
                order_id,
                MAX(order_status_id) AS latest_status
            FROM "{schema}"."order_status_history"
            WHERE status_datetime >= :yesterday
            GROUP BY order_id
        )

        SELECT
            o.id as order_id,
            o.total_amount as amount,
            o.order_date,
            los.latest_status as latest_status
        FROM "{schema}"."orders" o
        LEFT JOIN latest_order_status los ON o.id = los.order_id
        WHERE o.order_date >= :yesterday
          AND o.id NOT IN (
            SELECT order_id
            FROM "{schema}"."payments"
            WHERE payment_date >= :yesterday AND payment_status = 'paid'
          )
    '''

    delete_unpaid_payments = f'''
        DELETE FROM "{schema}"."payments"
        WHERE payment_date >= :yesterday AND payment_status = 'unpaid'
    '''

    payments_input = read_sql(payments_query, {"yesterday": yesterday})
    exec_sql(delete_unpaid_payments, {"yesterday": yesterday})

    records: list[dict] = []

    for row in payments_input.iter_rows(named=True):
        order_id = row["order_id"]
        amount = row["amount"]
        order_date = _normalize_date(row["order_date"])
        latest_status = int(row["latest_status"])
        method = np.random.choice(
            ["card", "cash", "cash+bonuses", "card+bonuses"], p=config.pmt_type_p
        )

        # Prepayment/after-payment based on delivery status and order date.
        if order_date == today and latest_status < delivered_status_id:
            payment_date = np.random.choice(
                [today, today + timedelta(days=1)], p=config.prepayment_p
            )
        elif order_date == today and latest_status == delivered_status_id:
            payment_date = today
        elif order_date < today and latest_status == delivered_status_id:
            payment_date = np.random.choice([yesterday, today], p=config.prepayment_p)
        else:
            payment_date = today

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

    return pl.DataFrame(records)


def generate_delivery_tracking(
    delivery_tracking_input: pl.DataFrame, config: GeneratorConfig
) -> pl.DataFrame:
    records: list[dict] = []

    for row in delivery_tracking_input.iter_rows(named=True):
        order_id = row["order_id"]
        courier_id = row["courier_id"]
        delivery_time = row["delivered_time"]
        # If packing time is missing, derive pickup time from delivered_time.
        if row.get("packing_time") is None:
            pickup_time = delivery_time - timedelta(
                minutes=np.random.randint(
                    config.pickup_timing[0], config.pickup_timing[1]
                )
            )
        else:
            pickup_time = row["packing_time"]

        records.append(
            {
                "order_id": order_id,
                "courier_id": courier_id,
                "delivery_status_id": 1,
                "status": "pick up",
                "status_datetime": pickup_time,
            }
        )
        records.append(
            {
                "order_id": order_id,
                "courier_id": courier_id,
                "delivery_status_id": 2,
                "status": "delivery",
                "status_datetime": delivery_time,
            }
        )

    return pl.DataFrame(records)


def prepare_raw_data(
    yesterday: date, config: GeneratorConfig, delivered_status_id: int
) -> tuple[pl.DataFrame, int, pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    # Pull required source data and yesterday's unfinished orders.
    client_query = f'''
        SELECT id, fullname, preferred_store_id, registration_date, churned
        FROM "{config.schema}"."clients"
    '''

    store_query = f'''
        SELECT COUNT(*) AS store_count
        FROM "{config.schema}"."stores"
    '''

    couriers_query = f'''
        SELECT id, delivery_type_id, store_id
        FROM "{config.schema}"."delivery_resource"
        WHERE active_flag = TRUE
    '''

    assortment_query = f'''
        SELECT a.product_id, a.store_id, p.unit_type, p.chance, p.price
        FROM "{config.schema}"."assortment" a
        LEFT JOIN "{config.schema}"."products" p ON a.product_id = p.id
    '''

    yesterday_orders = f'''
        WITH yesterday_orders AS (
            SELECT id
            FROM "{config.schema}"."orders"
            WHERE order_date = :yesterday
        ),
        latest_status AS (
            SELECT DISTINCT ON (osh.order_id)
                osh.order_id,
                osh.order_status_id
            FROM "{config.schema}"."order_status_history" osh
            JOIN yesterday_orders yo ON yo.id = osh.order_id
            ORDER BY osh.order_id, osh.status_datetime DESC
        ),
        delivery_tracking_yesterday AS (
            SELECT DISTINCT dt.order_id, dt.courier_id
            FROM "{config.schema}"."delivery_tracking" dt
            JOIN yesterday_orders yo ON yo.id = dt.order_id
        )
        SELECT
            o.id AS order_id,
            dty.courier_id,
            o.order_date,
            ls.order_status_id AS status_id
        FROM "{config.schema}"."orders" o
        LEFT JOIN latest_status ls ON o.id = ls.order_id
        LEFT JOIN delivery_tracking_yesterday dty ON o.id = dty.order_id
        WHERE o.order_date = :yesterday AND ls.order_status_id < :delivered_status_id
    '''

    clients_df = read_sql(client_query)
    store_count_df = read_sql(store_query)
    n_stores = int(store_count_df[0, 0]) if store_count_df.height else 0
    couriers_df = read_sql(couriers_query)
    assortment_df = read_sql(assortment_query)
    yesterday_orders_df = read_sql(
        yesterday_orders,
        {"yesterday": yesterday, "delivered_status_id": delivered_status_id},
    )

    return (
        clients_df,
        n_stores,
        couriers_df,
        assortment_df,
        yesterday_orders_df,
    )


def prepare_orders_statuses(
    today: date, yesterday: date, config: GeneratorConfig
) -> None:
    status_map = fetch_order_status_map(config.schema)
    for status_name in ("created", "packing", "delivered"):
        if status_name not in status_map:
            raise ValueError(f"Missing {status_name} in order_statuses dictionary")

    delivery_type_ids = fetch_delivery_type_ids(config.schema)
    if len(delivery_type_ids) < 3:
        raise ValueError("Expected at least 3 delivery types in dictionary")

    created_status_id = status_map["created"]
    packing_status_id = status_map["packing"]
    delivered_status_id = status_map["delivered"]

    (
        clients_df,
        n_stores,
        couriers_df,
        assortment_df,
        yesterday_orders_df,
    ) = prepare_raw_data(yesterday, config, delivered_status_id)

    orders_df, order_product = clients_to_orders(
        n_stores=n_stores,
        client_base=clients_df,
        couriers_df=couriers_df,
        assortment_df=assortment_df,
        today=today,
        config=config,
        delivery_type_ids=delivery_type_ids,
        created_status_id=created_status_id,
        packing_status_id=packing_status_id,
        delivered_status_id=delivered_status_id,
    )

    order_status_history_frames: list[pl.DataFrame] = []

    if orders_df.height and order_product.height:
        # Step 1: upload orders and order products.
        # Join lines to orders to compute totals and persist orders first.
        order_product = order_product.join(
            orders_df.select(
                [
                    "order_key",
                    "order_date",
                    "order_source_id",
                    "courier_id",
                    "status_id",
                ]
            ),
            on="order_key",
            how="left",
        )
        order_product = order_product.with_columns(
            (pl.col("quantity") * pl.col("price")).round(2).alias("total_amount")
        )

        orders_upload = (
            order_product.group_by("order_key")
            .agg(
                [
                    pl.first("client_id").alias("client_id"),
                    pl.first("order_date").alias("order_date"),
                    pl.first("order_source_id").alias("order_source_id"),
                    pl.sum("total_amount").alias("total_amount"),
                ]
            )
            .sort("order_key")
        )

        # Insert orders to get DB ids and map them back to order_key.
        order_rows = orders_upload.drop("order_key").to_dicts()
        inserted_ids = insert_orders_returning_ids(order_rows, config.schema)

        order_key_series = orders_upload.get_column("order_key")
        order_id_map = pl.DataFrame(
            {"order_key": order_key_series, "order_id": inserted_ids}
        )

        orders_with_ids = orders_df.join(order_id_map, on="order_key")
        order_product = order_product.join(order_id_map, on="order_key")

        orders_product_upload = order_product.select(
            ["order_id", "product_id", "quantity"]
        )
        upload_new_data(orders_product_upload, "order_product", config.schema)

        # Step 2: add initial delivery_tracking and status history input.
        # The tracking rows are placeholders; status history drives updates later.
        delivery_tracking_init = orders_with_ids.select(
            ["order_id", "courier_id"]
        ).unique()
        upload_new_data(
            delivery_tracking_init,
            "delivery_tracking",
            config.schema,
            yesterday=yesterday.isoformat(),
            delivered_status_id=delivered_status_id,
        )

        order_status_history_frames.append(
            orders_with_ids.select(
                ["order_id", "courier_id", "order_date", "status_id"]
            )
        )
    else:
        print("No new orders generated.")

    if yesterday_orders_df.height:
        # Include yesterday's unfinished orders for status completion.
        order_status_history_frames.append(yesterday_orders_df)

    if order_status_history_frames:
        # Step 3: generate order status history for today + yesterday unfinished.
        order_status_history_input = pl.concat(
            order_status_history_frames, how="vertical"
        )
        order_status_history = generate_order_status_history(
            order_status_history_input, today, status_map, config
        )
    else:
        order_status_history_input = pl.DataFrame()
        order_status_history = pl.DataFrame()

    if order_status_history.height:
        # Step 4: generate delivery tracking from status history.
        upload_new_data(
            order_status_history,
            "order_status_history",
            config.schema,
            yesterday=yesterday.isoformat(),
            delivered_status_id=delivered_status_id,
        )

        delivered = order_status_history.filter(
            pl.col("order_status_id") == delivered_status_id
        ).select(
            [
                "order_id",
                pl.col("status_datetime").alias("delivered_time"),
            ]
        )
        packing_id = status_map["packing"]
        packing = order_status_history.filter(
            pl.col("order_status_id") == packing_id
        ).select(
            [
                "order_id",
                pl.col("status_datetime").alias("packing_time"),
            ]
        )

        # Combine delivered + packing times; add courier id from input if available.
        delivery_tracking_input = delivered.join(packing, on="order_id", how="left")
        if order_status_history_input.height:
            courier_lookup = order_status_history_input.select(
                ["order_id", "courier_id"]
            ).unique()
            delivery_tracking_input = delivery_tracking_input.join(
                courier_lookup, on="order_id", how="left"
            )
        delivery_tracking_upload = generate_delivery_tracking(
            delivery_tracking_input, config
        )
        if delivery_tracking_upload.height:
            upload_new_data(
                delivery_tracking_upload,
                "delivery_tracking",
                config.schema,
                yesterday=yesterday.isoformat(),
                delivered_status_id=delivered_status_id,
            )

    # Step 5: generate and upload payments.
    payments_upload = generate_payments(
        today, yesterday, config.schema, delivered_status_id, config
    )
    if payments_upload.height:
        upload_new_data(payments_upload, "payments", config.schema)
