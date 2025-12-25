from __future__ import annotations

import os
from dataclasses import dataclass, replace
from typing import Iterable


def _get_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return int(value)


def _get_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return float(value)


def _get_str(name: str, default: str) -> str:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return value


def _get_list(
    name: str, default: Iterable[float], cast: type[float] | type[int] = float
) -> tuple:
    value = os.getenv(name)
    if value is None or value == "":
        return tuple(default)
    return tuple(cast(item.strip()) for item in value.split(",") if item.strip() != "")


@dataclass(frozen=True)
class GeneratorConfig:
    schema: str
    source_db_name: str
    source_db_host: str
    source_db_port: int
    project_start_date: str
    n_orders_days: int
    churn_rates: tuple[float, float]
    new_clients_rates: tuple[float, float]
    avg_orders: float
    min_orders: int
    max_orders: int
    avg_products: float
    min_products: int
    max_products: int
    min_units: int
    max_units: int
    min_weight: float
    max_weight: float
    dt_p1: tuple[float, float]
    dt_p2: tuple[float, float]
    os_p1: tuple[float, float]
    os_p2: tuple[float, float]
    ord_source_p: tuple[float, float, float]
    courier_hours: int
    courier_clock_delta: tuple[int, int]
    order_cycle_minutes: tuple[int, int]
    time_between_statuses: tuple[int, int]
    eod_orders_time: int
    pmt_type_p: tuple[float, float, float, float]
    prepayment_p: tuple[float, float]
    pickup_timing: tuple[int, int]


def load_config() -> GeneratorConfig:
    return GeneratorConfig(
        # Target schema for generated tables.
        schema=_get_str("SCHEMA_INIT", "source"),
        # Database connection parameters for source database.
        source_db_name=_get_str("PGDATABASE", "ampere_db"),
        source_db_host=_get_str("PGHOST", "postgres-service"),
        source_db_port=_get_int("PGPORT", 5432),
        # Base date for project timeline; drives synthetic history offsets.
        project_start_date=_get_str("PROJECT_START_DATE", "2025-08-01"),
        # How many days of orders to generate in backfill mode.
        n_orders_days=_get_int("N_ORDERS_DAYS", 10),
        # Daily churn rates range; higher means more clients marked churned.
        churn_rates=_get_list("CHURN_RATES", (0.004, 0.007)),
        # Daily new-client rates range; higher means more new registrations.
        new_clients_rates=_get_list("NEW_CLIENTS_RATES", (0.006, 0.01)),
        # Avg orders per client (weekly rate; divided by 7 for daily volume).
        avg_orders=_get_float("AVG_ORDERS", 1.8),
        # Bounds for per-client order count sampling.
        min_orders=_get_int("MIN_ORDERS", 1),
        max_orders=_get_int("MAX_ORDERS", 4),
        # Avg products per order; higher increases order lines.
        avg_products=_get_float("AVG_PRODUCTS", 10),
        # Bounds for products per order.
        min_products=_get_int("MIN_PRODUCTS", 5),
        max_products=_get_int("MAX_PRODUCTS", 20),
        # Bounds for unit-based product quantities.
        min_units=_get_int("MIN_UNITS", 1),
        max_units=_get_int("MAX_UNITS", 5),
        # Bounds for weight-based product quantities.
        min_weight=_get_float("MIN_WEIGHT", 0.100),
        max_weight=_get_float("MAX_WEIGHT", 4.000),
        # Delivery type probability ranges (p1, p2; p3 is derived).
        dt_p1=_get_list("DT_P1", (0.45, 0.55)),
        dt_p2=_get_list("DT_P2", (0.30, 0.40)),
        # Order status probability ranges for created and delivered.
        os_p1=_get_list("OS_P1", (0.005, 0.01)),
        os_p2=_get_list("OS_P2", (0.97, 0.98)),
        # Order source distribution parameters; shapes source_id frequency.
        ord_source_p=_get_list("ORD_SOURCE_P", (2, 1, 3)),
        # Courier shift start hour; shifts status timestamps earlier/later.
        courier_hours=_get_int("COURIER_HOURS", 8),
        # Random offset added to courier start time in minutes.
        courier_clock_delta=_get_list("COURIER_CLOCK_DELTA", (0, 16), int),
        # Total order lifecycle time (minutes) for status progression.
        order_cycle_minutes=_get_list("ORDER_CYCLE_MINUTES", (45, 55), int),
        # Gap between orders for the same courier.
        time_between_statuses=_get_list("TIME_BETWEEN_STATUSES", (8, 15), int),
        # Hour for end-of-day incomplete orders.
        eod_orders_time=_get_int("EOD_ORDERS_TIME", 19),
        # Payment method probabilities.
        pmt_type_p=_get_list("PMT_TYPE_P", (0.6, 0.2, 0.05, 0.15)),
        # Prepayment vs after-payment probabilities.
        prepayment_p=_get_list("PREPAYMENT_P", (0.2, 0.8)),
        # Delivery tracking pickup time offsets.
        pickup_timing=_get_list("PICKUP_TIMING", (5, 15), int),
    )


def apply_overrides(config: GeneratorConfig, **overrides) -> GeneratorConfig:
    clean_overrides = {
        key: value for key, value in overrides.items() if value is not None
    }
    if not clean_overrides:
        return config
    return replace(config, **clean_overrides)
