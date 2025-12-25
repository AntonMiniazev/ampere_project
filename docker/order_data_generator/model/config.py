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
        schema=_get_str("SCHEMA_INIT", "source"),
        source_db_name=_get_str("PGDATABASE", "ampere_db"),
        source_db_host=_get_str("PGHOST", "postgres-service"),
        source_db_port=_get_int("PGPORT", 5432),
        project_start_date=_get_str("PROJECT_START_DATE", "2025-08-01"),
        n_orders_days=_get_int("N_ORDERS_DAYS", 10),
        churn_rates=_get_list("CHURN_RATES", (0.004, 0.007)),
        new_clients_rates=_get_list("NEW_CLIENTS_RATES", (0.006, 0.01)),
        avg_orders=_get_float("AVG_ORDERS", 1.4),
        min_orders=_get_int("MIN_ORDERS", 1),
        max_orders=_get_int("MAX_ORDERS", 4),
        avg_products=_get_float("AVG_PRODUCTS", 10),
        min_products=_get_int("MIN_PRODUCTS", 5),
        max_products=_get_int("MAX_PRODUCTS", 20),
        min_units=_get_int("MIN_UNITS", 1),
        max_units=_get_int("MAX_UNITS", 5),
        min_weight=_get_float("MIN_WEIGHT", 0.100),
        max_weight=_get_float("MAX_WEIGHT", 4.000),
        dt_p1=_get_list("DT_P1", (0.45, 0.55)),
        dt_p2=_get_list("DT_P2", (0.30, 0.40)),
        os_p1=_get_list("OS_P1", (0.005, 0.01)),
        os_p2=_get_list("OS_P2", (0.97, 0.98)),
        ord_source_p=_get_list("ORD_SOURCE_P", (2, 1, 3)),
        courier_hours=_get_int("COURIER_HOURS", 8),
        courier_clock_delta=_get_list("COURIER_CLOCK_DELTA", (0, 16), int),
        order_cycle_minutes=_get_list("ORDER_CYCLE_MINUTES", (45, 55), int),
        time_between_statuses=_get_list("TIME_BETWEEN_STATUSES", (8, 15), int),
        eod_orders_time=_get_int("EOD_ORDERS_TIME", 19),
        pmt_type_p=_get_list("PMT_TYPE_P", (0.6, 0.2, 0.05, 0.15)),
        prepayment_p=_get_list("PREPAYMENT_P", (0.2, 0.8)),
        pickup_timing=_get_list("PICKUP_TIMING", (5, 15), int),
    )


def apply_overrides(config: GeneratorConfig, **overrides) -> GeneratorConfig:
    clean_overrides = {
        key: value for key, value in overrides.items() if value is not None
    }
    if not clean_overrides:
        return config
    return replace(config, **clean_overrides)
