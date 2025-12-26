from __future__ import annotations

import argparse
import logging
from datetime import datetime

from order_data_generator.config import apply_overrides, load_config
from order_data_generator.logging_utils import APP_NAME, setup_logging
from order_data_generator.main import run_generation, set_seed


def _parse_date(value: str):
    return datetime.strptime(value, "%Y-%m-%d").date()


def parse_args():
    parser = argparse.ArgumentParser(description="Run daily source data generator")
    # Sets "today" for generation; affects order_date, statuses, and payments.
    parser.add_argument("--run-date", type=_parse_date, help="YYYY-MM-DD")
    # Fixes RNG to make output deterministic for the same inputs.
    parser.add_argument("--seed", type=int, help="Random seed for deterministic output")
    # Overrides average weekly orders per client (daily rate is avg/7).
    parser.add_argument("--avg-orders", type=float)
    # Lower bound for per-client orders.
    parser.add_argument("--min-orders", type=int)
    # Upper bound for per-client orders.
    parser.add_argument("--max-orders", type=int)
    # Overrides average products per order.
    parser.add_argument("--avg-products", type=float)
    # Lower bound for products per order.
    parser.add_argument("--min-products", type=int)
    # Upper bound for products per order.
    parser.add_argument("--max-products", type=int)
    # Lower bound for unit-quantity products.
    parser.add_argument("--min-units", type=int)
    # Upper bound for unit-quantity products.
    parser.add_argument("--max-units", type=int)
    # Lower bound for weight-quantity products.
    parser.add_argument("--min-weight", type=float)
    # Upper bound for weight-quantity products.
    parser.add_argument("--max-weight", type=float)
    # Override new-client rate lower bound.
    parser.add_argument("--new-clients-rate-min", type=float)
    # Override new-client rate upper bound.
    parser.add_argument("--new-clients-rate-max", type=float)
    # Override churn rate lower bound.
    parser.add_argument("--churn-rate-min", type=float)
    # Override churn rate upper bound.
    parser.add_argument("--churn-rate-max", type=float)

    return parser.parse_args()


def main() -> None:
    setup_logging()
    logger = logging.getLogger(APP_NAME)
    logger.info("Starting order data generator")

    args = parse_args()
    config = load_config()

    overrides = {
        "avg_orders": args.avg_orders,
        "min_orders": args.min_orders,
        "max_orders": args.max_orders,
        "avg_products": args.avg_products,
        "min_products": args.min_products,
        "max_products": args.max_products,
        "min_units": args.min_units,
        "max_units": args.max_units,
        "min_weight": args.min_weight,
        "max_weight": args.max_weight,
    }

    effective_overrides = {key: value for key, value in overrides.items() if value is not None}

    if args.new_clients_rate_min is not None or args.new_clients_rate_max is not None:
        new_min = (
            args.new_clients_rate_min
            if args.new_clients_rate_min is not None
            else config.new_clients_rates[0]
        )
        new_max = (
            args.new_clients_rate_max
            if args.new_clients_rate_max is not None
            else config.new_clients_rates[1]
        )
        overrides["new_clients_rates"] = (new_min, new_max)
        effective_overrides["new_clients_rates"] = (new_min, new_max)

    if args.churn_rate_min is not None or args.churn_rate_max is not None:
        churn_min = (
            args.churn_rate_min
            if args.churn_rate_min is not None
            else config.churn_rates[0]
        )
        churn_max = (
            args.churn_rate_max
            if args.churn_rate_max is not None
            else config.churn_rates[1]
        )
        overrides["churn_rates"] = (churn_min, churn_max)
        effective_overrides["churn_rates"] = (churn_min, churn_max)

    if effective_overrides:
        logger.info("Applying overrides: %s", effective_overrides)

    config = apply_overrides(config, **overrides)

    if args.seed is not None:
        set_seed(args.seed)
        logger.info("Using random seed=%s", args.seed)

    run_date = args.run_date or datetime.utcnow().date()
    logger.info("Running generator for date=%s", run_date)
    run_generation(run_date, config)
    logger.info("Order data generator completed")


if __name__ == "__main__":
    main()
