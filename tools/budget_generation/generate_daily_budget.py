"""Generate daily, per-store budget rows from monthly budget input CSV."""

from __future__ import annotations

import csv
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable


# Default inputs requested in AGENTS Step 5.
INPUT_FILE = Path(__file__).with_name("budget_parameters.csv")
OUTPUT_FILE = Path(__file__).with_name("budget_parameters_daily.csv")
START_DATE_STR = "16/12/2025"
STORE_SPLIT = ["0.2", "0.2", "0.2", "0.2", "0.2"]
STORE_IDS = ["1", "2", "3", "4", "5"]
BUDGET_NAME = "Budget_FY26_v1"


def _parse_number(value: str) -> float:
    """Convert values like '80,000' to float."""
    return float(value.replace(",", "").strip())


def _parse_month_start(value: str) -> date:
    """Parse monthly budget key in d/m/Y format."""
    return datetime.strptime(value.strip(), "%d/%m/%Y").date()


def _month_last_day(month_start: date) -> date:
    """Get the last calendar day of the same month."""
    if month_start.month == 12:
        return date(month_start.year + 1, 1, 1) - timedelta(days=1)
    return date(month_start.year, month_start.month + 1, 1) - timedelta(days=1)


def _date_range(start_day: date, end_day: date) -> Iterable[date]:
    """Yield all calendar days from start to end inclusive."""
    current = start_day
    while current <= end_day:
        yield current
        current += timedelta(days=1)


def _allocate_integers(total: int, weights: list[float]) -> list[int]:
    """Allocate integer counts by weight while preserving the exact total."""
    raw = [total * weight for weight in weights]
    base = [int(value) for value in raw]
    remainder = total - sum(base)
    ranked = sorted(
        enumerate(raw),
        key=lambda item: (item[1] - int(item[1]), -item[0]),
        reverse=True,
    )
    for idx, _ in ranked[:remainder]:
        base[idx] += 1
    return base


def _scaled_int(value: float, scale: int) -> int:
    """Convert float to scaled integer using rounding."""
    return int(round(value * scale))


def build_daily_budget() -> None:
    """Expand monthly budget into daily rows and split each day by store."""
    start_date = datetime.strptime(START_DATE_STR, "%d/%m/%Y").date()
    store_weights = [float(weight) for weight in STORE_SPLIT]

    if len(store_weights) != len(STORE_IDS):
        raise ValueError("STORE_SPLIT and STORE_IDS must have the same length.")
    if abs(sum(store_weights) - 1.0) > 1e-9:
        raise ValueError("STORE_SPLIT must sum to 1.0.")

    output_rows: list[dict[str, str]] = []
    with INPUT_FILE.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            month_start = _parse_month_start(row["calendar_month"])
            month_end = _month_last_day(month_start)
            effective_start = max(month_start, start_date)
            if effective_start > month_end:
                continue

            total_orders = int(round(_parse_number(row["orders_budget"])))
            total_sales = _parse_number(row["sales_amount"])

            active_days = (month_end - effective_start).days + 1
            sales_scale = 1_000_000
            store_month_orders = _allocate_integers(total_orders, store_weights)
            total_sales_scaled = _scaled_int(total_sales, sales_scale)
            store_month_sales_scaled = _allocate_integers(
                total_sales_scaled, store_weights
            )

            # Create one row per active day and per store for downstream loading.
            for (
                store_id,
                store_weight,
                monthly_orders_for_store,
                monthly_sales_for_store_scaled,
            ) in zip(
                STORE_IDS,
                store_weights,
                store_month_orders,
                store_month_sales_scaled,
            ):
                per_day_orders = _allocate_integers(
                    monthly_orders_for_store, [1.0 / active_days] * active_days
                )
                per_day_sales_scaled = _allocate_integers(
                    monthly_sales_for_store_scaled, [1.0 / active_days] * active_days
                )
                for day_index, budget_day in enumerate(
                    _date_range(effective_start, month_end)
                ):
                    store_daily_orders = per_day_orders[day_index]
                    store_daily_sales = per_day_sales_scaled[day_index] / sales_scale
                    output_rows.append(
                        {
                            "budget_name": BUDGET_NAME,
                            "budget_date": budget_day.strftime("%Y-%m-%d"),
                            "store_id": store_id,
                            "orders_budget_daily": str(int(round(store_daily_orders))),
                            "sales_amount_daily": f"{store_daily_sales:.6f}",
                        }
                    )

    with OUTPUT_FILE.open("w", encoding="utf-8", newline="") as handle:
        fieldnames = [
            "budget_name",
            "budget_date",
            "store_id",
            "orders_budget_daily",
            "sales_amount_daily",
        ]
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(output_rows)

    print(f"Generated {len(output_rows)} rows: {OUTPUT_FILE}")


if __name__ == "__main__":
    build_daily_budget()
