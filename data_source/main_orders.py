from datetime import datetime, timedelta

from generators.orders_gen import (
    prepare_orders_statuses,
)
from generators.utils import today

today = datetime.strptime("2025-06-01", "%Y-%m-%d").date()
yesterday = today - timedelta(days=1)


for i in range(0, 10):
    print("Generation for " + str(today) + " and " + str(yesterday))
    prepare_orders_statuses(today, yesterday)

    today = today + timedelta(days=1)
    yesterday = today - timedelta(days=1)
