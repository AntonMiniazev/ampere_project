import random

import polars as pl
from faker import Faker

fake = Faker()


def generate_delivery_resource(
    n: int = 100, store_id_range: tuple[int, int] = (1, 5)
) -> pl.DataFrame:
    delivery_type_choices = [1, 2, 3]
    delivery_type_weights = [0.5, 0.35, 0.15]

    ids = list(range(1, n + 1))
    fullnames = [f"{fake.first_name()} {fake.last_name()}" for _ in range(n)]
    delivery_type_ids = [
        random.choices(delivery_type_choices, weights=delivery_type_weights, k=1)[0]
        for _ in range(n)
    ]
    store_ids = [random.randint(*store_id_range) for _ in range(n)]
    active_flags = [True] * n

    return pl.DataFrame(
        {
            "id": ids,
            "fullname": fullnames,
            "delivery_type_id": delivery_type_ids,
            "store_id": store_ids,
            "active_flag": active_flags,
        }
    )
