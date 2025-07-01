from faker import Faker
import random
import pandas as pd

fake = Faker()


def generate_delivery_resource(n=100, store_id_range=(1, 5)):
    delivery_resource = []
    delivery_type_choices = [1, 2, 3]
    delivery_type_weights = [0.5, 0.35, 0.15]

    for i in range(1, n + 1):
        fullname = f"{fake.first_name()} {fake.last_name()}"
        delivery_type_id = random.choices(
            delivery_type_choices, weights=delivery_type_weights, k=1
        )[0]

        delivery_resource.append(
            {
                "id": i,
                "fullname": fullname,
                "delivery_type_id": delivery_type_id,
                "store_id": random.randint(*store_id_range),
                "active_flag": True,
            }
        )

    return pd.DataFrame(delivery_resource)
