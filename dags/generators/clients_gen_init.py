import random
import pandas as pd
from datetime import datetime, timedelta
from generators.config import project_start_date
from faker import Faker


fake = Faker()


def generate_clients(n=100, store_id_range=(1, 5)):
    today = datetime.strptime(project_start_date, "%Y-%m-%d")
    clients = []

    for i in range(1, n + 1):
        fullname = f"{fake.first_name()} {fake.last_name()}"
        preferred_store_id = random.randint(*store_id_range)
        days_ago = random.randint(0, 14)
        registration_date = today - timedelta(days=days_ago)

        clients.append(
            {
                "fullname": fullname,
                "preferred_store_id": preferred_store_id,
                "registration_date": registration_date.strftime("%Y-%m-%d"),
                "churned": 0,
            }
        )

    return pd.DataFrame(clients)
