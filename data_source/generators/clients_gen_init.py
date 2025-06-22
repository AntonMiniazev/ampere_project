import os
import random
from datetime import datetime, timedelta

from faker import Faker

script_dir = os.path.dirname(os.path.abspath(__file__))

fake = Faker()


def generate_clients(n=100, store_id_range=(1, 5)):
    today = datetime.today()
    clients = []

    for i in range(1, n + 1):
        fullname = f"{fake.first_name()} {fake.last_name()}"
        preferred_store_id = random.randint(*store_id_range)
        days_ago = random.randint(0, 14)
        registration_date = today - timedelta(days=days_ago)

        clients.append(
            {
                "id": i,
                "fullname": fullname,
                "preferred_store_id": preferred_store_id,
                "registration_date": registration_date.strftime("%Y-%m-%d"),
            }
        )

    return clients


clients = generate_clients(4000)

# with open(os.path.join(script_dir, 'clients_init.csv'), 'w', newline='', encoding='utf-8') as f:
#    writer = csv.DictWriter(f, fieldnames=['id', 'fullname', 'preferred_store_id', 'registration_date'])
#    writer.writeheader()
#    writer.writerows(clients)
print(clients)
print(type(clients[0]))
