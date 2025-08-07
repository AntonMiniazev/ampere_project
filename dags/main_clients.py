import pandas as pd
from db.db_io import upload_new_data
from generators.clients_gen import (
    prepare_clients_update_and_generation,
    update_churned,
)
from generators.utils import today

if __name__ == "__main__":
    to_churn_ids, clients_for_upload = prepare_clients_update_and_generation()
    update_churned(to_churn_ids)
    upload_new_data(pd.DataFrame(clients_for_upload), "clients", today)
