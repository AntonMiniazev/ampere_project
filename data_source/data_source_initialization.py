# Use this script for initial data source organization
# It checks and creates tables in database, uploads dictionaries, generates first clients and uploads them

from db.ddl_init import table_queries
from db.db_io import (
    table_exists,
    truncate_table,
    exec_sql,
    upload_new_data,
)

from generators.config import (
    schema_init,
    n_of_init_clients,
    n_delivery_resource,
)
from generators.assortment_gen import generate_assortment
from generators.clients_gen_init import generate_clients
from generators.delivery_resource_gen_init import generate_delivery_resource
import importlib.resources
import pandas as pd

# Configuring schema with initial data sources
for table_name, data in table_queries.items():
    print(">>>>>>>>>> Starting work on " + table_name)

    query = data["query"]
    table_type = data["type"]

    if table_exists(schema_init, table_name):
        print("Truncating " + table_name)
        truncate_table(schema_init, table_name)
    else:
        print("Creating " + table_name)
        exec_sql(query)

    if table_type == "dict":
        with importlib.resources.path(
            "dictionaries", table_name + ".csv"
        ) as path_to_dict:
            df_dict = pd.read_csv(path_to_dict)
            upload_new_data(df_dict, table_name)

    if table_name == "clients":
        df_clients = generate_clients(n=n_of_init_clients)
        upload_new_data(df_clients, table_name)

    if table_name == "assortment":
        df_assortment = generate_assortment()
        upload_new_data(df_assortment, table_name)

    if table_name == "delivery_resource":
        df_delivery_resource = generate_delivery_resource(n_delivery_resource)
        upload_new_data(df_delivery_resource, table_name)
