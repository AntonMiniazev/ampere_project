from importlib.resources import as_file, files

import polars as pl

from init_source_preparation.config import (
    n_delivery_resource,
    n_of_init_clients,
    project_start_date,
    schema_init,
)
from init_source_preparation.db import (
    ensure_schema,
    exec_sql,
    table_exists,
    truncate_table,
    upload_new_data,
)
from init_source_preparation.ddl_init import table_queries
from init_source_preparation.generators.assortment_gen import generate_assortment
from init_source_preparation.generators.clients_gen_init import generate_clients
from init_source_preparation.generators.delivery_resource_gen_init import (
    generate_delivery_resource,
)


def _load_dictionary(table_name: str) -> pl.DataFrame:
    dictionaries = files("init_source_preparation.dictionaries")
    with as_file(dictionaries / f"{table_name}.csv") as path_to_dict:
        return pl.read_csv(path_to_dict)


def initialize_data_sources() -> None:
    ensure_schema(schema_init)

    for table_name, data in table_queries.items():
        print(f">>>>>>>>>> Starting work on {table_name}")

        query = data["query"]
        table_type = data["type"]

        if table_type == "index":
            print(f"Ensuring indexes for {schema_init}")
            exec_sql(query)
            continue

        if table_exists(schema_init, table_name):
            print(f"Truncating {table_name}")
            truncate_table(schema_init, table_name)
        else:
            print(f"Creating {table_name}")
            exec_sql(query)

        if table_type == "dict":
            df_dict = _load_dictionary(table_name)
            upload_new_data(df_dict, table_name, schema_init)

        if table_name == "clients":
            df_clients = generate_clients(
                n=n_of_init_clients, project_start_date=project_start_date
            )
            upload_new_data(df_clients, table_name, schema_init)

        if table_name == "assortment":
            df_assortment = generate_assortment()
            upload_new_data(df_assortment, table_name, schema_init)

        if table_name == "delivery_resource":
            df_delivery_resource = generate_delivery_resource(n_delivery_resource)
            upload_new_data(df_delivery_resource, table_name, schema_init)
