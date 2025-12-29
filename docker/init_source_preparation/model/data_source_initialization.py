from importlib.resources import as_file, files
import logging

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
from init_source_preparation.logging_utils import APP_NAME

logger = logging.getLogger(APP_NAME)


def _load_dictionary(table_name: str) -> pl.DataFrame:
    dictionaries = files("init_source_preparation.dictionaries")
    with as_file(dictionaries / f"{table_name}.csv") as path_to_dict:
        return pl.read_csv(path_to_dict)


def initialize_data_sources() -> None:
    ensure_schema(schema_init)

    for table_name, data in table_queries.items():
        logger.info("Starting work on %s", table_name)

        query = data["query"]
        table_type = data["type"]

        if table_type == "index":
            logger.info("Ensuring indexes for %s", schema_init)
            exec_sql(query)
            continue

        if table_exists(schema_init, table_name):
            logger.info("Truncating %s", table_name)
            truncate_table(schema_init, table_name)
        else:
            logger.info("Creating %s", table_name)
            exec_sql(query)

        if table_type == "dict":
            logger.info("Loading dictionary data for %s", table_name)
            df_dict = _load_dictionary(table_name)
            upload_new_data(df_dict, table_name, schema_init)

        if table_name == "clients":
            logger.info("Generating clients")
            df_clients = generate_clients(
                n=n_of_init_clients, project_start_date=project_start_date
            )
            upload_new_data(df_clients, table_name, schema_init)

        if table_name == "assortment":
            logger.info("Generating assortment")
            df_assortment = generate_assortment()
            upload_new_data(df_assortment, table_name, schema_init)

        if table_name == "delivery_resource":
            logger.info("Generating delivery resources")
            df_delivery_resource = generate_delivery_resource(
                n_delivery_resource, project_start_date=project_start_date
            )
            upload_new_data(df_delivery_resource, table_name, schema_init)
