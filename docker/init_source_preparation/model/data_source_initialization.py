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
    """Load a seed CSV file that represents a static dictionary table.

    Dictionary tables such as stores, delivery types, and product categories
    live in the package so the bootstrap image is self-contained. Returning a
    Polars DataFrame lets the same upload helper insert both static dictionary
    rows and generated rows through one common code path.
    """
    dictionaries = files("init_source_preparation.dictionaries")
    with as_file(dictionaries / f"{table_name}.csv") as path_to_dict:
        return pl.read_csv(path_to_dict)


def initialize_data_sources() -> None:
    """Create the source schema and populate every source table in bootstrap order.

    This is the heart of the one-time initialization phase. It creates tables
    when missing, truncates reusable tables when they already exist, loads CSV
    dictionaries, generates synthetic starter entities, and finally applies
    indexes so the later daily generator and Spark extract jobs see a ready-made
    PostgreSQL source schema.
    """
    ensure_schema(schema_init)

    # Walk through the declarative table plan from ddl_init so the bootstrap
    # logic stays data-driven instead of hardcoding per-table SQL inline.
    for table_name, data in table_queries.items():
        logger.info("Starting work on %s", table_name)

        query = data["query"]
        table_type = data["type"]

        if table_type == "index":
            # Indexes are applied after tables exist and after data is present.
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
            # Initial clients create the base population that the daily generator
            # will later churn, grow, and attach orders to.
            logger.info("Generating clients")
            df_clients = generate_clients(
                n=n_of_init_clients, project_start_date=project_start_date
            )
            upload_new_data(df_clients, table_name, schema_init)

        if table_name == "assortment":
            # Assortment links stores to the products they are allowed to sell.
            logger.info("Generating assortment")
            df_assortment = generate_assortment()
            upload_new_data(df_assortment, table_name, schema_init)

        if table_name == "delivery_resource":
            # Delivery resources represent the courier fleet that will later be
            # assigned to orders during daily generation.
            logger.info("Generating delivery resources")
            df_delivery_resource = generate_delivery_resource(
                n_delivery_resource, project_start_date=project_start_date
            )
            upload_new_data(df_delivery_resource, table_name, schema_init)
