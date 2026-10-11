from importlib.resources import as_file, files
import logging
import os
from datetime import date

import polars as pl
from sqlalchemy import text

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
from init_source_preparation.db import get_engine
from init_source_preparation.source_journal import (
    drop_source_capture_triggers,
    initialize_dataset_journal,
    migrate_existing_source_journal,
)

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
    project_date = date.fromisoformat(project_start_date)
    engine = get_engine()
    with engine.begin() as connection:
        # Init and generation share this lock; a reset cannot race an active
        # source-writing pod. Existing triggers allow this explicit reset path.
        connection.execute(text("SELECT pg_advisory_xact_lock(:lock_key)"), {"lock_key": 724614902631})
        ensure_schema(schema_init, connection=connection)

        # Keep reset, seed rows, baseline journal, and dataset activation in one
        # transaction. A failed init rolls back rather than leaving a half-ready
        # source dataset that the generator could extend.
        for table_name, data in table_queries.items():
            logger.info("Starting work on %s", table_name)
            query = data["query"]
            table_type = data["type"]

            if table_type == "index":
                logger.info("Ensuring indexes for %s", schema_init)
                exec_sql(query, connection=connection)
                continue

            if table_exists(schema_init, table_name, connection=connection):
                # Reset is the only unjournaled source write path. Remove the
                # capture trigger before truncating and bulk-loading the table;
                # the new dataset's baseline is journaled after all loads finish.
                drop_source_capture_triggers(connection, schema_init, table_name)
                logger.info("Truncating %s", table_name)
                truncate_table(schema_init, table_name, connection=connection)
            else:
                logger.info("Creating %s", table_name)
                exec_sql(query, connection=connection)

            if table_type == "dict":
                logger.info("Loading dictionary data for %s", table_name)
                upload_new_data(_load_dictionary(table_name), table_name, schema_init, connection=connection)

            if table_name == "clients":
                logger.info("Generating clients")
                upload_new_data(
                    generate_clients(n=n_of_init_clients, project_start_date=project_start_date),
                    table_name,
                    schema_init,
                    connection=connection,
                )

            if table_name == "assortment":
                logger.info("Generating assortment")
                upload_new_data(generate_assortment(), table_name, schema_init, connection=connection)

            if table_name == "delivery_resource":
                logger.info("Generating delivery resources")
                upload_new_data(
                    generate_delivery_resource(n_delivery_resource, project_start_date=project_start_date),
                    table_name,
                    schema_init,
                    connection=connection,
                )

        dataset_id = initialize_dataset_journal(
            connection,
            schema_init,
            project_date,
            source_code_version=os.getenv("AMPERE_SOURCE_CODE_VERSION", "unknown"),
        )
        logger.info("Initialized source dataset_id=%s start_date=%s", dataset_id, project_date)


def migrate_source_journal() -> str:
    """Install transactional journal capture without resetting source data."""
    project_date = date.fromisoformat(project_start_date)
    engine = get_engine()
    with engine.begin() as connection:
        connection.execute(
            text("SELECT pg_advisory_xact_lock(:lock_key)"),
            {"lock_key": 724614902631},
        )
        dataset_id = migrate_existing_source_journal(
            connection,
            schema_init,
            project_date,
            source_code_version=os.getenv("AMPERE_SOURCE_CODE_VERSION", "unknown"),
        )
        logger.info("Journal migration ready dataset_id=%s", dataset_id)
        return dataset_id
