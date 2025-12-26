import logging

from init_source_preparation.config import (
    n_delivery_resource,
    n_of_init_clients,
    schema_init,
    source_db_host,
    source_db_name,
    source_db_port,
)
from init_source_preparation.data_source_initialization import initialize_data_sources
from init_source_preparation.logging_utils import APP_NAME, setup_logging


def main() -> None:
    setup_logging()
    logger = logging.getLogger(APP_NAME)
    logger.info(
        "Starting init source preparation (schema=%s, clients=%s, delivery_resources=%s)",
        schema_init,
        n_of_init_clients,
        n_delivery_resource,
    )
    logger.info(
        "Configured Postgres target host=%s port=%s db=%s",
        source_db_host,
        source_db_port,
        source_db_name,
    )
    initialize_data_sources()
    logger.info("Init source preparation completed")


if __name__ == "__main__":
    main()
