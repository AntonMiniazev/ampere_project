import logging

from init_source_preparation.config import load_policy
from init_source_preparation.data_source_initialization import initialize_data_sources
from init_source_preparation.logging_utils import APP_NAME, setup_logging
from init_source_preparation.settings import BootstrapSettings


def main() -> None:
    setup_logging()
    logger = logging.getLogger(APP_NAME)
    settings = BootstrapSettings()
    policy = load_policy(
        n_of_init_clients=settings.n_of_init_clients,
        n_delivery_resource=settings.n_delivery_resource,
        project_start_date=settings.project_start_date,
    )
    initialize_data_sources(settings, policy)
    logger.info(
        "Source reset and initialization completed for %s", settings.source_schema
    )


if __name__ == "__main__":
    main()
