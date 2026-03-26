import logging
import sys

APP_NAME = "init-source-preparation"


def setup_logging(level: str = "INFO") -> None:
    """Configure consistent stdout logging for the bootstrap container.

    The init job runs as a short-lived Kubernetes pod, so stdout is the main
    place where Airflow and kubectl users inspect what happened. A single
    formatting helper keeps timestamps and logger names consistent across all
    bootstrap modules while forcing a clean logging state on process start.
    """
    logging.basicConfig(
        level=level,
        format=("%(asctime)s | %(levelname)s | %(name)s | %(message)s"),
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout),
        ],
        force=True,
    )
