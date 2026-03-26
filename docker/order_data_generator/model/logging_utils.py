import logging
import sys

APP_NAME = "order-generator"


def setup_logging(level: str = "INFO") -> None:
    """Configure stdout logging for the daily generator container.

    This application is usually launched by Airflow inside Kubernetes, where
    pod logs are the first debugging surface for generation failures. Keeping
    logging setup in one helper makes every module emit the same timestamped
    format and resets any inherited handler state from the runtime image.
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
