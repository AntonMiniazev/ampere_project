import os
from datetime import date


def _get_int(name: str, default: int) -> int:
    """Read an integer setting from the environment with a safe fallback.

    Bootstrap containers are configured through environment variables injected
    by Kubernetes/Airflow. This helper keeps that wiring simple by converting
    blank or missing values into a predictable default instead of letting the
    rest of the code worry about parsing edge cases.
    """
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return int(value)


def _get_str(name: str, default: str) -> str:
    """Read a string setting from the environment with a safe fallback.

    The init pipeline uses this for schema names, hostnames, and dates. Treating
    empty strings as "not set" keeps local runs and Kubernetes runs consistent,
    which makes the config easier to explain and reason about.
    """
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return value


# Target schema for init tables; changing it isolates where data is created.
schema_init = _get_str("SCHEMA_INIT", "source")
# Initial client count; higher values increase generated clients
n_of_init_clients = _get_int("N_OF_INIT_CLIENTS", 20000)
# Initial courier pool size; drives rows in source.delivery_resource.
n_delivery_resource = _get_int("N_DELIVERY_RESOURCE", 700)
# Base date for generated registrations; shifts client registration_date backward.
project_start_date = _get_str("PROJECT_START_DATE", date.today().isoformat())
# Postgres database name; selects which database is seeded.
source_db_name = _get_str("PGDATABASE", "ampere_db")
# Postgres host; controls where init tables are created.
source_db_host = _get_str("PGHOST", "postgres-service")
# Postgres port; controls connection target.
source_db_port = _get_int("PGPORT", 5432)
