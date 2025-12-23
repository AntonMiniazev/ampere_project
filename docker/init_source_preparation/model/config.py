import os


def _get_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return int(value)


def _get_str(name: str, default: str) -> str:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return value


schema_init = _get_str("SCHEMA_INIT", "source")
n_of_init_clients = _get_int("N_OF_INIT_CLIENTS", 4000)
n_delivery_resource = _get_int("N_DELIVERY_RESOURCE", 125)
project_start_date = _get_str("PROJECT_START_DATE", "2025-08-01")
source_db_name = _get_str("PGDATABASE", "ampere_db")
source_db_host = _get_str("PGHOST", "postgres-service")
source_db_port = _get_int("PGPORT", 5432)
