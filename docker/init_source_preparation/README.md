# init_source_preparation

Standalone batch application that bootstraps the Source (Pre-Raw) schema in PostgreSQL and loads initial dictionaries plus core seed data.

## Runtime configuration (env vars)

- `DATABASE_URL`: full SQLAlchemy/psycopg URL (recommended)
- `PGUSER`/`pguser` and `PGPASSWORD`/`pgpass`: credentials for PostgreSQL
- `PGHOST`, `PGPORT`, `PGDATABASE`: optional overrides (defaults live in `config.py`)
- `SCHEMA_INIT`: target schema (default: `source`)
- `N_OF_INIT_CLIENTS`: number of clients to generate (default: `20000`)
- `N_DELIVERY_RESOURCE`: number of delivery resources (default: `700`)
- `PROJECT_START_DATE`: YYYY-MM-DD base date (default: current date)

## Runtime shape

The application can run as a Python module in a local environment or as the container image used by Airflow. The same environment variables define database connectivity and generation volume in both modes.

## Airflow

Airflow DAGs use this image with the same environment variable contract.

## Image tagging (GitHub Actions)

Image version comes from the shared repo release tag `v*`.
Branch pushes rebuild this image only when `docker/init_source_preparation/**` changes.
Airflow selects the runtime tag through variable `ampere_release_version`.

## Autovacuum and indexing notes

Indexes are created during init (see `ddl_init.py`). PostgreSQL autovacuum and
occasional database-level maintenance handle ongoing table and index upkeep.
