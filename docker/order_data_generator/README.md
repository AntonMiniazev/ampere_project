# order_data_generator

Standalone batch application that generates daily Source (Pre-Raw) data in PostgreSQL: client churn + new clients, orders, order products, statuses, delivery tracking, and payments.

## Runtime configuration (env vars)

Database:
- `DATABASE_URL`: full SQLAlchemy/psycopg URL (recommended)
- `PGHOST`, `PGPORT`, `PGDATABASE`: optional overrides (defaults live in `config.py`)
- `PGUSER`/`pguser` and `PGPASSWORD`/`pgpass`: credentials for PostgreSQL

Generator:
- `SCHEMA_INIT`: target schema (default: `source`)
- `PROJECT_START_DATE`, `N_ORDERS_DAYS`, `CHURN_RATES`, `NEW_CLIENTS_RATES`,
  `AVG_ORDERS`, `MIN_ORDERS`, `MAX_ORDERS`, `AVG_PRODUCTS`, `MIN_PRODUCTS`, `MAX_PRODUCTS`,
  `MIN_UNITS`, `MAX_UNITS`, `MIN_WEIGHT`, `MAX_WEIGHT`,
  `DT_P1`, `DT_P2`, `OS_P1`, `OS_P2`,
  `ORD_SOURCE_P`, `COURIER_HOURS`, `COURIER_CLOCK_DELTA`, `ORDER_CYCLE_MINUTES`,
  `TIME_BETWEEN_STATUSES`, `EOD_ORDERS_TIME`,
  `PMT_TYPE_P`, `PREPAYMENT_P`, `PICKUP_TIMING`.

List values are comma-separated, e.g. `CHURN_RATES=0.004,0.007`.
`AVG_ORDERS` is interpreted as a weekly per-client rate and divided by 7 for daily volume.

## Runtime shape

The generator can run as a Python module in a local environment or as the container image used by Airflow. Runtime arguments control the generation date, deterministic seed, and optional volume overrides; environment variables define PostgreSQL connectivity and default generation settings.

## Image tagging (GitHub Actions)

Image version comes from the shared repo release tag `v*`.
Branch pushes rebuild this image only when `docker/order_data_generator/**` changes.
Airflow selects the runtime tag through variable `ampere_release_version`.

## Autovacuum and indexing notes

Indexes are created once during init and maintained by PostgreSQL. Reindexing
after every generator run is not part of the normal design. `pg_stat_user_tables`
and `pg_stat_all_indexes` provide bloat and index-usage signals for occasional
database maintenance.

