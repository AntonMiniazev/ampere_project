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

## CLI usage

```bash
python -m order_data_generator --run-date 2025-12-23 --seed 123
```

Optional volume overrides:

```bash
python -m order_data_generator \
  --run-date 2025-12-23 \
  --avg-orders 1.5 --min-orders 1 --max-orders 5 \
  --avg-products 9 --min-products 4 --max-products 18 \
  --new-clients-rate-min 0.006 --new-clients-rate-max 0.01 \
  --churn-rate-min 0.004 --churn-rate-max 0.007
```

## Docker build/run

```bash
cd docker/order_data_generator
docker build -t order-data-generator:latest .

docker run --rm \
  -e DATABASE_URL="postgresql+psycopg://user:pass@host:5432/ampere_db" \
  -e SCHEMA_INIT=source \
  order-data-generator:latest --run-date 2025-12-23 --seed 123
```

