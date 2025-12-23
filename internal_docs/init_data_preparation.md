# Initial Data Preparation (Source Layer)

This document explains how the initial source data is prepared and which configs control it. It is written to make refactoring into a GHCR image (triggered in-cluster) straightforward, especially when targeting PostgreSQL.

## Entry Point and Execution Flow

- Airflow DAG: `dags/my_dags/source_preparation_dag.py`
  - `dag_id`: `source_preparation_dag`
  - `schedule`: `None` (manual/one-time run)
  - Task: `initialize_data_sources` (PythonOperator)
- Callable: `dags/generators/data_source_initialization.py::initialize_data_sources`
  1. Iterates over `table_queries` from `dags/db/ddl_init.py`
  2. For each table:
     - If table exists: truncate
     - Else: create via DDL
  3. If table is a dictionary table: load CSV from `dags/dictionaries`
  4. If table is `clients`, `assortment`, or `delivery_resource`: generate data and upload

Note: The routine truncates tables on re-run, so it is destructive and intended for initial bootstrapping.

## Ongoing Generation (After Init)

`dags/my_dags/orders_clients_gen_dag.py` handles daily generation after the initial bootstrap:

- `dag_id`: `orders_clients_generation`
- `schedule`: `0 3 * * *` (daily at 03:00)
- `start_date`: `2025-08-24`
- `catchup`: `False`
- `max_active_runs`: `1`

Tasks:

- `generate_and_update_clients`
  - Uses `prepare_clients_update_and_generation(today)` from `dags/generators/clients_gen.py`
  - Calls `update_churned(...)` to mark churned clients
  - Uploads new clients to `core.clients` via `upload_new_data`
- `generate_orders`
  - Uses `prepare_orders_statuses(today, yesterday)` from `dags/generators/orders_gen.py`
  - Generates orders and statuses for the current and previous day

Downstream trigger:

- Triggers the `source_to_minio` DAG via `TriggerDagRunOperator` once orders are generated.

## Configuration and How It Is Used

### generators/config.py

`dags/generators/config.py` provides the main initialization parameters:

- `schema_init = "core"`
  - Used by DDL and DB operations to target schema.
- `n_of_init_clients = 4000`
  - Number of client rows generated in the init step.
- `n_delivery_resource = 125`
  - Number of delivery resource rows generated.
- `project_start_date = "2025-08-01"`
  - Base date for generating client registration dates.
- `database_init = "source"`
  - Not used by `initialize_data_sources`, but referenced by other DAGs (e.g., source export).

### db/mssql.py and dags/.env

`dags/db/mssql.py` uses `python-dotenv` to load `dags/.env` and builds a SQL Server connection string:

- `SERVER_ADDRESS` (default: `localhost`)
- `MSSQL_DB` (default: `source`)
- `MSSQL_USER` (required)
- `MSSQL_PASSWORD` (required; not in repo)
- `MSSQL_NODE_PORT` (default: `1433`)
- `ODBC_DRIVER` (default: `ODBC Driver 17 for SQL Server`)

The engine is created via SQLAlchemy with `mssql+pyodbc`.

### Table DDL Configuration

`dags/db/ddl_init.py` defines the DDL and table types via `table_queries`. Each entry has:

- `query`: CREATE TABLE statement (SQL Server syntax)
- `type`: one of `dict`, `init`, or `gen`

Tables and their types:

- `dict`: `stores`, `zones`, `costing`, `products`, `product_categories`, `delivery_type`,
  `delivery_costing`, `order_statuses`
- `init`: `clients`, `assortment`, `delivery_resource`
- `gen`: `payments`, `delivery_tracking`, `orders`, `order_product`, `order_status_history`

Only `dict` and the three named `init` tables are populated in `initialize_data_sources`.

## Data Sources Used During Initialization

### Dictionaries (CSV)

Loaded from `dags/dictionaries` using `importlib.resources`:

- `stores.csv`
- `zones.csv`
- `costing.csv`
- `products.csv`
- `product_categories.csv`
- `delivery_type.csv`
- `delivery_costing.csv`
- `order_statuses.csv`

These files must be packaged into the container image and importable as the Python package `dictionaries`.

### Generated Data

- `clients` (`dags/generators/clients_gen_init.py`)
  - Uses Faker for names
  - `preferred_store_id` is random in range `(1, 5)`
  - `registration_date` is within 0–14 days before `project_start_date`
  - `churned` is always `0`
- `delivery_resource` (`dags/generators/delivery_resource_gen_init.py`)
  - Uses Faker for names
  - Sequential `id`
  - `delivery_type_id` from `[1, 2, 3]` with weights `[0.5, 0.35, 0.15]`
  - `store_id` random in range `(1, 5)`
  - `active_flag` set to `True`
- `assortment` (`dags/generators/assortment_gen.py`)
  - Uses `products.csv` and `stores.csv`
  - For each store and category, randomly filters products
  - Output columns: `product_id`, `store_id`

## Database IO Behavior

`dags/db/db_io.py` controls execution:

- `table_exists`: checks via `INFORMATION_SCHEMA.TABLES`
- `truncate_table`: `TRUNCATE TABLE [schema].[table]` (SQL Server syntax)
- `exec_sql`: runs DDL / SQL via SQLAlchemy `text`
- `upload_new_data`: `pandas.DataFrame.to_sql(..., if_exists="append", schema="core")`

## Refactor Notes for GHCR + PostgreSQL

These points focus on config and wiring changes needed to target PostgreSQL:

1. **Connection config**
   - Replace `dags/db/mssql.py` with a Postgres engine module (e.g., `db/postgres.py`).
   - Prefer a single `DATABASE_URL` or standard Postgres env vars:
     `PGHOST`, `PGDATABASE`, `PGUSER`, `PGPASSWORD`, `PGPORT`.
   - Update `db_io.py` to import the new engine module.

2. **DDL syntax**
   - Replace SQL Server types and brackets:
     - `tinyint` -> `smallint`
     - `bit` -> `boolean`
     - `datetime2` -> `timestamp`
     - `decimal` -> `numeric`
     - `IDENTITY(1,1)` -> `GENERATED BY DEFAULT AS IDENTITY`
   - Remove `[]` quoting and use standard `schema.table`.

3. **Truncate syntax**
   - Replace `TRUNCATE TABLE [schema].[table]` with `TRUNCATE TABLE schema.table`.

4. **Schema management**
   - `schema_init` is used everywhere. Ensure the schema exists in Postgres (create if needed).
   - If you want `public`, set `schema_init = "public"`.

5. **Packaging for container execution**
   - The generator expects `dictionaries` to be importable. In a container:
     - Install the project as a package, or
     - Set `PYTHONPATH` to the repo root.
   - Include `dags/dictionaries/*.csv` in the image build context.

6. **Dependencies**
   - Required packages: `pandas`, `numpy`, `faker`, `sqlalchemy`, and a Postgres driver (`psycopg` or `psycopg2`).

## Suggested Minimal Triggerable Command (container entrypoint idea)

When refactored into a standalone image, the main command can be a simple Python entrypoint:

```bash
python -c "from generators.data_source_initialization import initialize_data_sources; initialize_data_sources()"
```

This avoids Airflow and runs the same init logic directly.
