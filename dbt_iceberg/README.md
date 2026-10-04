# Iceberg Silver and Gold dbt project

This separate dbt v2 project copies the current Silver and Gold SQL and tests.
Bronze sources resolve to `iceberg_bronze.bronze`, published Silver models to
`iceberg_silver.silver`, and Gold models to `iceberg_gold.gold`. Staging and
intermediate relations live in the pod-local `ampere_work` DuckDB file. The
existing budget CSV is loaded as `silver.budget_orders_sales` and then consumed
by the Gold budget model. The Delta dbt project is not changed.

`docker/dbt_iceberg` installs dbt v2 and the DuckDB ADBC driver. The entrypoint
prepares pod-local persistent DuckDB secrets because dbt v2 opens its own driver
connections and needs `iceberg` plus `httpfs` extensions on each one. The
Kubernetes pod receives Lakekeeper and MinIO credentials from Secrets, writes
them only into its ephemeral `secret_store` directory, and is deleted after the
run. Neither credentials nor a generated profile are committed.

To test locally without Lakekeeper, attach local DuckDB files under the four
database aliases and point `BUDGET_DAILY_CSV_PATH` at the tracked CSV. A full
`dbt build` against empty contract-shaped Bronze sources passed 46 models and
150 tests on dbt 2.0.6; this checks SQL compatibility and model order only.

Before an Airflow run, the Bohr deployment must provide the three Lakekeeper
warehouses and `lakekeeper-dbt-client` Secret. Validate the native Iceberg
`ATTACH`, Bronze read and Silver/Gold writes in that environment.
