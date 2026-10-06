# Iceberg Silver and Gold dbt project

This separate dbt v2 project copies the current Silver and Gold SQL and tests.
Bronze sources resolve to `iceberg_bronze.bronze`, published Silver models to
`iceberg_silver.silver`, and Gold models to `iceberg_gold.gold`. Staging and
intermediate relations live in the pod-local `ampere_work` DuckDB file. The
existing budget CSV is loaded as `silver.budget_orders_sales` and then consumed
by the Gold budget model. The Delta dbt project is not changed.

Iceberg does not store `SMALLINT`; the Iceberg staging models widen those IDs to
`INTEGER` before publishing tables. Their values and join keys are unchanged.

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

## Airflow run modes

`ampere__iceberg__silver_gold__dbt_duckdb__daily` runs Silver and Gold using
separate `iceberg_silver_run_mode` and `iceberg_gold_run_mode` variables
(default `daily_refresh`) and their corresponding `iceberg_*_lookback_days`
variables. The modes and dbt pod sizing are independent of Delta. In the daily
DAG, dbt first builds and tests its Silver and Gold slice in pod-local DuckDB
files. Only after dbt succeeds does `publish_catalog.py` attach Lakekeeper and
publish the results. Silver facts and changing Gold facts use keyed Iceberg
`MERGE INTO` updates/inserts, so rows outside the daily slice remain available.
Small dimension and budget tables are rebuilt from complete source data. The
publisher checks staged key uniqueness and expected model coverage before
modifying any Iceberg table. A missing fact target fails closed; run the manual
full rebuild to establish the historical baseline first.

The staged Gold models consume the staged Silver slice without applying a
second Gold date filter. Keyed updates preserve older rows even when a recent
source event changes an older order. Silver first selects changed orders from
Bronze, then loads all their product, payment, status, and delivery records so
older orders are recomputed with complete order context. Iceberg facts currently have no date
partitioning, so monitor merge runtime and metadata growth. This path updates
and inserts rows; records that disappear entirely from a staged fact are not
deleted from the published table and need a separate deletion strategy.

`ampere__iceberg__silver_gold__dbt_duckdb__full_rebuild` runs the same dbt
build with both modes set to `full_history`. It rebuilds the Iceberg Silver and
Gold tables from the complete Bronze history currently present in Lakekeeper,
then triggers the Iceberg Curie cache refresh. It does not backfill Bronze.
The full rebuild keeps the direct dbt table materialization and replaces the
published tables from all Bronze history. Run it once after deploying staged
daily publication to restore the dates removed by earlier daily runs. dbt
`--full-refresh` is not needed for these table and view models.

The rebuild sizing is controlled by these optional Airflow variables (defaults
shown): `iceberg_full_rebuild_dbt_threads` (`1`),
`iceberg_full_rebuild_duckdb_threads` (`2`),
`iceberg_full_rebuild_duckdb_memory_limit` (`7GB`),
`iceberg_full_rebuild_dbt_cpu_request` (`1`),
`iceberg_full_rebuild_dbt_cpu_limit` (`4`),
`iceberg_full_rebuild_dbt_pod_memory_request` (`5Gi`), and
`iceberg_full_rebuild_dbt_pod_memory_limit` (`10Gi`). Daily pod sizing uses
`iceberg_dbt_threads`, `iceberg_dbt_duckdb_memory_limit`,
`iceberg_dbt_cpu_request`, `iceberg_dbt_cpu_limit`,
`iceberg_dbt_pod_memory_request`, and `iceberg_dbt_pod_memory_limit`.
The full rebuild also disables DuckDB insertion-order preservation. These
settings limit concurrent full-history sorts while retaining the 10 GiB pod
limit; the daily pipeline keeps its existing settings.

Before an Airflow run, the Bohr deployment must provide the three Lakekeeper
warehouses and `lakekeeper-dbt-client` Secret. Validate the native Iceberg
`ATTACH`, Bronze read and Silver/Gold writes in that environment.
