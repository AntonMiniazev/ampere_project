# Iceberg Silver and Gold dbt project

This dbt v2 project defines the Silver and Gold SQL models and tests.
Bronze sources resolve to `iceberg_bronze.bronze`, published Silver models to
`iceberg_silver.silver`, and Gold models to `iceberg_gold.gold`. Staging and
intermediate relations live in the pod-local `ampere_work` DuckDB file. The
budget CSV is loaded as `silver.budget_orders_sales` and then consumed
by the Gold budget model.

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
variables. In the daily
DAG, dbt first builds and tests its Silver and Gold slice in pod-local DuckDB
files. Only after dbt succeeds does `publish_catalog.py` attach Lakekeeper and
publish the results. Silver facts and changing Gold facts use keyed Iceberg
`MERGE INTO` updates/inserts, so rows outside the daily slice remain available.
Complete dimension and budget tables use keyed `MERGE` for updates/inserts,
then a separate `DELETE` removes published keys absent from the complete
staged source. DuckDB-Iceberg 1.5.6 rejects a single `MERGE` with all three
actions. Daily fact slices use update/insert `MERGE` without deleting absent
keys. The publisher checks
staged key uniqueness, nonempty complete snapshots, target presence, and
expected model coverage before modifying any Iceberg table. A missing daily
target fails closed; run the manual full rebuild to establish its baseline.
The complete-source path retains Iceberg table identity and can also
synchronize a staged full-history table. Upsert and cleanup are two Iceberg
commits: a failed cleanup can temporarily leave stale rows, but rerunning
converges. Curie refresh follows only a fully successful publish. Full-history
merges on large facts need a measured cluster run before routine use.

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
The full rebuild uses direct dbt table materialization and replaces published
tables from all Bronze history. It is the recovery path; the staged
full-history publisher path is available for a future measured change. dbt
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
