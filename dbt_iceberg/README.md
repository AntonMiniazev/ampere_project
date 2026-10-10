# Iceberg dbt runtime

The project builds Silver and Gold with dbt v2 and DuckDB. Contract v3 in
`tools/contracts/ampere_tables.json` defines the schema, Iceberg partition
specification, writer profile, and maintenance policy for every layer table.
The shared resolver is packaged into the Spark and dbt images from the same
repository revision.

## Layer execution

Silver and Gold each run in a separate Kubernetes pod with a private DuckDB
workspace. The pod prepares its catalogs and credentials, runs dbt build and
tests for one layer, validates every staged model against the contract and
published table, then publishes that layer. A Silver run reads Bronze through
Lakekeeper. A Gold run reads published Silver through Lakekeeper and stages
only Gold output locally. Gold never reads a local Silver workspace.

Silver daily runs use the current and previous month window for their fact
inputs. Fact publication merges the staged keys without deleting history
outside that slice. Complete Silver dimensions and budgets also remove keys
absent from their staged source. Every Gold run reads all published Silver
history and stages complete snapshots of all eight aggregates. Daily and
full-history Gold publication both delete keys missing from the validated
snapshot; an empty complete Gold model fails before any table is changed.

The Gold Iceberg tables are `marketing_sales_budget_monthly_store`,
`marketing_product_sales_monthly_store`,
`marketing_category_sales_monthly_store`,
`marketing_client_metrics_monthly_store`, `marketing_active_client_month`,
`financial_performance_monthly_store`,
`financial_product_margin_monthly_store`, and
`delivery_courier_performance_monthly_store`.

Three large Silver fact tables use deterministic `order_id` ranges during a
full-history publish: `fact_delivery_tracking`, `fact_order_product`, and
`fact_order_status_history`. `fact_order_product` uses six ranges to keep each
large merge smaller; the other two use three. Each range is its own Iceberg
commit. A retry converges by merging each range again. Catalog initialization
must run first; the publisher does not create or replace target tables.

The Silver/Gold write layout follows DuckDB Iceberg 1.5 capabilities. These
tables have no sort order because DuckDB mutations reject sorted tables. Silver
facts use only supported partition transforms; the monthly fact tables use
identity `order_date` partitions. DuckDB cannot enforce the target-file-size
property while writing partitioned tables, so those writes explicitly ignore
that setting and weekly Spark housekeeping applies the contract target during
compaction. The contract loader rejects unsupported DuckDB layouts.

Housekeeping evaluates active `data_files` and `delete_files` metadata by Iceberg
partition. `min_input_files`, `delete_file_count_threshold`, and
`manifest_count_threshold` are validated profile settings; the delete-file
threshold counts delete files, not deleted records.

Airflow DAGs:

- `ampere__iceberg__silver__dbt_duckdb__daily`
- `ampere__iceberg__gold__dbt_duckdb__daily`
- `ampere__iceberg__silver__dbt_duckdb__full_rebuild`
- `ampere__iceberg__gold__dbt_duckdb__full_rebuild`

The daily Silver DAG triggers daily Gold. The full-history Silver DAG triggers
full-history Gold. Gold triggers Curie cache refresh only after successful
publication. Catalog initialization, Bronze/Silver/Gold publication, and
housekeeping share the one-slot Airflow pool `iceberg_pipeline_mutation`. It
must be provisioned before deploying the DAGs; it serializes all table writes
and maintenance.

## Runtime settings

Daily pod settings use `iceberg_dbt_threads`,
`iceberg_dbt_duckdb_memory_limit`, `iceberg_dbt_duckdb_threads`,
`iceberg_dbt_duckdb_max_temp_directory_size`, `iceberg_dbt_cpu_request`,
`iceberg_dbt_cpu_limit`, `iceberg_dbt_pod_memory_request`, and
`iceberg_dbt_pod_memory_limit`.

Full-history pod settings use `iceberg_full_rebuild_dbt_threads`,
`iceberg_full_rebuild_duckdb_threads`,
`iceberg_full_rebuild_duckdb_memory_limit`,
`iceberg_full_rebuild_duckdb_max_temp_directory_size`,
`iceberg_full_rebuild_min_scratch_gb`,
`iceberg_full_rebuild_scratch_pvc`, and the corresponding
`iceberg_full_rebuild_dbt_*` CPU and memory variables. When no PVC is set, the
pod uses a private 24 GiB `emptyDir`, requests 16 GiB ephemeral storage, and
checks the configured free-space threshold before dbt starts. Temporary
workspace and spill files are removed when the pod exits.

Both pod types use the Airflow image variables, three Lakekeeper warehouse
variables, MinIO credentials, the `lakekeeper-dbt-client` Kubernetes Secret,
and the local CA bundle. The dbt release image is shared by both layers.

## Checks

Run the contract validation and focused unit checks from the repository root:

```bash
uv run python tools/contracts/ampere_contract.py
uv run python -m unittest tests.test_iceberg_bronze_contract tests.test_iceberg_housekeeping -v
```

For image builds, `.github/workflows/build-ampere-dbt-iceberg.yml` checks dbt
catalog setup and publisher behavior before building the shared image.
