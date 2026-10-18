# Iceberg dbt runtime

The project builds Silver and Gold with dbt v2 and DuckDB. Contract v7 in
`tools/contracts/ampere_tables.json` defines the schema, Iceberg partition
specification, writer profile, and maintenance policy for every layer table.
The shared resolver is packaged into the Spark and dbt images from the same
repository revision.

For v7 order-time fields, catalog initialization appends nullable columns to
existing Raw, Bronze, and Silver tables. The contract keeps these columns at
the end of those table schemas so schema validation matches Iceberg evolution.

## Layer execution

Silver and Gold each run in a separate Kubernetes pod with a private DuckDB
workspace. The pod prepares its catalogs and credentials, runs dbt build and
tests for one layer, validates every staged model against the contract and
published table, then publishes that layer. A Silver run reads Bronze through
Lakekeeper. A Gold run reads published Silver through Lakekeeper and stages
only Gold output locally. Gold never reads a local Silver workspace.

Silver daily runs read the immutable Raw change journal for the active dataset,
bounded by the successful Silver checkpoint and the applied Bronze batch sequence. They rebuild complete order
records and event rows only for affected order IDs, including orders touched by
late events or relevant dimension changes. PostgreSQL checkpoints advance only
after all Silver tables publish. MERGE change detection ignores volatile Silver
lineage columns, so unchanged replays do not rewrite rows. The large
`fact_order_product` publication visits both old and new `order_date` weeks,
including moved and removed rows. Delivery tracking is unpartitioned and uses
bounded affected-order ID ranges. Executed query profiles report scans; a date
predicate does not establish Iceberg file pruning. Complete dimensions
still synchronize their full source snapshots.

Silver persists pending month invalidations before any Iceberg commits and
retains them on retries. Budget snapshot comparison includes old/new dates,
deleted rows, empty budgets and store labels on budget-only months. Preference
history merges only current-dataset delta records daily and synchronizes the
complete current-dataset history during a full rebuild.

Gold daily runs consume only completed publications for the active dataset,
up to a frozen publication sequence. Each selected month is rebuilt as a complete slice and replaces
only that month, including explicit deletion when the month becomes empty.
Untouched months remain unchanged. Full-history Gold still reads full Silver
history. The preference-history model derives as-of client preference from
Raw records and returns NULL for preference-based historical metrics before
that history begins, including partial first months. Daily client histories are
collapsed to effective intervals before joining selected month start/end dates.
Clients prefer stores, and a store may cover multiple delivery zones. Existing
order zones are never inferred from stores. Known zones must belong to the
selected store; `preferred_zone_id_at_order` is unsupported and always NULL in
Silver. Gold delivery costs use the order's selected store and the final courier's
delivery type; `order_zone_id` is not required. Every zone covered by that store
must have exactly one non-null tariff for that type, and all amounts must agree.
The common amount is charged once per order. Duplicate or conflicting tariffs
fail checks. Missing coverage or tariffs are
reported and remain NULL in cost/profit totals; they are never replaced by zero.

No source, Raw or Bronze data/schema changes are required for this correction.
The contract change is limited to Silver preference-history publication policy.
See [downstream validation](../docs/dataflow/silver_gold_incremental_validation.md)
for isolated runs, reference comparison and memory evidence.

The generator seeds active orders with delivery-tracking placeholders that
contain a courier but no status or timestamp. `stg_delivery_tracking` excludes
these rows so full-history Silver builds publish only actual tracking events.

The Gold Iceberg tables are `marketing_sales_budget_monthly_store`,
`marketing_product_sales_monthly_store`,
`marketing_category_sales_monthly_store`,
`marketing_client_metrics_monthly_store`, `marketing_active_client_month`,
`financial_performance_monthly_store`,
`financial_product_margin_monthly_store`, and
`delivery_courier_performance_monthly_store`.

Large Silver fact tables are bounded during full-history publication.
`fact_order_product` uses `order_date` week slices so target scans can be
pruned by the Iceberg partition key; `fact_delivery_tracking` and
`fact_order_status_history` use deterministic order-ID ranges. Each slice is
its own Iceberg commit and retries converge by replaying committed slices.
Targets must already exist; missing targets block publication. The downstream
recovery does not initialize or replace upstream tables.

The full-history `stg_order_status_history` model first copies only its required
Bronze columns to the disk-backed DuckDB workspace, then deduplicates four
deterministic `hash(order_id)` buckets sequentially. Rows for an order remain
in one bucket, so the `(order_id, status_datetime)` window partition and its
latest-apply selection produce the same result as a single full-history query.
The daily path keeps the related-order filter and single-pass deduplication.
This bounds the window operation's working set at the cost of four local scans;
it does not truncate or calendar-batch history.

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

The daily Silver DAG triggers daily Gold. Full-history rebuild DAGs are manual
entrypoints and do not start the next layer. The daily Gold DAG triggers Curie
cache refresh only after successful publication. Catalog initialization,
Bronze/Silver/Gold publication, and housekeeping share the one-slot Airflow
pool `iceberg_pipeline_mutation`. It must be provisioned before deploying the
DAGs; it serializes all table writes and maintenance.

## Runtime settings

Daily pod settings use `iceberg_dbt_threads`,
`iceberg_dbt_duckdb_memory_limit`, `iceberg_dbt_duckdb_threads`,
`iceberg_dbt_duckdb_max_temp_directory_size`, `iceberg_dbt_cpu_request`,
`iceberg_dbt_cpu_limit`, `iceberg_dbt_pod_memory_request`, and
`iceberg_dbt_pod_memory_limit`.
The Silver daily DuckDB memory limit defaults to 7 GB under its 10 GiB pod
limit, leaving room for dbt, Python, and native process overhead. Gold keeps its
4 GB daily default.

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
