# Ampere dataflow

Ampere produces synthetic source data in PostgreSQL and publishes analytical Iceberg tables in Lakekeeper. Airflow starts the daily generator and hands off between Raw, Bronze, Silver, Gold, and Curie cache refresh. Contract v3 is the canonical definition for all 42 published and operational Iceberg tables.

After a successful Gold publish, the Curie refresh DAG requests a FlightSQL cache refresh through `/api/cache/refresh_flightsql`, then polls the Iceberg cache status until a new release is active.

## Layers

1. **Source:** Python generators create orders, clients, products, payments, delivery activity, and costs in PostgreSQL.
2. **Raw:** Spark extracts immutable Parquet batches to MinIO. A manifest and `_SUCCESS` marker identify a complete batch; state files track extraction progress. The small mutable `clients` dimension is fully extracted each day so a missed or previously absent client can be reconciled; Bronze merges those rows by key. Other mutable dimensions use their configured watermarks. The Raw and Bronze SparkApplication templates load bundled JARs from driver and executor classpaths, avoiding a startup copy into the application directory, which the runtime user cannot write.
3. **Bronze:** Spark applies complete batches to Iceberg tables in Lakekeeper. Snapshot partitions are replaced, mutable dimensions and events are merged, and facts follow their configured append or merge strategy. The Bronze apply registry tracks processed batches.
4. **Silver:** An isolated dbt/DuckDB pod stages, cleans, joins, tests, and publishes reusable entities.
5. **Gold:** A separate dbt/DuckDB pod reads published Silver tables and publishes sales, delivery, cost, and margin marts.
6. **Serving:** Curie refreshes its cache after successful Gold publication.

[Project diagram](dataflow/generated/project_dataflow.md) · [Table movement](dataflow/generated/table_groups.md) · [Layer responsibilities](dataflow/generated/layer_responsibilities.md)

## Orchestration and recovery

The scheduled generator waits for Raw landing; Raw landing waits for Bronze; and the Bronze, Silver, and Gold handoffs wait for downstream success while preserving the same Airflow logical date. Full rebuild uses Bronze, Silver, and Gold full-history DAGs in the same order after catalog initialization. All catalog initialization, publication, and housekeeping tasks share the one-slot Airflow pool `iceberg_pipeline_mutation`, so these table mutations cannot overlap. This pool must exist before deploying the DAGs. [The DAG inventory and trigger graph](dataflow/generated/airflow_dag_orchestration.md) is generated from checked-in DAG metadata.

Default CPU requests target the 6-CPU node4 capacity while preserving headroom for resident services: Raw executors request 700m each; Bronze snapshots/mutable-dimension executors request 750m each; Bronze facts/events executors request 1250m each with two executors; the generator requests 1500m; and daily and full-history dbt pods request 2 CPUs. Airflow Variables can override Spark and dbt CPU requests; the request overrides checked for this sizing change are absent. Catalog-init and housekeeping client pods stay at 250m because their Spark work runs on Spark Connect.

After a successful Sunday Curie cache refresh, housekeeping uses contract v3 to discover its table scope and resolve retention, compaction thresholds, target file size, delete-file handling, and manifest rewrite policy. Its delete-file trigger counts active delete files per Iceberg partition, not deleted records. It checks table conformance before maintenance and keeps extra tables and Raw landing outside its scope. Table rows are unchanged.

For a clean rebuild, initialize the catalog, replay all validated Raw landing batches through the Bronze full-rebuild DAG, then run the Silver and Gold full-rebuild DAGs. Gold reads published Silver from Lakekeeper. The final Gold DAG triggers Curie refresh.

The daily publisher stages and tests models locally before changing Lakekeeper tables. Silver daily `fact_delivery_tracking` and `fact_order_product` inputs publish through three `order_id` ranges each, with keyed `MERGE` updates and inserts; target-only historical rows are preserved. Complete Silver dimensions and budgets use a keyed merge followed by deletion of keys absent from the complete source. Every Gold dbt model reads the full published Silver inputs and produces a complete aggregate snapshot in daily and full-history modes. Gold publication updates and inserts its keys, then removes keys absent from that snapshot. Empty complete sources fail before publication. Fact ranges commit independently, so a failed publish can leave earlier ranges applied; rerunning the DAG converges by replaying all ranges.

Silver and Gold layouts follow DuckDB Iceberg 1.5 write limits: they are unsorted, and Silver fact partitioning uses DuckDB-supported identity transforms. For partitioned writes DuckDB ignores the target-file-size property; weekly Spark housekeeping uses the contract size target when compacting those tables. Contract validation rejects Spark-only distribution modes and unsupported DuckDB transforms before initialization.

## Contract and catalog

[The canonical Iceberg contract](../tools/contracts/ampere_tables.json) defines schemas, physical layouts, Silver/Gold merge keys and source completeness, write profiles, and maintenance policies across Bronze, Silver, and Gold. `tools/contracts/ampere_contract.py` validates and resolves profile defaults plus per-table layout overrides. Spark conformance checks the active Iceberg partition spec from the table's structured metadata JSON, including source field IDs and transforms. Bronze profile `write.mode` records intended writer behavior; the Bronze runtime dispatches from its stream-group and per-table settings, so that profile value is descriptive and is not independently enforced. Lakekeeper provides separate Bronze, Silver, and Gold warehouses; the apply registry is in Bronze's `ops` namespace. dbt models define Silver and Gold transformations and tests.

To regenerate the diagrams and DAG inventory, run `uv run python tools/docs/generate_dataflow_docs.py` from the repository root.
