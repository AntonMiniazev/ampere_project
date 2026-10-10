# Ampere dataflow

Ampere produces synthetic source data in PostgreSQL and publishes analytical Iceberg tables in Lakekeeper. Airflow starts the daily generator and hands off between Raw, Bronze, Silver, Gold, and Curie cache refresh. Contract v3 is the canonical definition for all 42 published and operational Iceberg tables.

After a successful Gold publish, the Curie refresh DAG requests a FlightSQL cache refresh through `/api/cache/refresh_flightsql`, then polls the Iceberg cache status until a new release is active.

## Layers

1. **Source:** Python generators create orders, clients, products, payments, delivery activity, and costs in PostgreSQL.
2. **Raw:** Spark extracts immutable Parquet batches to MinIO. A manifest and `_SUCCESS` marker identify a complete batch; state files track extraction progress. The Raw and Bronze SparkApplication templates load bundled JARs from driver and executor classpaths, avoiding a startup copy into the application directory, which the runtime user cannot write.
3. **Bronze:** Spark applies complete batches to Iceberg tables in Lakekeeper. Snapshot partitions are replaced, mutable dimensions and events are merged, and facts follow their configured append or merge strategy. The Bronze apply registry tracks processed batches.
4. **Silver:** An isolated dbt/DuckDB pod stages, cleans, joins, tests, and publishes reusable entities.
5. **Gold:** A separate dbt/DuckDB pod reads published Silver tables and publishes sales, delivery, cost, and margin marts.
6. **Serving:** Curie refreshes its cache after successful Gold publication.

[Project diagram](dataflow/generated/project_dataflow.md) · [Table movement](dataflow/generated/table_groups.md) · [Layer responsibilities](dataflow/generated/layer_responsibilities.md)

## Orchestration and recovery

The scheduled generator triggers Raw landing, then Bronze daily, Silver daily, Gold daily, and Curie. Each handoff preserves the same Airflow logical date; downstream stages are trigger-only. Full rebuild uses Bronze, Silver, and Gold full-history DAGs in the same order after catalog initialization. Named Airflow pools `iceberg_silver_publish` and `iceberg_gold_publish` serialize daily and full rebuilds for each layer. [The DAG inventory and trigger graph](dataflow/generated/airflow_dag_orchestration.md) are generated from checked-in DAG metadata.

After a successful Sunday Curie cache refresh, housekeeping uses contract v3 to discover its table scope and resolve retention, compaction thresholds, target file size, delete-file handling, and manifest rewrite policy. It checks table conformance before maintenance and keeps extra tables and Raw landing outside its scope. Table rows are unchanged.

For a clean rebuild, initialize the catalog, replay all validated Raw landing batches through the Bronze full-rebuild DAG, then run the Silver and Gold full-rebuild DAGs. Gold reads published Silver from Lakekeeper. The final Gold DAG triggers Curie refresh.

The daily publisher stages and tests models locally before changing Lakekeeper tables. Facts use keyed `MERGE` updates and inserts. Complete dimensions and budgets use a keyed merge followed by deletion of keys absent from the complete source. A failed publish can leave some tables updated while others remain at their prior snapshot; rerunning the DAG completes the publication. Daily fact publication does not delete rows that disappear entirely from the staged slice.

Silver and Gold layouts follow DuckDB Iceberg 1.5 write limits: they are unsorted, and Silver fact partitioning uses DuckDB-supported identity transforms. For partitioned writes DuckDB ignores the target-file-size property; weekly Spark housekeeping uses the contract size target when compacting those tables. Contract validation rejects Spark-only distribution modes and unsupported DuckDB transforms before initialization.

## Contract and catalog

[The canonical Iceberg contract](../tools/contracts/ampere_tables.json) defines schemas, physical layouts, write profiles, and maintenance policies across Bronze, Silver, and Gold. `tools/contracts/ampere_contract.py` validates and resolves profile defaults plus per-table layout overrides. Lakekeeper provides separate Bronze, Silver, and Gold warehouses; the apply registry is in Bronze's `ops` namespace. dbt models define Silver and Gold transformations and tests.

To regenerate the diagrams and DAG inventory, run `uv run python tools/docs/generate_dataflow_docs.py` from the repository root.
