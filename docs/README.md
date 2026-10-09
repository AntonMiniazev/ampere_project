# Ampere dataflow

Ampere produces synthetic source data in PostgreSQL and publishes analytical Iceberg tables in Lakekeeper. Airflow starts the daily generator and hands off between Raw, Bronze, Silver/Gold, and Curie cache refresh.

After a successful Gold publish, the Curie refresh DAG requests a FlightSQL cache refresh through `/api/cache/refresh_flightsql`, then polls the Iceberg cache status until a new release is active.

## Layers

1. **Source:** Python generators create orders, clients, products, payments, delivery activity, and costs in PostgreSQL.
2. **Raw:** Spark extracts immutable Parquet batches to MinIO. A manifest and `_SUCCESS` marker identify a complete batch; state files track extraction progress. The Raw and Bronze SparkApplication templates load bundled JARs from driver and executor classpaths, avoiding a startup copy into the application directory, which the runtime user cannot write.
3. **Bronze:** Spark applies complete batches to Iceberg tables in Lakekeeper. Snapshot partitions are replaced, mutable dimensions and events are merged, and facts follow their configured append or merge strategy. The Bronze apply registry tracks processed batches.
4. **Silver:** dbt and DuckDB stage, clean, join, and test reusable entities before publishing Iceberg tables.
5. **Gold:** dbt publishes sales, delivery, cost, and margin marts as Iceberg tables.
6. **Serving:** Curie refreshes its cache after successful Gold publication.

[Project diagram](dataflow/generated/project_dataflow.md) · [Table movement](dataflow/generated/table_groups.md) · [Layer responsibilities](dataflow/generated/layer_responsibilities.md)

## Orchestration and recovery

The scheduled generator triggers Raw landing; Raw triggers Iceberg Bronze; Bronze triggers the combined Silver/Gold dbt DAG; a successful dbt publish triggers Curie. Each handoff uses the same Airflow logical date. [The DAG inventory and trigger graph](dataflow/generated/airflow_dag_orchestration.md) are generated from checked-in DAG files.

After a successful Sunday Curie cache refresh, the Iceberg housekeeping DAG uses Spark Connect to expire snapshots older than 14 days and remove aged orphan files from the 42 known Bronze, Silver, Gold, and Bronze `ops` tables. It also bounds tracked metadata JSON versions to 14 previous files. Current table rows and active data files are preserved; a Bronze repair backup and Raw landing storage are outside this job.

The manual Iceberg Silver/Gold full rebuild reads all available Bronze history. It builds and validates Silver and Gold in separate dbt processes on node4's local SSD, then publishes the staged tables and triggers Curie refresh. It does not backfill missing Bronze batches.

The daily publisher stages and tests models locally before changing Lakekeeper tables. Facts use keyed `MERGE` updates and inserts. Complete dimensions and budgets use a keyed merge followed by deletion of keys absent from the complete source. A failed publish can leave some tables updated while others remain at their prior snapshot; rerunning the DAG completes the publication. Daily fact publication does not delete rows that disappear entirely from the staged slice.

## Contract and catalog

[The Bronze schema contract](../tools/iceberg/contracts/ampere_tables.json) defines the table names, column types, and partition keys used when Spark creates Iceberg Bronze tables. Lakekeeper provides separate Bronze, Silver, and Gold warehouses. The operational Bronze registry lives in the Bronze warehouse's `ops` namespace. The dbt model files under `dbt_iceberg/` define Silver and Gold transformations and tests.

To regenerate the diagrams and DAG inventory, run `uv run python tools/docs/generate_dataflow_docs.py` from the repository root.
