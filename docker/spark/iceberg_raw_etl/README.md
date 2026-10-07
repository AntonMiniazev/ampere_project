# Iceberg Bronze runtime

This image applies Raw batches to Lakekeeper's `bronze` warehouse.
It tracks completed batches in an Iceberg apply registry. Bronze table columns come from
`tools/iceberg/contracts/ampere_tables.json`; the operational registry columns come
from `app/iceberg_bronze/bronze_apply_registry_schema.json`.

`build-ampere-spark-iceberg.yml` builds this image from the repository root and
publishes `ghcr.io/antonminiazev/ampere-spark-iceberg:sha-<commit>` on a manual
workflow run or a relevant push to `migration/iceberg` or `main`. Pushes to
`migration/iceberg` also update `migration-latest`. Numeric release tags publish the image through
`release-images.yml`.

The same image runs the Raw extractor for
`ampere__raw_landing__postgres_to_landing__daily` and the Bronze apply job for
`ampere__iceberg__bronze__raw_to_iceberg__daily`. The image bundles its PostgreSQL
JDBC and S3A libraries, so the Raw job does not depend on a shared Ivy cache.
The same image runs the weekly Spark Connect housekeeping client. It expires
Iceberg snapshots and orphan files older than 14 days and limits previous
tracked metadata JSON files to 14 for known pipeline tables. Extra catalog
tables, including repair backups, remain untouched.
The driver reads `lakekeeper-spark-client` (`client-id`, `client-secret`) and
`minio-creds`. Lakekeeper URI, warehouse, OAuth URI and scope have separate
`iceberg_` Airflow variables. The DAG triggers Silver/Gold after success.
Bronze places its driver on node2 and its two executors on node4 by default.
The nodes can be changed with `spark_bronze_driver_node_selector` and
`spark_executor_node_selector` Airflow Variables. Node3 currently lacks the
free CPU and memory reservations for the driver. Confirm both executors reach
Running and compare group durations before changing executor count or cores.
The facts/events group keeps two executors on node4 but gives each two Spark
task slots and a 3 GiB executor heap by default. Its one-core CPU request
per executor should leave enough node4 reservation headroom with Spark
Connect still present; verify the actual pod requests and both Running pods
on the next release. The snapshot and mutable-dimension groups retain one core per
executor; their short, sequential table work has little to gain from another
executor pod. The first run on node2 spent about three minutes pulling the
Spark image, so compare later warm-image runs when measuring throughput.

Mutable dimension merges compare the Raw extract date with the date recorded
in each Bronze row's manifest path. Retrying an older Raw batch therefore
cannot overwrite a newer dimension value. The guard applies to matched rows;
new business keys are still inserted.

Run the local contract check with:

```powershell
.venv/Scripts/python.exe -m unittest discover -s tests -p test_iceberg_bronze_contract.py -v
```

An end-to-end run requires three Lakekeeper warehouses, an Entra machine client,
and the Airflow DAG source in the cluster.
