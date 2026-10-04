# Iceberg Bronze runtime

This image runs the existing Raw-to-Bronze business flow against Lakekeeper's
`ampere-bronze` warehouse. It has its own Iceberg apply registry and never writes
to the Delta Bronze bucket. The Bronze table columns come from
`tools/uc/contracts/ampere_tables.json`; the operational registry columns come
from `app/iceberg_bronze/bronze_apply_registry_schema.json`.

`build-ampere-spark-iceberg.yml` builds this image from the repository root and
publishes `ghcr.io/antonminiazev/ampere-spark-iceberg:sha-<commit>` on a manual
workflow run or a relevant push to `migration/iceberg`. Pushes to that integration
branch also update `migration-latest`. The production `release-images.yml` and
`ampere-spark` image are unchanged.

The manual Airflow DAG is
`ampere__iceberg__bronze__raw_to_iceberg__daily`. Set `iceberg_spark_image` to
the tested immutable image tag before running it. The driver reads
`lakekeeper-spark-client` (`client-id`, `client-secret`) and the existing
`minio-creds` secret. Lakekeeper URI, warehouse, OAuth URI and scope have
separate `iceberg_` Airflow variables. No Iceberg DAG triggers a production
downstream DAG.

Run the local contract check with:

```powershell
.venv/Scripts/python.exe -m unittest discover -s tests -p test_iceberg_bronze_contract.py -v
```

An end-to-end run additionally requires three Lakekeeper warehouses, an Entra
machine client and an Airflow DAG source for `migration/iceberg` in the cluster.
