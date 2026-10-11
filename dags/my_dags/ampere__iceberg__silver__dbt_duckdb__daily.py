"""Airflow DAG: build and publish daily Silver tables."""

from utils.iceberg_dbt_layer import build_layer_dag

DAG_ID = "ampere__iceberg__silver__dbt_duckdb__daily"
SCHEDULE = None
TAGS = ["layer:silver", "format:iceberg", "system:dbt", "mode:daily"]
dag = build_layer_dag("silver", full_rebuild=False)
