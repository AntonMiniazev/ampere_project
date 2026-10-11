"""Airflow DAG: build and publish daily Gold tables."""

from utils.iceberg_dbt_layer import build_layer_dag

DAG_ID = "ampere__iceberg__gold__dbt_duckdb__daily"
SCHEDULE = None
TAGS = ["layer:gold", "format:iceberg", "system:dbt", "mode:daily"]
dag = build_layer_dag("gold", full_rebuild=False)
