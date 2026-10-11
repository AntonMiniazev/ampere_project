"""Airflow DAG: rebuild Silver from all available Bronze history."""

from utils.iceberg_dbt_layer import build_layer_dag

DAG_ID = "ampere__iceberg__silver__dbt_duckdb__full_rebuild"
SCHEDULE = None
TAGS = ["layer:silver", "format:iceberg", "system:dbt", "mode:full_rebuild"]
dag = build_layer_dag("silver", full_rebuild=True)
