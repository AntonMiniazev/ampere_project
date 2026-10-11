"""Airflow DAG: rebuild Gold from published Silver history."""

from utils.iceberg_dbt_layer import build_layer_dag

DAG_ID = "ampere__iceberg__gold__dbt_duckdb__full_rebuild"
SCHEDULE = None
TAGS = ["layer:gold", "format:iceberg", "system:dbt", "mode:full_rebuild"]
dag = build_layer_dag("gold", full_rebuild=True)
