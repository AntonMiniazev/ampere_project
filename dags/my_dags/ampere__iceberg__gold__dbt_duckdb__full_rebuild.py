"""Airflow DAG: rebuild Gold from published Silver history and refresh Curie's cache."""

from utils.iceberg_dbt_layer import build_layer_dag

DAG_ID = "ampere__iceberg__gold__dbt_duckdb__full_rebuild"
SCHEDULE = None
TAGS = ["layer:gold", "format:iceberg", "system:dbt", "mode:full_rebuild"]
TRIGGER_TASK_ID = "trigger__curie__cache_refresh__post_iceberg_gold"
TRIGGER_DAG_ID = "ampere__curie__cache_refresh__post_iceberg_gold"

dag = build_layer_dag("gold", full_rebuild=True)
