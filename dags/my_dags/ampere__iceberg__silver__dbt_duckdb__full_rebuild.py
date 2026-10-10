"""Rebuild Silver from all available Bronze history, then trigger Gold."""

from utils.iceberg_dbt_layer import build_layer_dag

DAG_ID = "ampere__iceberg__silver__dbt_duckdb__full_rebuild"
SCHEDULE = None
TAGS = ["layer:silver", "format:iceberg", "system:dbt", "mode:full_rebuild"]
TRIGGER_TASK_ID = "trigger__iceberg__gold__dbt_duckdb__full_rebuild"
TRIGGER_DAG_ID = "ampere__iceberg__gold__dbt_duckdb__full_rebuild"

dag = build_layer_dag("silver", full_rebuild=True)
