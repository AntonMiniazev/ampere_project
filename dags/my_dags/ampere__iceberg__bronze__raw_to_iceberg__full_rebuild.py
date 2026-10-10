"""Rebuild Bronze from all complete Raw landing history."""

from utils.iceberg_bronze_dag import build_bronze_dag

DAG_ID = "ampere__iceberg__bronze__raw_to_iceberg__full_rebuild"
SCHEDULE = None
TAGS = ["layer:bronze", "format:iceberg", "system:spark", "mode:full_rebuild"]
TRIGGER_TASK_ID = "trigger__iceberg__silver__dbt_duckdb__full_rebuild"
TRIGGER_DAG_ID = "ampere__iceberg__silver__dbt_duckdb__full_rebuild"

dag = build_bronze_dag(full_rebuild=True)
