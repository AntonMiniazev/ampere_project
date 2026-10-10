"""Airflow DAG: apply the latest Raw landing batches to Bronze Iceberg tables."""

from utils.iceberg_bronze_dag import build_bronze_dag

DAG_ID = "ampere__iceberg__bronze__raw_to_iceberg__daily"
SCHEDULE = None
TAGS = ["layer:bronze", "format:iceberg", "system:spark", "mode:daily"]
TRIGGER_TASK_ID = "trigger__iceberg__silver__dbt_duckdb__daily"
TRIGGER_DAG_ID = "ampere__iceberg__silver__dbt_duckdb__daily"

dag = build_bronze_dag(full_rebuild=False)
