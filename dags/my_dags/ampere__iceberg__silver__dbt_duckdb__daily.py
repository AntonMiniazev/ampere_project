"""Airflow DAG: build and publish daily Silver tables, then trigger daily Gold."""

from utils.iceberg_dbt_layer import build_layer_dag

DAG_ID = "ampere__iceberg__silver__dbt_duckdb__daily"
SCHEDULE = None
TAGS = ["layer:silver", "format:iceberg", "system:dbt", "mode:daily"]
TRIGGER_TASK_ID = "trigger__iceberg__gold__dbt_duckdb__daily"
TRIGGER_DAG_ID = "ampere__iceberg__gold__dbt_duckdb__daily"
TRIGGER_WAIT_FOR_COMPLETION = False

dag = build_layer_dag(
    "silver", full_rebuild=False, wait_for_completion=TRIGGER_WAIT_FOR_COMPLETION
)
