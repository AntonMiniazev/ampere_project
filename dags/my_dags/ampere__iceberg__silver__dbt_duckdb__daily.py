"""Airflow DAG: build and publish daily Silver tables."""

from utils.iceberg_dbt_layer import build_layer_dag

DAG_ID = "ampere__iceberg__silver__dbt_duckdb__daily"
SCHEDULE = None
TAGS = ["layer:silver", "format:iceberg", "system:dbt", "mode:daily"]
TRIGGER_TASK_ID = "trigger__iceberg__gold__dbt_duckdb__daily"
TRIGGER_DAG_ID = "ampere__iceberg__gold__dbt_duckdb__daily"
TRIGGER_WAIT_FOR_COMPLETION = True

dag = build_layer_dag(
    "silver",
    full_rebuild=False,
    trigger_dag_id=TRIGGER_DAG_ID,
    trigger_task_id=TRIGGER_TASK_ID,
    wait_for_completion=TRIGGER_WAIT_FOR_COMPLETION,
)
