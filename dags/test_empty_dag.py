from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

with DAG(
    dag_id="test_empty_dag",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    description="A test DAG that does nothing",
    tags=["test"],
) as dag:
    EmptyOperator(task_id="noop")
