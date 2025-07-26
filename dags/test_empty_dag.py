# dags/test_empty_dag.py

from airflow import DAG
from datetime import datetime

with DAG(
    dag_id="test_empty_dag",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    description="A test DAG that does nothing",
    tags=["test"],
) as dag:
    pass
