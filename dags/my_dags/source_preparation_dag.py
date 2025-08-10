from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from generators.data_source_initialization import initialize_data_sources

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "retries": 0,
}

with DAG(
    dag_id="source_preparation_dag",
    default_args=default_args,
    schedule=None,  # Only run manually
    catchup=False,
    description="Initial data source creation and dictionary population",
    tags=["init", "source_layer", "database", "prod"],
) as dag:
    init_data_task = PythonOperator(
        task_id="initialize_data_sources", python_callable=initialize_data_sources
    )

    init_data_task
