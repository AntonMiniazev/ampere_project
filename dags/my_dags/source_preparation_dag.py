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
    schedule=None,
    catchup=False,
    description="Initial data source creation and dictionary population with data stored in excel tables. Executed one time only, after that data generated in orders_clients_generation.",
    tags=["init", "source_layer", "database", "prod"],
) as dag:
    init_data_task = PythonOperator(
        task_id="initialize_data_sources", python_callable=initialize_data_sources
    )

    init_data_task
