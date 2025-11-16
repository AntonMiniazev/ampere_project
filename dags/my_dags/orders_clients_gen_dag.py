from datetime import datetime, timedelta
from airflow import DAG  # type: ignore
from airflow.sdk import task  # type: ignore
from generators.orders_gen import prepare_orders_statuses
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

import pandas as pd
from db.db_io import upload_new_data
from generators.clients_gen import (
    prepare_clients_update_and_generation,
    update_churned,
)


@task(task_id="generate_and_update_clients")
def gen_clients(**context):
    today = context["logical_date"].date()
    to_churn_ids, clients_for_upload = prepare_clients_update_and_generation(
        today)
    update_churned(to_churn_ids)
    upload_new_data(pd.DataFrame(clients_for_upload), "clients")


@task(task_id="generate_orders")
def gen_orders(**context):
    today = context["logical_date"].date()
    yesterday = today - timedelta(days=1)
    print("Generation for " + str(today) + " and " + str(yesterday))
    prepare_orders_statuses(today, yesterday)

with DAG(
    dag_id="orders_clients_generation",
    schedule="0 3 * * *",
    start_date=datetime(2025, 8, 24),
    tags=["prod", "init", "generator", "source_layer"],
    catchup=True,
    max_active_runs=1,
) as dag:
    clients_generation = gen_clients()
    orders_generation = gen_orders()


    trigger_source_to_minio = TriggerDagRunOperator(
        task_id="trigger_source_to_minio",
        trigger_dag_id="source_to_minio",
    )

    clients_generation >> orders_generation >> trigger_source_to_minio