
from airflow import DAG
from airflow.sdk import task


@task(task_id="generate_and_update_clients")
def gen_clients():
    print("Clients task will be executed as if it was" &
          context["logical_date"].date())


@task(task_id="generate_orders")
def gen_orders():
    print("Orders task will be executed as if it was" &
          context["logical_date"].date())


with DAG(
    dag_id="orders_clients_generation",
    schedule="@daily",
    start_date=datetime(2025, 8, 1),
    tags=["example"],
    catchup=True,
) as dag:

    run_this = gen_clients()
    and_this = gen_orders()

    run_this >> and_this
