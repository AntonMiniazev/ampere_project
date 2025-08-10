from utils.messages import build_client_message
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys

# Append the repo path to PYTHONPATH if needed
sys.path.append("/opt/airflow/dags/repo")

# Import the external helper function

DAG_ID = "example_mssql"


def print_sql_result(**context):
    result = context["ti"].xcom_pull(task_ids="get_orders")

    print("🧪 Raw XCom:", result)

    # Handle case: list of rows
    if isinstance(result, list) and len(result) > 0:
        row = result[0]
    else:
        row = result

    message = build_client_message(row)
    print("🟢", message)


with DAG(
    DAG_ID,
    schedule="@daily",
    start_date=datetime(2025, 8, 1),
    tags=["test"],
    catchup=False,
) as dag:
    # Execute a SQL query to get the latest order
    get_orders = SQLExecuteQueryOperator(
        task_id="get_orders",
        conn_id="mssql_conn",
        sql="SELECT TOP 1 * FROM Source.core.orders",
        do_xcom_push=True,
        return_last=True,
    )

    # Print a message based on the SQL result
    print_result = PythonOperator(
        task_id="print_result",
        python_callable=print_sql_result,
    )

    # Define task execution order
    get_orders >> print_result
