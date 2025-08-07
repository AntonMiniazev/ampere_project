from db.db_io import exec_sql
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

test_sql = """ SELECT TOP 1 * FROM Source.core.orders"""


def _run_test_sql():
    df = exec_sql(test_sql)
    print(df)
    return df.to_dict(orient="records")  # ✅ safe for XCom


def print_sql_result(**context):
    result = context["ti"].xcom_pull(task_ids="test_sql")
    print("🧪 Raw XCom:", result)

    if isinstance(result, list) and len(result) > 0:
        row = result[0]
    else:
        row = result

    message = build_client_message(row)
    print("🟢", message)


DAG_ID = "init_data_source_dag"

with DAG(
    DAG_ID,
    schedule="@daily",
    start_date=datetime(2025, 8, 1),
    tags=["example"],
    catchup=True,
) as dag:

    test_output = PythonOperator(
        task_id="test_sql",
        python_callable=_run_test_sql,
    )

    print_result = PythonOperator(
        task_id="print_result",
        python_callable=print_sql_result,
    )

    test_output >> print_result
