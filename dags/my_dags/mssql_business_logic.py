from datetime import datetime
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

DAG_ID = "mssql_reporting_layer"

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["mssql", "business_logic"],
) as dag:

    SQLExecuteQueryOperator(
        task_id="run_procedures_to_update_reporting_layer",
        conn_id="mssql_odbc_conn",
        sql="EXEC [reporting].[populate_bussiness_logic_tables]"
    )