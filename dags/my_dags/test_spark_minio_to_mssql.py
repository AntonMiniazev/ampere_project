# All comments are in English.
from datetime import datetime, timedelta
from pathlib import Path
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)

BASE = Path(__file__).resolve().parents[1]

with DAG(
    dag_id="test_spark_minio_to_mssql",
    description="Submit SparkApplication via Spark Operator and wait for completion.",
    start_date=datetime(2025, 8, 1),
    schedule=None,
    catchup=False,
    default_args={"owner": "ampere", "retries": 0},
    dagrun_timeout=timedelta(hours=2),
    tags=["spark", "test"],
    template_searchpath=[str(BASE), str(BASE / "spark_apps")],
) as dag:
    submit = SparkKubernetesOperator(
        task_id="submit_spark_app",
        namespace="ampere",
        application_file="spark_apps/test-mssql-to-minio.yaml",
        delete_on_termination=True,
        do_xcom_push=False,
        get_logs=True,
    )

    submit
