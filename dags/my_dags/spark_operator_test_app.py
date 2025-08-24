# All comments are in English.
from datetime import datetime, timedelta
from pathlib import Path
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import (
    SparkKubernetesSensor,
)

BASE = Path(__file__).resolve().parents[1]  # /opt/airflow/dags/repo/dags

with DAG(
    dag_id="spark_operator_test_app",
    description="Submit SparkApplication via Spark Operator and wait for completion.",
    start_date=datetime(2025, 8, 1),
    schedule=None,
    catchup=False,
    default_args={"owner": "ampere", "retries": 0},
    dagrun_timeout=timedelta(hours=2),
    tags=["spark", "operator", "minio"],
    # Important: let Jinja look both in /dags and in /dags/spark_apps
    template_searchpath=[str(BASE), str(BASE / "spark_apps")],
) as dag:
    submit = SparkKubernetesOperator(
        task_id="submit_spark_app",
        namespace="ampere",
        # Relative to template_searchpath
        application_file="spark_apps/test-app-python.yaml",
        delete_on_termination=False,
        do_xcom_push=True,
        get_logs=True,
    )

    submit
