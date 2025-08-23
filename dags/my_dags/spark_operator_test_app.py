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

# Points to /opt/airflow/dags/repo/dags
BASE = Path(__file__).resolve().parents[1]
# New location without the "k8s" level
APP_FILE = BASE / "spark_apps" / "test-app.yaml"

with DAG(
    dag_id="spark_operator_test_app",
    description="Submit SparkApplication via Spark Operator and wait for completion.",
    start_date=datetime(2025, 8, 1),
    schedule=None,
    catchup=False,
    default_args={"owner": "ampere", "retries": 0},
    dagrun_timeout=timedelta(hours=2),
    tags=["spark", "operator", "minio"],
) as dag:
    submit = SparkKubernetesOperator(
        task_id="submit_spark_app",
        namespace="ampere",
        application_file=str(APP_FILE),
        delete_on_termination=False,  # keep CR so the sensor can watch it
        do_xcom_push=True,  # pushes app name to XCom
        get_logs=True,  # stream driver logs
    )

    wait_done = SparkKubernetesSensor(
        task_id="wait_spark_app_succeeded",
        namespace="ampere",
        application_name="{{ ti.xcom_pull(task_ids='submit_spark_app')['metadata']['name'] }}",
        attach_log=True,
        poke_interval=15,
        timeout=60 * 60,
    )

    submit >> wait_done
