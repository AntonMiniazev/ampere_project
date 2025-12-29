from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)

DAG_ID = "source_to_raw_spark"

SPARK_NAMESPACE = Variable.get("spark_namespace", default_var="ampere")
SERVICE_ACCOUNT = Variable.get(
    "spark_service_account",
    default_var="spark-operator-spark",
)
DEFAULT_IMAGE = "ghcr.io/antonminiazev/source-to-raw-spark:latest"


def _resolve_image(value: str | None) -> str:
    if not value:
        return DEFAULT_IMAGE
    if "/" in value:
        return value
    return f"ghcr.io/antonminiazev/source-to-raw-spark:{value}"


IMAGE = _resolve_image(Variable.get("source_to_raw_spark_image", default_var=None))
IMAGE_PULL_POLICY = Variable.get("image_pull_policy", default_var="IfNotPresent")

PG_HOST = Variable.get("pg_host", default_var="postgres-service")
PG_PORT = Variable.get("pg_port", default_var="5432")
PG_DATABASE = Variable.get("pg_database", default_var="ampere_db")
SCHEMA = Variable.get("pg_schema", default_var="source")

MINIO_ENDPOINT = Variable.get(
    "minio_s3_endpoint",
    default_var="http://minio.ampere.svc.cluster.local:9000",
)
MINIO_BUCKET = Variable.get("minio_raw_bucket", default_var="ampere-raw")
OUTPUT_PREFIX = Variable.get("raw_output_prefix", default_var="source")

DRIVER_CORES = int(Variable.get("spark_driver_cores", default_var="1"))
DRIVER_CORE_REQUEST = Variable.get(
    "spark_driver_core_request", default_var="500m"
)
DRIVER_MEMORY = Variable.get("spark_driver_memory", default_var="1g")
EXECUTOR_CORES = int(Variable.get("spark_executor_cores", default_var="1"))
EXECUTOR_CORE_REQUEST = Variable.get(
    "spark_executor_core_request", default_var="500m"
)
EXECUTOR_MEMORY = Variable.get("spark_executor_memory", default_var="1g")
EXECUTOR_INSTANCES = int(Variable.get("spark_executor_instances", default_var="1"))
MAX_ACTIVE_TASKS = int(
    Variable.get("spark_source_to_raw_max_active_tasks", default_var="3")
)

SPARK_APP_TEMPLATE = "source_to_raw_template.yaml"
SPARK_TEMPLATE_PATHS = [
    str(Path(__file__).resolve().parent),
    str(Path(__file__).resolve().parents[1] / "sparkapplications"),
]

TABLES = [
    "clients",
    "stores",
    "zones",
    "assortment",
    "costing",
    "products",
    "product_categories",
    "payments",
    "delivery_tracking",
    "delivery_resource",
    "delivery_type",
    "delivery_costing",
    "orders",
    "order_product",
    "order_status_history",
    "order_statuses",
]

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime.now() - timedelta(days=1),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "max_active_runs": 1,
    "retries": 0,
}


def startBatch() -> None:
    print("##### startBatch #####")


def done() -> None:
    print("##### done #####")


def _minio_ssl_enabled(endpoint: str) -> str:
    return "true" if endpoint.startswith("https://") else "false"


with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule=None,
    start_date=datetime.now() - timedelta(days=1),
    tags=["spark", "raw_layer", "source_layer", "prod"],
    catchup=False,
    max_active_runs=1,
    max_active_tasks=MAX_ACTIVE_TASKS,
    template_searchpath=SPARK_TEMPLATE_PATHS,
) as dag:
    start_batch_task = PythonOperator(
        task_id="startBatch",
        python_callable=startBatch,
    )

    done_task = PythonOperator(
        task_id="done",
        python_callable=done,
    )

    submit_tasks = []

    for table in TABLES:
        params = {
            "namespace": SPARK_NAMESPACE,
            "image": IMAGE,
            "image_pull_policy": IMAGE_PULL_POLICY,
            "service_account": SERVICE_ACCOUNT,
            "table": table,
            "schema": SCHEMA,
            "bucket": MINIO_BUCKET,
            "output_prefix": OUTPUT_PREFIX,
            "pg_host": PG_HOST,
            "pg_port": PG_PORT,
            "pg_database": PG_DATABASE,
            "minio_endpoint": MINIO_ENDPOINT,
            "minio_ssl_enabled": _minio_ssl_enabled(MINIO_ENDPOINT),
            "driver_cores": DRIVER_CORES,
            "driver_core_request": DRIVER_CORE_REQUEST,
            "driver_memory": DRIVER_MEMORY,
            "executor_cores": EXECUTOR_CORES,
            "executor_core_request": EXECUTOR_CORE_REQUEST,
            "executor_memory": EXECUTOR_MEMORY,
            "executor_instances": EXECUTOR_INSTANCES,
        }

        submit = SparkKubernetesOperator(
            task_id=f"submit_{table}",
            namespace=SPARK_NAMESPACE,
            application_file=SPARK_APP_TEMPLATE,
            params=params,
            kubernetes_conn_id="kubernetes_default",
            do_xcom_push=False,
        )
        submit_tasks.append(submit)

    start_batch_task >> submit_tasks >> done_task
