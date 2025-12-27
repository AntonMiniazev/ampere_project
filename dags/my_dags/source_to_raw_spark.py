from __future__ import annotations

from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.models import Variable
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import (
    SparkKubernetesSensor,
)

DAG_ID = "source_to_raw_spark"

SPARK_NAMESPACE = Variable.get("spark_namespace", default_var="ampere")
SERVICE_ACCOUNT = Variable.get("spark_service_account", default_var="default")
IMAGE = Variable.get(
    "source_to_raw_spark_image",
    default_var="ghcr.io/antonminiazev/source-to-raw-spark:latest",
)
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
DRIVER_MEMORY = Variable.get("spark_driver_memory", default_var="1g")
EXECUTOR_CORES = int(Variable.get("spark_executor_cores", default_var="1"))
EXECUTOR_MEMORY = Variable.get("spark_executor_memory", default_var="1g")
EXECUTOR_INSTANCES = int(Variable.get("spark_executor_instances", default_var="2"))

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
    "start_date": datetime(2025, 1, 1),
    "retries": 0,
}


def _minio_ssl_enabled(endpoint: str) -> str:
    return "true" if endpoint.startswith("https://") else "false"


with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule=None,
    catchup=False,
    max_active_runs=1,
    template_searchpath=SPARK_TEMPLATE_PATHS,
    tags=["spark", "raw_layer", "source_layer", "prod"],
) as dag:
    for table in TABLES:
        app_name = f"source-to-raw-{table}-{{{{ ts_nodash }}}}"
        params = {
            "app_name": app_name,
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
            "driver_memory": DRIVER_MEMORY,
            "executor_cores": EXECUTOR_CORES,
            "executor_memory": EXECUTOR_MEMORY,
            "executor_instances": EXECUTOR_INSTANCES,
        }

        submit = SparkKubernetesOperator(
            task_id=f"submit_{table}",
            namespace=SPARK_NAMESPACE,
            application_file=SPARK_APP_TEMPLATE,
            params=params,
            do_xcom_push=False,
        )

        monitor = SparkKubernetesSensor(
            task_id=f"monitor_{table}",
            namespace=SPARK_NAMESPACE,
            application_name=app_name,
            poke_interval=30,
            timeout=60 * 60,
            attach_log=True,
        )

        submit >> monitor
