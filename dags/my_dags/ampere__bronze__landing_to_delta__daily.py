from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.models import Variable
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)

from utils.stream_group_config import build_bronze_stream_groups

DAG_ID = "ampere__bronze__landing_to_delta__daily"

SPARK_NAMESPACE = Variable.get("spark_namespace", default_var="ampere")
SERVICE_ACCOUNT = Variable.get(
    "spark_service_account",
    default_var="spark-operator-spark",
)
DEFAULT_IMAGE = "ghcr.io/antonminiazev/ampere-spark:latest"


def _resolve_image(value: str | None) -> str:
    if not value:
        return DEFAULT_IMAGE
    if "/" in value:
        return value
    return f"ghcr.io/antonminiazev/ampere-spark:{value}"


IMAGE = _resolve_image(Variable.get("ampere-spark-image", default_var=None))
IMAGE_PULL_POLICY = Variable.get("image_pull_policy", default_var="IfNotPresent")

MINIO_ENDPOINT = Variable.get(
    "minio_s3_endpoint",
    default_var="http://minio.ampere.svc.cluster.local:9000",
)
SCHEMA = Variable.get("pg_schema", default_var="source")
RAW_BUCKET = Variable.get("minio_raw_bucket", default_var="ampere-raw")
RAW_PREFIX = Variable.get("raw_output_prefix", default_var="postgres-pre-raw")
BRONZE_BUCKET = Variable.get("minio_bronze_bucket", default_var="ampere-bronze")
BRONZE_PREFIX = Variable.get("bronze_output_prefix", default_var="bronze")
SOURCE_SYSTEM = Variable.get("raw_source_system", default_var="postgres-pre-raw")

DRIVER_CORES = int(Variable.get("spark_driver_cores", default_var="1"))
DRIVER_CORE_REQUEST = Variable.get(
    "spark_driver_core_request", default_var="250m"
)
DRIVER_MEMORY = Variable.get("spark_driver_memory", default_var="512m")
DRIVER_MEMORY_OVERHEAD = Variable.get(
    "spark_driver_memory_overhead", default_var="64m"
)
EXECUTOR_CORES = int(Variable.get("spark_executor_cores", default_var="1"))
EXECUTOR_CORE_REQUEST = Variable.get(
    "spark_executor_core_request", default_var="250m"
)
EXECUTOR_MEMORY = Variable.get("spark_executor_memory", default_var="512m")
EXECUTOR_MEMORY_OVERHEAD = Variable.get(
    "spark_executor_memory_overhead", default_var="64m"
)
EXECUTOR_INSTANCES = int(Variable.get("spark_executor_instances", default_var="3"))
EVENT_LOOKBACK_DAYS = int(Variable.get("spark_event_lookback_days", default_var="2"))
MAX_ACTIVE_TASKS = int(
    Variable.get("spark_raw_to_bronze_max_active_tasks", default_var="2")
)

SPARK_APP_TEMPLATE = "raw_to_bronze_template.yaml"
REGISTRY_INIT_TEMPLATE = "bronze_registry_init.yaml"
SPARK_TEMPLATE_PATHS = [
    str(Path(__file__).resolve().parent),
    str(Path(__file__).resolve().parents[1] / "sparkapplications"),
]



def _minio_ssl_enabled(endpoint: str) -> str:
    return "true" if endpoint.startswith("https://") else "false"


def startBatch() -> None:
    print("##### startBatch #####")


def done() -> None:
    print("##### done #####")


def _base_params() -> dict:
    return {
        "namespace": SPARK_NAMESPACE,
        "image": IMAGE,
        "image_pull_policy": IMAGE_PULL_POLICY,
        "service_account": SERVICE_ACCOUNT,
        "schema": SCHEMA,
        "raw_bucket": RAW_BUCKET,
        "raw_prefix": RAW_PREFIX,
        "bronze_bucket": BRONZE_BUCKET,
        "bronze_prefix": BRONZE_PREFIX,
        "source_system": SOURCE_SYSTEM,
        "minio_endpoint": MINIO_ENDPOINT,
        "minio_ssl_enabled": _minio_ssl_enabled(MINIO_ENDPOINT),
        "driver_cores": DRIVER_CORES,
        "driver_core_request": DRIVER_CORE_REQUEST,
        "driver_memory": DRIVER_MEMORY,
        "driver_memory_overhead": DRIVER_MEMORY_OVERHEAD,
        "executor_cores": EXECUTOR_CORES,
        "executor_core_request": EXECUTOR_CORE_REQUEST,
        "executor_memory": EXECUTOR_MEMORY,
        "executor_memory_overhead": EXECUTOR_MEMORY_OVERHEAD,
        "executor_instances": EXECUTOR_INSTANCES,
    }


with DAG(
    dag_id=DAG_ID,
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "start_date": datetime.now() - timedelta(days=1),
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "max_active_runs": 1,
        "retries": 0,
    },
    schedule=None,
    start_date=datetime.now() - timedelta(days=1),
    tags=[
        "layer:bronze",
        "system:spark",
        "system:minio",
        "mode:daily",
    ],
    catchup=False,
    max_active_runs=1,
    max_active_tasks=MAX_ACTIVE_TASKS,
    template_searchpath=SPARK_TEMPLATE_PATHS,
) as dag:
    start_batch_task = PythonOperator(
        task_id="run__sparkapp__start",
        python_callable=startBatch,
    )

    done_task = PythonOperator(
        task_id="run__sparkapp__done",
        python_callable=done,
    )

    base_params = _base_params()

    stream_groups = build_bronze_stream_groups(EVENT_LOOKBACK_DAYS)

    registry_params = {
        **base_params,
        "app_name": "bronze-registry-init",
        "executor_instances": 1,
    }

    init_registry_task = SparkKubernetesOperator(
        task_id="run__sparkapp__registry_init",
        namespace=SPARK_NAMESPACE,
        application_file=REGISTRY_INIT_TEMPLATE,
        params=registry_params,
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=False,
    )

    submit_tasks = []
    for group in stream_groups:
        params = {
            **base_params,
            "group": group["group"],
            "tables": ",".join(group["tables"]),
            "table_config": group["table_config"],
            "groups_config": [],
            "stream": group["group"],
            "mode": group["mode"],
            "partition_key": group["partition_key"],
            "event_date_column": group["event_date_column"],
            "lookback_days": group["lookback_days"],
            "app_name": f"raw-to-bronze-{group['group']}",
        }
        task = SparkKubernetesOperator(
            task_id=f"run__sparkapp__group_{group['group']}",
            namespace=SPARK_NAMESPACE,
            application_file=SPARK_APP_TEMPLATE,
            params=params,
            kubernetes_conn_id="kubernetes_default",
            do_xcom_push=False,
        )
        submit_tasks.append(task)

    start_batch_task >> init_registry_task >> submit_tasks
    submit_tasks >> done_task
