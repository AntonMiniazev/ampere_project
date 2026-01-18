from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.models import Variable
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)

DAG_ID = "ampere__bronze__landing_to_delta__daily"

SPARK_NAMESPACE = Variable.get("spark_namespace", default_var="ampere")
SERVICE_ACCOUNT = Variable.get(
    "spark_service_account",
    default_var="spark-operator-spark",
)
DEFAULT_IMAGE = "ghcr.io/antonminiazev/raw-to-bronze-spark:latest"


def _resolve_image(value: str | None) -> str:
    if not value:
        return DEFAULT_IMAGE
    if "/" in value:
        return value
    return f"ghcr.io/antonminiazev/raw-to-bronze-spark:{value}"


IMAGE = _resolve_image(
    Variable.get("spark_etl_image", default_var=None)
    or Variable.get("raw_to_bronze_spark_image", default_var=None)
)
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
DRIVER_CORE_REQUEST = "500m"
DRIVER_MEMORY = Variable.get("spark_driver_memory", default_var="768m")
EXECUTOR_CORES = int(Variable.get("spark_executor_cores", default_var="1"))
EXECUTOR_CORE_REQUEST = "500m"
EXECUTOR_MEMORY = Variable.get("spark_executor_memory", default_var="768m")
EXECUTOR_INSTANCES = int(Variable.get("spark_executor_instances", default_var="4"))
EVENT_LOOKBACK_DAYS = int(
    Variable.get("spark_event_lookback_days", default_var="2")
)
MAX_ACTIVE_TASKS = int(
    Variable.get("spark_raw_to_bronze_max_active_tasks", default_var="4")
)

SPARK_APP_TEMPLATE = "raw_to_bronze_template.yaml"
SPARK_TEMPLATE_PATHS = [
    str(Path(__file__).resolve().parent),
    str(Path(__file__).resolve().parents[1] / "sparkapplications"),
]

SNAPSHOT_TABLES = [
    "stores",
    "zones",
    "product_categories",
    "order_statuses",
    "delivery_type",
    "delivery_costing",
    "assortment",
]

MUTABLE_DIM_TABLES = {
    "clients": {"merge_keys": ["id"]},
    "delivery_resource": {"merge_keys": ["id"]},
    "products": {"merge_keys": ["id", "valid_from"]},
    "costing": {"merge_keys": ["product_id", "store_id", "valid_from"]},
}

FACT_TABLES = {
    "orders": {"merge_keys": ["id"]},
    "order_product": {"merge_keys": ["order_id", "product_id"]},
    "payments": {"merge_keys": ["order_id", "payment_date"]},
}

EVENT_TABLES = {
    "order_status_history": {"merge_keys": ["order_id", "status_datetime"]},
    "delivery_tracking": {"merge_keys": ["order_id", "status_datetime"]},
}


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
        "executor_cores": EXECUTOR_CORES,
        "executor_core_request": EXECUTOR_CORE_REQUEST,
        "executor_memory": EXECUTOR_MEMORY,
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

    stream_groups = [
        {
            "group": "snapshots",
            "mode": "snapshot",
            "partition_key": "snapshot_date",
            "tables": SNAPSHOT_TABLES,
            "table_config": {},
            "event_date_column": "",
            "lookback_days": 0,
        },
        {
            "group": "mutable_dims",
            "mode": "incremental",
            "partition_key": "extract_date",
            "tables": list(MUTABLE_DIM_TABLES.keys()),
            "table_config": MUTABLE_DIM_TABLES,
            "event_date_column": "",
            "lookback_days": 0,
        },
        {
            "group": "facts",
            "mode": "incremental",
            "partition_key": "event_date",
            "tables": list(FACT_TABLES.keys()),
            "table_config": FACT_TABLES,
            "event_date_column": "",
            "lookback_days": 0,
        },
        {
            "group": "events",
            "mode": "incremental",
            "partition_key": "event_date",
            "tables": list(EVENT_TABLES.keys()),
            "table_config": EVENT_TABLES,
            "event_date_column": "",
            "lookback_days": EVENT_LOOKBACK_DAYS,
        },
    ]

    submit_tasks = []
    for group in stream_groups:
        params = {
            **base_params,
            "group": group["group"],
            "tables": ",".join(group["tables"]),
            "table_config": group["table_config"],
            "stream": group["group"],
            "mode": group["mode"],
            "partition_key": group["partition_key"],
            "event_date_column": group["event_date_column"],
            "lookback_days": group["lookback_days"],

            "app_name": f"raw-to-bronze-{group['group']}",
        }
        submit_tasks.append(
            SparkKubernetesOperator(
                task_id=f"run__sparkapp__group_{group['group']}",
                namespace=SPARK_NAMESPACE,
                application_file=SPARK_APP_TEMPLATE,
                params=params,
                kubernetes_conn_id="kubernetes_default",
                do_xcom_push=False,
            )
        )

    start_batch_task >> submit_tasks >> done_task
