from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.models import Variable
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)

from utils.stream_group_config import build_raw_stream_groups

DAG_ID = "ampere__raw_landing__postgres_to_landing__daily"

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

PG_HOST = Variable.get("pg_host", default_var="postgres-service")
PG_PORT = Variable.get("pg_port", default_var="5432")
PG_DATABASE = Variable.get("pg_database", default_var="ampere_db")
SCHEMA = Variable.get("pg_schema", default_var="source")
SOURCE_SYSTEM = Variable.get("raw_source_system", default_var="postgres-pre-raw")

MINIO_ENDPOINT = Variable.get(
    "minio_s3_endpoint",
    default_var="http://minio.ampere.svc.cluster.local:9000",
)
MINIO_BUCKET = Variable.get("minio_raw_bucket", default_var="ampere-raw")
OUTPUT_PREFIX = Variable.get("raw_output_prefix", default_var="postgres-pre-raw")

DRIVER_CORES = int(Variable.get("spark_driver_cores", default_var="1"))
DRIVER_CORE_REQUEST = Variable.get("spark_driver_core_request", default_var="250m")
DRIVER_MEMORY = Variable.get("spark_driver_memory", default_var="512m")
EXECUTOR_CORES = int(Variable.get("spark_executor_cores", default_var="1"))
EXECUTOR_CORE_REQUEST = Variable.get("spark_executor_core_request", default_var="250m")
EXECUTOR_MEMORY = Variable.get("spark_executor_memory", default_var="512m")
EXECUTOR_INSTANCES = int(Variable.get("spark_executor_instances", default_var="1"))
EVENT_LOOKBACK_DAYS = int(Variable.get("spark_event_lookback_days", default_var="2"))
SHUFFLE_PARTITIONS = int(
    Variable.get("spark_sql_shuffle_partitions", default_var="4")
)
MAX_ACTIVE_TASKS = int(
    Variable.get("spark_source_to_raw_max_active_tasks", default_var="2")
)

SPARK_APP_TEMPLATE = "source_to_raw_template.yaml"
SPARK_TEMPLATE_PATHS = [
    str(Path(__file__).resolve().parent),
    str(Path(__file__).resolve().parents[1] / "sparkapplications"),
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


def _base_params() -> dict:
    return {
        "namespace": SPARK_NAMESPACE,
        "image": IMAGE,
        "image_pull_policy": IMAGE_PULL_POLICY,
        "service_account": SERVICE_ACCOUNT,
        "schema": SCHEMA,
        "bucket": MINIO_BUCKET,
        "output_prefix": OUTPUT_PREFIX,
        "source_system": SOURCE_SYSTEM,
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
        "shuffle_partitions": SHUFFLE_PARTITIONS,
    }


with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule="0 4 * * *",
    start_date=datetime(2025, 8, 24),
    tags=[
        "layer:raw_landing",
        "system:postgres",
        "system:spark",
        "system:minio",
        "mode:daily",
    ],
    catchup=False,
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

    submit_tasks = []
    base_params = _base_params()

    stream_groups = build_raw_stream_groups(EVENT_LOOKBACK_DAYS)
    group_map = {group["group"]: group for group in stream_groups}
    for name, group in group_map.items():
        group["shuffle_partitions"] = (
            1 if name == "snapshots" else SHUFFLE_PARTITIONS
        )

    group_pairs = [
        ("snapshots-mutable-dims", ["snapshots", "mutable_dims"]),
        ("facts-events", ["facts", "events"]),
    ]
    for group_name, group_keys in group_pairs:
        groups_config = [group_map[key] for key in group_keys if key in group_map]
        if not groups_config:
            continue
        seed_group = groups_config[0]
        params = {
            **base_params,
            "group": group_name,
            "tables": "",
            "table_config": {},
            "groups_config": groups_config,
            "stream": group_name,
            "mode": seed_group.get("mode", "snapshot"),
            "partition_key": seed_group.get("partition_key", "snapshot_date"),
            "event_date_column": seed_group.get("event_date_column", ""),
            "watermark_column": seed_group.get("watermark_column", ""),
            "watermark_from": "",
            "watermark_to": "",
            "lookback_days": seed_group.get("lookback_days", 0),
            "snapshot_partitioned": seed_group.get("snapshot_partitioned", "true"),
            "app_name": f"source-to-raw-{group_name}",
        }
        submit_tasks.append(
            SparkKubernetesOperator(
                task_id=f"run__sparkapp__group_{group_name}",
                namespace=SPARK_NAMESPACE,
                application_file=SPARK_APP_TEMPLATE,
                params=params,
                kubernetes_conn_id="kubernetes_default",
                do_xcom_push=False,
            )
        )

    start_batch_task >> submit_tasks >> done_task
