"""Apply completed Raw batches to Bronze Iceberg tables."""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import Variable

from utils.ampere_dag_config import (
    load_bronze_dag_config,
    minio_ssl_enabled,
    resolve_spark_image,
    standard_default_args,
)
from utils.safe_spark_kubernetes import SafeSparkKubernetesOperator
from utils.stream_group_config import build_bronze_stream_groups


DAG_ID = "ampere__iceberg__bronze__raw_to_iceberg__daily"
CONFIG = load_bronze_dag_config(__file__)
TEMPLATE = "raw_to_bronze_template_iceberg.yaml"


def _image() -> str:
    """Resolve the same Spark image used by Raw landing."""
    return resolve_spark_image()


def _base_params() -> dict:
    """Assemble Spark resources and Lakekeeper catalog settings."""
    return {
        "namespace": CONFIG.spark_namespace,
        "image": _image(),
        "image_pull_policy": "Always",
        "service_account": CONFIG.service_account,
        "schema": CONFIG.schema,
        "raw_bucket": CONFIG.raw_bucket,
        "raw_prefix": CONFIG.raw_prefix,
        "source_system": CONFIG.source_system,
        "minio_endpoint": CONFIG.minio_endpoint,
        "minio_ssl_enabled": minio_ssl_enabled(CONFIG.minio_endpoint),
        "driver_cores": CONFIG.driver_cores,
        "driver_core_request": CONFIG.driver_core_request,
        "driver_memory": CONFIG.driver_memory,
        "driver_memory_overhead": CONFIG.driver_memory_overhead,
        "executor_cores": CONFIG.executor_cores,
        "executor_core_request": CONFIG.executor_core_request,
        "executor_memory": CONFIG.executor_memory,
        "executor_memory_overhead": CONFIG.executor_memory_overhead,
        "executor_instances": CONFIG.executor_instances,
        "executor_node_selector": CONFIG.executor_node_selector,
        "shuffle_partitions": CONFIG.shuffle_partitions,
        "lakekeeper_uri": Variable.get(
            "iceberg_lakekeeper_uri",
            default="http://lakekeeper.ampere.svc.cluster.local:8181/catalog",
        ),
        "lakekeeper_warehouse": Variable.get(
            "iceberg_bronze_warehouse", default="bronze"
        ),
        "lakekeeper_oauth_uri": Variable.get(
            "iceberg_lakekeeper_oauth_uri",
            default="https://login.microsoftonline.com/2ffd8fbf-1421-46ed-9e6b-021c955dbe34/oauth2/v2.0/token",
        ),
        "lakekeeper_scope": Variable.get(
            "iceberg_lakekeeper_scope",
            default="api://4120baf5-d479-464e-8c83-a96b5d475fdf/.default",
        ),
    }


with DAG(
    dag_id=DAG_ID,
    default_args=standard_default_args(),
    schedule=None,
    start_date=datetime(2025, 8, 24),
    tags=["layer:bronze", "format:iceberg", "system:spark", "mode:manual"],
    catchup=False,
    max_active_tasks=1,
    template_searchpath=CONFIG.template_paths,
) as dag:
    start = EmptyOperator(task_id="start")
    done = EmptyOperator(task_id="done")
    trigger_silver_gold = TriggerDagRunOperator(
        task_id="trigger__iceberg__silver_gold__dbt_duckdb__daily",
        trigger_dag_id="ampere__iceberg__silver_gold__dbt_duckdb__daily",
        logical_date="{{ (dag_run.logical_date or dag_run.run_after).isoformat() }}",
        reset_dag_run=True,
        wait_for_completion=False,
    )
    groups = {group["group"]: group for group in build_bronze_stream_groups({})}
    tasks = []
    for name, keys in (
        ("snapshots", ["snapshots"]),
        ("mutable-dims", ["mutable_dims"]),
        ("facts-events", ["facts", "events"]),
    ):
        selected = [groups[key] for key in keys if key in groups]
        if not selected:
            continue
        for group in selected:
            group["shuffle_partitions"] = (
                CONFIG.shuffle_partitions_facts_events
                if group["group"] in {"facts", "events"}
                else CONFIG.shuffle_partitions_mutable_dims
                if group["group"] == "mutable_dims"
                else CONFIG.shuffle_partitions
            )
            if group["group"] in {"facts", "events"}:
                group["files_max_partition_bytes"] = (
                    CONFIG.files_max_partition_bytes_facts_events
                )
                group["files_open_cost_bytes"] = CONFIG.files_open_cost_bytes_facts_events
                group["adaptive_coalesce"] = CONFIG.adaptive_coalesce_facts_events
        params = {
            **_base_params(),
            "stream": name,
            "groups_config": selected,
            "app_name": f"raw-to-iceberg-{name}",
            "executor_instances": (
                CONFIG.executor_instances_facts_events
                if name == "facts-events"
                else CONFIG.executor_instances_snapshots
            ),
            "executor_memory": (
                CONFIG.executor_memory_facts_events
                if name == "facts-events"
                else CONFIG.executor_memory_snapshots
            ),
            "executor_memory_overhead": (
                CONFIG.executor_memory_overhead_facts_events
                if name == "facts-events"
                else CONFIG.executor_memory_overhead
            ),
        }
        tasks.append(
            SafeSparkKubernetesOperator(
                task_id=f"run__sparkapp__group_{name}",
                namespace=CONFIG.spark_namespace,
                application_file=TEMPLATE,
                params=params,
                kubernetes_conn_id="kubernetes_default",
                get_logs=True,
                base_container_status_polling_interval=30,
                log_events_on_failure=True,
                do_xcom_push=False,
            )
        )
    previous = start
    for task in tasks:
        previous >> task
        previous = task
    previous >> done >> trigger_silver_gold
