"""Construct daily and historical Raw-journal-to-Bronze DAG entrypoints."""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.sdk import Variable

from utils.ampere_dag_config import (
    ICEBERG_MUTATION_POOL,
    load_bronze_dag_config,
    minio_ssl_enabled,
    resolve_spark_image,
    standard_default_args,
)
from utils.safe_spark_kubernetes import SafeSparkKubernetesOperator
from utils.stream_group_config import build_bronze_execution_groups


def build_bronze_dag(*, full_rebuild: bool) -> DAG:
    """Apply one accepted Raw boundary in sequential, resource-sized Spark apps."""
    run_name = "full_rebuild" if full_rebuild else "daily"
    dag_id = f"ampere__iceberg__bronze__raw_to_iceberg__{run_name}"
    config = load_bronze_dag_config(__file__)
    execution_groups = build_bronze_execution_groups()
    if full_rebuild:
        snapshots_group, facts_events_group = execution_groups
        fact_tables = facts_events_group["tables"]
        facts_events_group = [
            {
                **facts_events_group,
                "name": f"fact{index}",
                "task_name": f"facts_events__{table}",
                "tables": [table],
                "complete_transfer": index == len(fact_tables) - 1,
            }
            for index, table in enumerate(fact_tables)
        ]
        execution_groups = [snapshots_group, *facts_events_group]
    with DAG(
        dag_id=dag_id,
        default_args=standard_default_args(),
        schedule=None,
        start_date=datetime(2025, 8, 24),
        tags=["layer:bronze", "format:iceberg", "system:spark", f"mode:{run_name}"],
        catchup=False,
        max_active_tasks=1,
        max_active_runs=1,
        template_searchpath=config.template_paths,
    ) as dag:
        tasks = []
        for group in execution_groups:
            facts_events = group["name"].startswith("fact") or group["name"] == "facts_events"
            tasks.append(
                SafeSparkKubernetesOperator(
                    task_id=f"apply__{group.get('task_name', group['name'])}",
                    pool=ICEBERG_MUTATION_POOL,
                    pool_slots=1,
                    namespace=config.spark_namespace,
                    application_file="raw_to_bronze_template_iceberg.yaml",
                    params={
                        "namespace": config.spark_namespace,
                        "image": resolve_spark_image(),
                        "image_pull_policy": "Always",
                        "service_account": config.service_account,
                        "raw_warehouse": Variable.get("iceberg_raw_warehouse", default="raw"),
                        "bronze_warehouse": Variable.get("iceberg_bronze_warehouse", default="bronze"),
                        "lakekeeper_uri": Variable.get(
                            "iceberg_lakekeeper_uri",
                            default="http://lakekeeper.ampere.svc.cluster.local:8181/catalog",
                        ),
                        "lakekeeper_oauth_uri": Variable.get(
                            "iceberg_lakekeeper_oauth_uri",
                            default="https://login.microsoftonline.com/2ffd8fbf-1421-46ed-9e6b-021c955dbe34/oauth2/v2.0/token",
                        ),
                        "lakekeeper_scope": Variable.get(
                            "iceberg_lakekeeper_scope",
                            default="api://4120baf5-d479-464e-8c83-a96b5d475fdf/.default",
                        ),
                        "minio_endpoint": config.minio_endpoint,
                        "pg_host": config.pg_host,
                        "pg_port": config.pg_port,
                        "pg_database": config.pg_database,
                        "minio_ssl_enabled": minio_ssl_enabled(config.minio_endpoint),
                        "driver_cores": config.driver_cores,
                        "driver_core_request": config.driver_core_request,
                        "driver_memory": config.driver_memory,
                        "driver_memory_overhead": config.driver_memory_overhead,
                        "driver_node_selector": config.driver_node_selector,
                        "executor_cores": (
                            config.executor_cores_facts_events
                            if facts_events
                            else config.executor_cores
                        ),
                        "executor_core_request": (
                            config.executor_core_request_facts_events
                            if facts_events
                            else config.executor_core_request
                        ),
                        "executor_memory": (
                            config.executor_memory_facts_events
                            if facts_events
                            else config.executor_memory_snapshots
                        ),
                        "executor_memory_overhead": (
                            config.executor_memory_overhead_facts_events
                            if facts_events
                            else config.executor_memory_overhead
                        ),
                        "executor_instances": (
                            config.executor_instances_facts_events
                            if facts_events
                            else config.executor_instances_snapshots
                        ),
                        "executor_node_selector": config.executor_node_selector,
                        "shuffle_partitions": (
                            config.shuffle_partitions_facts_events
                            if facts_events
                            else config.shuffle_partitions_mutable_dims
                        ),
                        "files_max_partition_bytes": config.files_max_partition_bytes_facts_events,
                        "files_open_cost_bytes": config.files_open_cost_bytes_facts_events,
                        "adaptive_coalesce": (
                            config.adaptive_coalesce_facts_events.lower()
                            if facts_events
                            else "true"
                        ),
                        "rebuild_all": full_rebuild,
                        "tables": group["tables"],
                        "complete_transfer": group["complete_transfer"],
                        "execution_group": group["name"],
                    },
                    kubernetes_conn_id="kubernetes_default",
                    get_logs=True,
                    base_container_status_polling_interval=30,
                    log_events_on_failure=True,
                    do_xcom_push=False,
                )
            )
        tasks[0] >> tasks[1]
        for upstream, downstream in zip(tasks[1:], tasks[2:]):
            upstream >> downstream
    return dag
