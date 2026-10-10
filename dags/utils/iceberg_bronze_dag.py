"""Construct daily and historical Raw-to-Bronze DAG entrypoints."""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import Variable

from utils.ampere_dag_config import load_bronze_dag_config, minio_ssl_enabled, resolve_spark_image, standard_default_args
from utils.safe_spark_kubernetes import SafeSparkKubernetesOperator
from utils.stream_group_config import build_bronze_stream_groups


def build_bronze_dag(*, full_rebuild: bool) -> DAG:
    """Create a Bronze daily or all-history run and hand it to Silver."""
    run_name = "full_rebuild" if full_rebuild else "daily"
    dag_id = f"ampere__iceberg__bronze__raw_to_iceberg__{run_name}"
    config = load_bronze_dag_config(__file__)
    base = {
        "namespace": config.spark_namespace,
        "image": resolve_spark_image(),
        "image_pull_policy": "Always",
        "service_account": config.service_account,
        "schema": config.schema,
        "raw_bucket": config.raw_bucket,
        "raw_prefix": config.raw_prefix,
        "source_system": config.source_system,
        "minio_endpoint": config.minio_endpoint,
        "minio_ssl_enabled": minio_ssl_enabled(config.minio_endpoint),
        "driver_cores": config.driver_cores,
        "driver_core_request": config.driver_core_request,
        "driver_memory": config.driver_memory,
        "driver_memory_overhead": config.driver_memory_overhead,
        "driver_node_selector": config.driver_node_selector,
        "executor_cores": config.executor_cores,
        "executor_core_request": config.executor_core_request,
        "executor_memory": config.executor_memory,
        "executor_memory_overhead": config.executor_memory_overhead,
        "executor_instances": config.executor_instances,
        "shuffle_partitions": config.shuffle_partitions,
        "lakekeeper_uri": Variable.get("iceberg_lakekeeper_uri", default="http://lakekeeper.ampere.svc.cluster.local:8181/catalog"),
        "lakekeeper_warehouse": Variable.get("iceberg_bronze_warehouse", default="bronze"),
        "lakekeeper_oauth_uri": Variable.get("iceberg_lakekeeper_oauth_uri", default="https://login.microsoftonline.com/2ffd8fbf-1421-46ed-9e6b-021c955dbe34/oauth2/v2.0/token"),
        "lakekeeper_scope": Variable.get("iceberg_lakekeeper_scope", default="api://4120baf5-d479-464e-8c83-a96b5d475fdf/.default"),
        "rebuild_all": full_rebuild,
    }
    group_map = {group["group"]: group for group in build_bronze_stream_groups({})}
    task_groups = []
    for task_name, keys in (("snapshots", ["snapshots"]), ("mutable-dims", ["mutable_dims"]), ("facts-events", ["facts", "events"])):
        selected = [group_map[key] for key in keys if key in group_map]
        if not selected:
            continue
        for group in selected:
            group["shuffle_partitions"] = (
                config.shuffle_partitions_facts_events
                if group["group"] in {"facts", "events"}
                else config.shuffle_partitions_mutable_dims
                if group["group"] == "mutable_dims"
                else config.shuffle_partitions
            )
            if group["group"] in {"facts", "events"}:
                group["files_max_partition_bytes"] = config.files_max_partition_bytes_facts_events
                group["files_open_cost_bytes"] = config.files_open_cost_bytes_facts_events
                group["adaptive_coalesce"] = config.adaptive_coalesce_facts_events
        task_groups.append((task_name, selected))

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
        start = EmptyOperator(task_id="start")
        done = EmptyOperator(task_id="done")
        tasks = []
        for name, groups in task_groups:
            params = {
                **base,
                "stream": name,
                "groups_config": groups,
                "app_name": f"raw-to-iceberg-{name}-{run_name}",
                "executor_instances": config.executor_instances_facts_events if name == "facts-events" else config.executor_instances_snapshots,
                "executor_cores": config.executor_cores_facts_events if name == "facts-events" else config.executor_cores,
                "executor_core_request": config.executor_core_request_facts_events if name == "facts-events" else config.executor_core_request,
                "executor_memory": config.executor_memory_facts_events if name == "facts-events" else config.executor_memory_snapshots,
                "executor_memory_overhead": config.executor_memory_overhead_facts_events if name == "facts-events" else config.executor_memory_overhead,
                "shuffle_partitions": config.shuffle_partitions,
            }
            tasks.append(
                SafeSparkKubernetesOperator(
                    task_id=f"run__sparkapp__group_{name}",
                    namespace=config.spark_namespace,
                    application_file="raw_to_bronze_template_iceberg.yaml",
                    params=params,
                    kubernetes_conn_id="kubernetes_default",
                    get_logs=True,
                    base_container_status_polling_interval=30,
                    log_events_on_failure=True,
                    do_xcom_push=False,
                )
            )
        handoff = TriggerDagRunOperator(
            task_id=f"trigger__iceberg__silver__dbt_duckdb__{run_name}",
            trigger_dag_id=f"ampere__iceberg__silver__dbt_duckdb__{run_name}",
            logical_date="{{ (dag_run.logical_date or dag_run.run_after).isoformat() }}",
            reset_dag_run=True,
            wait_for_completion=True,
        )
        previous = start
        for task in tasks:
            previous >> task
            previous = task
        previous >> done >> handoff
    return dag
