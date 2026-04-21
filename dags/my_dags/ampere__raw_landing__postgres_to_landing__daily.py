from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)
from airflow.providers.standard.operators.python import PythonOperator

from utils.ampere_dag_config import (
    get_optional_nonnegative_int_variable,
    load_raw_landing_dag_config,
    minio_ssl_enabled,
    standard_default_args,
)
from utils.stream_group_config import build_raw_stream_groups

DAG_ID = "ampere__raw_landing__postgres_to_landing__daily"
DAG_CONFIG = load_raw_landing_dag_config(__file__)
SPARK_APP_TEMPLATE = "source_to_raw_template.yaml"
MAX_SNAPSHOT_EXECUTORS = 1
MAX_FACTS_EVENTS_EXECUTORS = 2


def _base_params() -> dict:
    """Assemble the common parameter set shared by every source-to-raw Spark app."""
    return {
        "namespace": DAG_CONFIG.spark_namespace,
        "image": DAG_CONFIG.image,
        "image_pull_policy": DAG_CONFIG.image_pull_policy,
        "service_account": DAG_CONFIG.service_account,
        "schema": DAG_CONFIG.schema,
        "bucket": DAG_CONFIG.minio_bucket,
        "output_prefix": DAG_CONFIG.output_prefix,
        "source_system": DAG_CONFIG.source_system,
        "pg_host": DAG_CONFIG.pg_host,
        "pg_port": DAG_CONFIG.pg_port,
        "pg_database": DAG_CONFIG.pg_database,
        "minio_endpoint": DAG_CONFIG.minio_endpoint,
        "minio_ssl_enabled": minio_ssl_enabled(DAG_CONFIG.minio_endpoint),
        "driver_cores": DAG_CONFIG.driver_cores,
        "driver_core_request": DAG_CONFIG.driver_core_request,
        "driver_memory": DAG_CONFIG.driver_memory,
        "driver_memory_overhead": DAG_CONFIG.driver_memory_overhead,
        "executor_cores": DAG_CONFIG.executor_cores,
        "executor_core_request": DAG_CONFIG.executor_core_request,
        "executor_memory": DAG_CONFIG.executor_memory,
        "executor_memory_overhead": DAG_CONFIG.executor_memory_overhead,
        "executor_instances": DAG_CONFIG.executor_instances,
        "jdbc_fetchsize": DAG_CONFIG.jdbc_fetchsize,
        "shuffle_partitions": DAG_CONFIG.shuffle_partitions,
    }


def _resolve_lookback_overrides() -> dict[str, int | None]:
    """Collect optional per-group recovery windows from Airflow variables."""
    events_override = get_optional_nonnegative_int_variable("spark_events_lookback_days")
    if events_override is None:
        events_override = get_optional_nonnegative_int_variable("spark_event_lookback_days")
    return {
        "facts": get_optional_nonnegative_int_variable("spark_facts_lookback_days"),
        "events": events_override,
        "mutable_dims": get_optional_nonnegative_int_variable(
            "spark_mutable_dims_lookback_days"
        ),
    }


def _group_pair_lookback_days(groups_config: list[dict]) -> int:
    """Return the widest lookback used by a combined Spark submission."""
    return max(
        (int(group.get("lookback_days", 0) or 0) for group in groups_config),
        default=0,
    )


with DAG(
    dag_id=DAG_ID,
    default_args=standard_default_args(),
    schedule=None,
    start_date=datetime(2025, 8, 24),
    tags=[
        "layer:raw_landing",
        "system:postgres",
        "system:spark",
        "system:minio",
        "mode:daily",
    ],
    catchup=False,
    max_active_tasks=DAG_CONFIG.max_active_tasks,
    template_searchpath=DAG_CONFIG.template_paths,
) as dag:
    # Boundary marker before submitting Spark workloads.
    start_batch_task = PythonOperator(
        task_id="run__sparkapp__start",
        python_callable=print,
        op_args=["##### startBatch #####"],
    )

    # Boundary marker after all raw batches have been extracted.
    done_task = PythonOperator(
        task_id="run__sparkapp__done",
        python_callable=print,
        op_args=["##### done #####"],
    )

    # After raw landing is written successfully, Airflow hands control to the
    # bronze DAG that converts validated raw batches into Delta tables.
    trigger_bronze = TriggerDagRunOperator(
        task_id="trigger__bronze__landing_to_delta__daily",
        trigger_dag_id="ampere__bronze__landing_to_delta__daily",
        logical_date="{{ (dag_run.logical_date or dag_run.run_after).isoformat() }}",
        reset_dag_run=True,
        wait_for_completion=False,
    )

    submit_tasks = []
    base_params = _base_params()

    # Convert JSON config into two execution bundles so related table families
    # can be extracted together with matching Spark sizing.
    stream_groups = build_raw_stream_groups(_resolve_lookback_overrides())
    group_map = {group["group"]: group for group in stream_groups}
    for name, group in group_map.items():
        group["shuffle_partitions"] = (
            1 if name == "snapshots" else DAG_CONFIG.shuffle_partitions
        )

    group_pairs = [
        ("snapshots-mutable-dims", ["snapshots", "mutable_dims"]),
        ("facts-events", ["facts", "events"]),
    ]
    # Each loop iteration creates one SparkApplication submission that may cover
    # multiple logical table groups. Pairing related groups keeps the Airflow
    # graph smaller and lets us size one Spark job for a family of tables that
    # have similar extraction behavior.
    for group_name, group_keys in group_pairs:
        groups_config = [group_map[key] for key in group_keys if key in group_map]
        if not groups_config:
            continue
        seed_group = groups_config[0]
        executor_instances = (
            min(DAG_CONFIG.executor_instances_snapshots, MAX_SNAPSHOT_EXECUTORS)
            if group_name == "snapshots-mutable-dims"
            else min(
                DAG_CONFIG.executor_instances_facts_events,
                MAX_FACTS_EVENTS_EXECUTORS,
            )
        )
        executor_memory = (
            DAG_CONFIG.executor_memory
            if group_name == "snapshots-mutable-dims"
            else DAG_CONFIG.executor_memory_facts_events
        )
        executor_memory_overhead = (
            DAG_CONFIG.executor_memory_overhead
            if group_name == "snapshots-mutable-dims"
            else DAG_CONFIG.executor_memory_overhead_facts_events
        )
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
            "lookback_days": _group_pair_lookback_days(groups_config),
            "snapshot_partitioned": seed_group.get("snapshot_partitioned", "true"),
            "app_name": f"source-to-raw-{group_name}",
            "executor_instances": executor_instances,
            "executor_memory": executor_memory,
            "executor_memory_overhead": executor_memory_overhead,
        }
        submit_tasks.append(
            SparkKubernetesOperator(
                task_id=f"run__sparkapp__group_{group_name}",
                namespace=DAG_CONFIG.spark_namespace,
                application_file=SPARK_APP_TEMPLATE,
                params=params,
                kubernetes_conn_id="kubernetes_default",
                do_xcom_push=False,
            )
        )

    # Run raw SparkApplications in parallel: node4 now has enough headroom for
    # one snapshots/mutable-dims app and one facts/events app concurrently.
    start_batch_task >> submit_tasks
    for task in submit_tasks:
        task >> done_task
    done_task >> trigger_bronze
