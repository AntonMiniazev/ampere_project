from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import BranchPythonOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.task.trigger_rule import TriggerRule
from kubernetes.client import V1LocalObjectReference, V1ResourceRequirements

from utils.ampere_dag_config import (
    get_optional_variable,
    load_bronze_cleanup_dag_config,
    standard_default_args,
)
from utils.stream_group_config import build_bronze_stream_groups

DAG_ID = "ampere__housekeeping__bronze_delta_cleanup__weekly"
DAG_CONFIG = load_bronze_cleanup_dag_config()


def _is_truthy(value: str | None) -> bool:
    """Parse an optional Airflow variable into a boolean flag."""
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def _select_cleanup_task(**context) -> str:
    """Run cleanup on Sundays or when bronze_optimization is enabled."""
    optimization_enabled = _is_truthy(get_optional_variable("bronze_optimization"))
    logical_dt = context["dag_run"].logical_date or context["dag_run"].run_after
    if optimization_enabled or logical_dt.weekday() == 6:
        return "cleanup__bronze_delta__retention"
    return "skip__bronze_delta__retention"


def _tables_for_group(group_name: str) -> list[str]:
    """Return Bronze table names for one logical stream group."""
    for group in build_bronze_stream_groups():
        if group.get("group") == group_name:
            return list(group.get("tables", []))
    return []


def _chunks(values: list[str], chunk_size: int) -> list[list[str]]:
    """Split a table list into stable chunks for parallel cleanup pods."""
    return [
        values[index : index + chunk_size]
        for index in range(0, len(values), chunk_size)
    ]


snapshot_tables = _tables_for_group("snapshots")
maintenance_tables = (
    _tables_for_group("mutable_dims")
    + _tables_for_group("facts")
    + _tables_for_group("events")
)
cleanup_chunks = [
    ("snapshots", chunk, [])
    for chunk in _chunks(snapshot_tables, DAG_CONFIG.chunk_size)
] + [
    ("maintenance", [], chunk)
    for chunk in _chunks(maintenance_tables, DAG_CONFIG.chunk_size)
]

minio_access_key = Secret(
    deploy_type="env",
    deploy_target="MINIO_ACCESS_KEY",
    secret="minio-creds",
    key="MINIO_ACCESS_KEY",
)

minio_secret_key = Secret(
    deploy_type="env",
    deploy_target="MINIO_SECRET_KEY",
    secret="minio-creds",
    key="MINIO_SECRET_KEY",
)


with DAG(
    dag_id=DAG_ID,
    default_args=standard_default_args(),
    schedule=None,
    start_date=datetime(2025, 8, 24),
    tags=[
        "layer:housekeeping",
        "system:spark",
        "system:delta",
        "system:minio",
        "mode:weekly",
    ],
    catchup=False,
    max_active_runs=DAG_CONFIG.max_active_runs,
    max_active_tasks=DAG_CONFIG.max_active_tasks,
) as dag:
    # Boundary marker before branch logic decides whether this run should clean.
    start_task = PythonOperator(
        task_id="run__bronze_delta__cleanup_start",
        python_callable=print,
        op_args=["##### startBronzeCleanup #####"],
    )

    select_cleanup = BranchPythonOperator(
        task_id="run__bronze_delta__cleanup_check",
        python_callable=_select_cleanup_task,
    )

    skip_cleanup = EmptyOperator(
        task_id="skip__bronze_delta__retention",
    )

    run_cleanup = EmptyOperator(
        task_id="cleanup__bronze_delta__retention",
    )

    cleanup_tasks = []
    for index, (kind, snapshot_chunk, maintenance_chunk) in enumerate(
        cleanup_chunks,
        start=1,
    ):
        cleanup_tasks.append(
            KubernetesPodOperator(
                task_id=f"cleanup__bronze_delta__{kind}_chunk_{index}",
                name=f"ampere-bronze-delta-cleanup-{kind}-{index}",
                namespace=DAG_CONFIG.namespace,
                image=DAG_CONFIG.image,
                image_pull_policy=DAG_CONFIG.image_pull_policy,
                image_pull_secrets=[V1LocalObjectReference(name="ghcr-pull")],
                service_account_name=DAG_CONFIG.service_account,
                node_selector=DAG_CONFIG.node_selector,
                secrets=[minio_access_key, minio_secret_key],
                # Run as a Spark Connect client. Do not use spark-submit here
                # because it injects spark.master=local[*], which conflicts
                # with remote().
                cmds=["python3", "/opt/spark/app/bronze_cleanup_connect.py"],
                arguments=[
                    "--spark-remote",
                    DAG_CONFIG.spark_remote,
                    "--uc-catalog",
                    DAG_CONFIG.uc_catalog,
                    "--uc-bronze-schema",
                    DAG_CONFIG.uc_bronze_schema,
                    "--run-date",
                    "{{ (dag_run.logical_date or dag_run.run_after).strftime('%Y-%m-%d') }}",
                    "--retention-days",
                    str(DAG_CONFIG.retention_days),
                    "--snapshot-tables",
                    ",".join(snapshot_chunk),
                    "--maintenance-tables",
                    ",".join(maintenance_chunk),
                ],
                container_resources=V1ResourceRequirements(
                    requests={
                        "cpu": DAG_CONFIG.client_cpu_request,
                        "memory": DAG_CONFIG.client_memory_request,
                    },
                    limits={
                        "cpu": DAG_CONFIG.client_cpu_limit,
                        "memory": DAG_CONFIG.client_memory_limit,
                    },
                ),
                get_logs=True,
                is_delete_operator_pod=True,
            )
        )

    done_task = EmptyOperator(
        task_id="run__bronze_delta__cleanup_done",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    start_task >> select_cleanup
    select_cleanup >> skip_cleanup >> done_task
    select_cleanup >> run_cleanup >> cleanup_tasks >> done_task
