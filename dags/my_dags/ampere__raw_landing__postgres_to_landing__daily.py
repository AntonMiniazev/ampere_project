"""Transfer completed PostgreSQL change batches into the Raw Iceberg warehouse."""

from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

from utils.ampere_dag_config import (
    ICEBERG_MUTATION_POOL,
    load_raw_landing_dag_config,
    minio_ssl_enabled,
    standard_default_args,
)
from utils.safe_spark_kubernetes import SafeSparkKubernetesOperator

DAG_ID = "ampere__raw_landing__postgres_to_landing__daily"
DAG_CONFIG = load_raw_landing_dag_config(__file__)

with DAG(
    dag_id=DAG_ID,
    default_args=standard_default_args(),
    schedule=None,
    start_date=datetime(2025, 8, 24),
    tags=["layer:raw", "system:postgres", "system:spark", "system:lakekeeper"],
    catchup=False,
    max_active_tasks=1,
    max_active_runs=1,
    template_searchpath=DAG_CONFIG.template_paths,
) as dag:
    start = EmptyOperator(task_id="start")
    transfer = SafeSparkKubernetesOperator(
        task_id="transfer__completed_source_batches_to_raw",
        pool=ICEBERG_MUTATION_POOL,
        pool_slots=1,
        namespace=DAG_CONFIG.spark_namespace,
        application_file="journal_to_raw_template.yaml",
        params={
            "namespace": DAG_CONFIG.spark_namespace,
            "image": DAG_CONFIG.image,
            "image_pull_policy": DAG_CONFIG.image_pull_policy,
            "service_account": DAG_CONFIG.service_account,
            "pg_host": DAG_CONFIG.pg_host,
            "pg_port": DAG_CONFIG.pg_port,
            "pg_database": DAG_CONFIG.pg_database,
            "warehouse": DAG_CONFIG.warehouse,
            "lakekeeper_uri": DAG_CONFIG.lakekeeper_uri,
            "lakekeeper_oauth_uri": DAG_CONFIG.lakekeeper_oauth_uri,
            "lakekeeper_scope": DAG_CONFIG.lakekeeper_scope,
            "minio_endpoint": DAG_CONFIG.minio_endpoint,
            "minio_ssl_enabled": minio_ssl_enabled(DAG_CONFIG.minio_endpoint),
            "node_selector": DAG_CONFIG.node_selector,
            "driver_cores": DAG_CONFIG.driver_cores,
            "driver_core_request": DAG_CONFIG.driver_core_request,
            "driver_memory": DAG_CONFIG.driver_memory,
            "driver_memory_overhead": DAG_CONFIG.driver_memory_overhead,
            "executor_cores": DAG_CONFIG.executor_cores,
            "executor_core_request": DAG_CONFIG.executor_core_request,
            "executor_memory": DAG_CONFIG.executor_memory,
            "executor_memory_overhead": DAG_CONFIG.executor_memory_overhead,
            "executor_instances": min(DAG_CONFIG.executor_instances, 2),
            "shuffle_partitions": DAG_CONFIG.shuffle_partitions,
        },
        kubernetes_conn_id="kubernetes_default",
        get_logs=True,
        base_container_status_polling_interval=30,
        log_events_on_failure=True,
        do_xcom_push=False,
    )
    trigger_bronze = TriggerDagRunOperator(
        task_id="trigger__iceberg__bronze__raw_to_iceberg__daily",
        trigger_dag_id="ampere__iceberg__bronze__raw_to_iceberg__daily",
        logical_date="{{ (dag_run.logical_date or dag_run.run_after).isoformat() }}",
        reset_dag_run=True,
        wait_for_completion=False,
    )
    done = EmptyOperator(task_id="done")
    start >> transfer >> trigger_bronze >> done
