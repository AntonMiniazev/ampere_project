"""Manually initialize empty Iceberg namespaces and tables from contract v3."""

from __future__ import annotations

from datetime import datetime

from airflow import DAG

from utils.ampere_dag_config import (
    ICEBERG_MUTATION_POOL,
    load_bronze_dag_config,
    minio_ssl_enabled,
    resolve_spark_image,
    standard_default_args,
)
from utils.safe_spark_kubernetes import SafeSparkKubernetesOperator
from airflow.sdk import Variable


DAG_ID = "ampere__iceberg__catalog__init"
CONFIG = load_bronze_dag_config(__file__)

with DAG(
    dag_id=DAG_ID,
    default_args=standard_default_args(),
    schedule=None,
    start_date=datetime(2025, 8, 24),
    tags=["layer:catalog", "format:iceberg", "system:spark", "mode:manual"],
    catchup=False,
    max_active_runs=1,
    template_searchpath=CONFIG.template_paths,
) as dag:
    initialize = SafeSparkKubernetesOperator(
        task_id="initialize__iceberg__catalog_from_contract_v3",
        pool=ICEBERG_MUTATION_POOL,
        pool_slots=1,
        namespace=CONFIG.spark_namespace,
        application_file="catalog_init_template_iceberg.yaml",
        params={
            "namespace": CONFIG.spark_namespace,
            "image": resolve_spark_image(),
            "service_account": CONFIG.service_account,
            "minio_endpoint": CONFIG.minio_endpoint,
            "minio_ssl_enabled": minio_ssl_enabled(CONFIG.minio_endpoint),
            "driver_cores": CONFIG.driver_cores,
            "driver_core_request": CONFIG.driver_core_request,
            "driver_memory": CONFIG.driver_memory,
            "driver_node_selector": CONFIG.driver_node_selector,
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
            "bronze_warehouse": Variable.get("iceberg_bronze_warehouse", default="bronze"),
            "silver_warehouse": Variable.get("iceberg_silver_warehouse", default="silver"),
            "gold_warehouse": Variable.get("iceberg_gold_warehouse", default="gold"),
        },
        kubernetes_conn_id="kubernetes_default",
        get_logs=True,
        base_container_status_polling_interval=30,
        log_events_on_failure=True,
        do_xcom_push=False,
    )
