"""Drop and reinitialize only the contract-managed Bronze tables."""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.sdk import Variable
from kubernetes.client import V1LocalObjectReference, V1ResourceRequirements

from utils.ampere_dag_config import (
    ICEBERG_MUTATION_POOL,
    load_silver_dag_config,
    resolve_spark_connect_client_image,
    standard_default_args,
)


DAG_ID = "ampere__iceberg__bronze__catalog_reset"
CONFIG = load_silver_dag_config()

with DAG(
    dag_id=DAG_ID,
    default_args=standard_default_args(retries=0),
    schedule=None,
    start_date=datetime(2025, 8, 24),
    tags=["layer:bronze", "format:iceberg", "system:spark-connect", "mode:reset"],
    catchup=False,
    max_active_runs=1,
) as dag:
    reset_bronze = KubernetesPodOperator(
        task_id="drop_and_reinitialize__bronze_tables_from_contract_v6",
        name="ampere-iceberg-bronze-catalog-reset",
        namespace=CONFIG.namespace,
        image=resolve_spark_connect_client_image(),
        image_pull_policy="Always",
        image_pull_secrets=[V1LocalObjectReference(name="ghcr-pull")],
        service_account_name=CONFIG.service_account,
        node_selector=CONFIG.node_selector,
        pool=ICEBERG_MUTATION_POOL,
        pool_slots=1,
        cmds=[
            "python3",
            "/opt/ampere/app/initialize_iceberg_catalog.py",
            "--layers",
            "bronze",
            "--reset-bronze",
        ],
        env_vars={
            "SPARK_REMOTE": Variable.get(
                "iceberg_spark_connect_remote",
                default="sc://spark-connect.ampere.svc.cluster.local:15002",
            ),
        },
        container_resources=V1ResourceRequirements(
            requests={"cpu": "250m", "memory": "512Mi"},
            limits={"cpu": "1", "memory": "2Gi"},
        ),
        execution_timeout=timedelta(hours=1),
        startup_timeout_seconds=600,
        get_logs=True,
        is_delete_operator_pod=True,
    )
