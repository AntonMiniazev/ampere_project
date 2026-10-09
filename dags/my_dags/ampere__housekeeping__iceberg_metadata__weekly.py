"""Compact Iceberg files and clean expired history after the Sunday pipeline."""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.sdk import Variable
from kubernetes.client import V1LocalObjectReference, V1ResourceRequirements

from utils.ampere_dag_config import (
    load_silver_dag_config,
    resolve_spark_image,
    standard_default_args,
)


DAG_ID = "ampere__housekeeping__iceberg_metadata__weekly"
CONFIG = load_silver_dag_config()


with DAG(
    dag_id=DAG_ID,
    default_args=standard_default_args(retries=1),
    schedule=None,
    start_date=datetime(2025, 8, 24),
    tags=["layer:housekeeping", "format:iceberg", "system:spark-connect", "mode:weekly"],
    catchup=False,
    max_active_runs=1,
) as dag:
    clean_iceberg_metadata = KubernetesPodOperator(
        task_id="clean__iceberg__expired_snapshots_and_orphans",
        name="ampere-iceberg-housekeeping",
        namespace=CONFIG.namespace,
        image=resolve_spark_image(),
        image_pull_policy="Always",
        image_pull_secrets=[V1LocalObjectReference(name="ghcr-pull")],
        service_account_name=CONFIG.service_account,
        node_selector=CONFIG.node_selector,
        # Only a client pod starts; maintenance runs on the existing Spark service.
        cmds=["python3", "/opt/spark/app/iceberg_housekeeping_connect.py"],
        arguments=[
            "--spark-remote",
            Variable.get(
                "iceberg_housekeeping_spark_remote",
                default="sc://spark-connect.ampere.svc.cluster.local:15002",
            ),
            "--retention-days",
            "14",
        ] + (
            ["--dry-run"]
            if Variable.get("iceberg_housekeeping_dry_run", default="false")
            .strip()
            .lower()
            in {"1", "true", "yes"}
            else []
        ),
        container_resources=V1ResourceRequirements(
            requests={"cpu": "250m", "memory": "512Mi"},
            limits={"cpu": "1", "memory": "2Gi"},
        ),
        execution_timeout=timedelta(hours=3),
        startup_timeout_seconds=600,
        get_logs=True,
        is_delete_operator_pod=True,
    )
