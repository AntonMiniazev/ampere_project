"""Manually build isolated Silver and Gold Iceberg tables through dbt v2."""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret
from airflow.sdk import Variable
from kubernetes.client import (
    V1ConfigMapVolumeSource,
    V1Container,
    V1EmptyDirVolumeSource,
    V1LocalObjectReference,
    V1ResourceRequirements,
    V1Volume,
    V1VolumeMount,
)

from utils.ampere_dag_config import load_silver_dag_config, standard_default_args


DAG_ID = "ampere__iceberg__silver_gold__dbt_duckdb__daily"
CONFIG = load_silver_dag_config()


def _secret(key: str, deployment_name: str, env_name: str) -> Secret:
    """Map one Kubernetes Secret key into the dbt pod environment."""
    return Secret(
        deploy_type="env",
        deploy_target=env_name,
        secret=deployment_name,
        key=key,
    )


def _image() -> str:
    """Reject a production dbt image accidentally selected by variable."""
    image = Variable.get(
        "iceberg_dbt_image",
        default="ghcr.io/antonminiazev/ampere-dbt-iceberg:migration-latest",
    )
    if not image.startswith("ghcr.io/antonminiazev/ampere-dbt-iceberg:"):
        raise ValueError("iceberg_dbt_image must name the Iceberg repository")
    return image


with DAG(
    dag_id=DAG_ID,
    default_args=standard_default_args(),
    schedule=None,
    start_date=datetime(2025, 8, 24),
    tags=["layer:silver_gold", "format:iceberg", "system:dbt", "mode:manual"],
    catchup=False,
    max_active_runs=1,
) as dag:
    build = KubernetesPodOperator(
        task_id="run__silver_gold__dbt_build",
        name="ampere-dbt-iceberg-silver-gold",
        namespace=CONFIG.namespace,
        image=_image(),
        image_pull_policy="Always",
        image_pull_secrets=[V1LocalObjectReference(name="ghcr-pull")],
        service_account_name=CONFIG.service_account,
        node_selector=CONFIG.node_selector,
        init_containers=[
            V1Container(
                name="combine-ca-bundle",
                image="python:3.12-alpine",
                command=["sh", "-ec"],
                args=[
                    "cat /etc/ssl/certs/ca-certificates.crt "
                    "/etc/ampere-local-ca/ca.crt > /etc/ampere-ca-bundle/ca.crt"
                ],
                volume_mounts=[
                    V1VolumeMount(name="local-ca-public", mount_path="/etc/ampere-local-ca", read_only=True),
                    V1VolumeMount(name="combined-ca-bundle", mount_path="/etc/ampere-ca-bundle"),
                ],
            )
        ],
        volumes=[
            V1Volume(name="local-ca-public", config_map=V1ConfigMapVolumeSource(name="local-ca-public")),
            V1Volume(name="combined-ca-bundle", empty_dir=V1EmptyDirVolumeSource()),
        ],
        volume_mounts=[
            V1VolumeMount(name="combined-ca-bundle", mount_path="/etc/ampere-ca-bundle", read_only=True),
            # dbt v2's DuckDB driver does not retain ca_cert_file from profile settings.
            V1VolumeMount(
                name="combined-ca-bundle",
                mount_path="/etc/ssl/certs/ca-certificates.crt",
                sub_path="ca.crt",
                read_only=True,
            ),
        ],
        secrets=[
            _secret("MINIO_ACCESS_KEY", "minio-creds", "MINIO_ACCESS_KEY"),
            _secret("MINIO_SECRET_KEY", "minio-creds", "MINIO_SECRET_KEY"),
            _secret("client-id", "lakekeeper-dbt-client", "LAKEKEEPER_CLIENT_ID"),
            _secret("client-secret", "lakekeeper-dbt-client", "LAKEKEEPER_CLIENT_SECRET"),
        ],
        env_vars={
            "DUCKDB_CA_CERT_FILE": "/etc/ampere-ca-bundle/ca.crt",
            "MINIO_S3_ENDPOINT": CONFIG.minio_endpoint,
            "LAKEKEEPER_CATALOG_URI": Variable.get(
                "iceberg_lakekeeper_uri",
                default="http://lakekeeper.ampere.svc.cluster.local:8181/catalog",
            ),
            "LAKEKEEPER_OAUTH_URI": Variable.get(
                "iceberg_lakekeeper_oauth_uri",
                default="https://login.microsoftonline.com/2ffd8fbf-1421-46ed-9e6b-021c955dbe34/oauth2/v2.0/token",
            ),
            "LAKEKEEPER_SCOPE": Variable.get(
                "iceberg_lakekeeper_scope",
                default="api://4120baf5-d479-464e-8c83-a96b5d475fdf/.default",
            ),
            "ICEBERG_BRONZE_WAREHOUSE": Variable.get(
                "iceberg_bronze_warehouse", default="bronze"
            ),
            "ICEBERG_SILVER_WAREHOUSE": Variable.get(
                "iceberg_silver_warehouse", default="silver"
            ),
            "ICEBERG_GOLD_WAREHOUSE": Variable.get(
                "iceberg_gold_warehouse", default="gold"
            ),
            "DBT_THREADS": CONFIG.dbt_threads,
            "DUCKDB_MEMORY_LIMIT": CONFIG.duckdb_memory_limit,
            "SILVER_RUN_MODE": CONFIG.run_mode,
            "SILVER_LOOKBACK_DAYS": CONFIG.lookback_days,
            "GOLD_RUN_MODE": CONFIG.run_mode,
            "GOLD_LOOKBACK_DAYS": CONFIG.lookback_days,
            "LOGICAL_DATE": "{{ (dag_run.logical_date or dag_run.run_after).strftime('%Y-%m-%d') }}",
        },
        arguments=["build"],
        container_resources=V1ResourceRequirements(
            requests={"cpu": CONFIG.cpu_request, "memory": CONFIG.memory_request},
            limits={"cpu": CONFIG.cpu_limit, "memory": CONFIG.memory_limit},
        ),
        get_logs=True,
        is_delete_operator_pod=True,
    )
