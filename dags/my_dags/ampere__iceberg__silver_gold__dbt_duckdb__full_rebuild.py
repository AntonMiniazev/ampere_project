"""Rebuild Iceberg Silver and Gold from the complete available Bronze history."""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import Variable
from kubernetes.client import (
    V1ConfigMapVolumeSource,
    V1Container,
    V1EmptyDirVolumeSource,
    V1LocalObjectReference,
    V1PersistentVolumeClaimVolumeSource,
    V1ResourceRequirements,
    V1Volume,
    V1VolumeMount,
)

from utils.ampere_dag_config import (
    load_silver_dag_config,
    resolve_release_image,
    standard_default_args,
)


DAG_ID = "ampere__iceberg__silver_gold__dbt_duckdb__full_rebuild"
CONFIG = load_silver_dag_config()


def _secret(key: str, deployment_name: str, env_name: str) -> Secret:
    """Map a Kubernetes Secret key into the dbt pod environment."""
    return Secret(
        deploy_type="env",
        deploy_target=env_name,
        secret=deployment_name,
        key=key,
    )


def _image() -> str:
    """Resolve the Iceberg dbt image override or shared release image."""
    image = Variable.get(
        "iceberg_dbt_image",
        default=resolve_release_image("ghcr.io/antonminiazev/ampere-dbt-iceberg"),
    )
    if not image.startswith("ghcr.io/antonminiazev/ampere-dbt-iceberg:"):
        raise ValueError("iceberg_dbt_image must name the Iceberg repository")
    return image


def _catalog_env() -> dict[str, str]:
    """Build the Iceberg catalog, auth, and object-store environment."""
    return {
        "DUCKDB_CA_CERT_FILE": "/etc/ampere-ca-bundle/ca.crt",
        "MINIO_S3_ENDPOINT": CONFIG.minio_endpoint,
        "LAKEKEEPER_CATALOG_URI": Variable.get(
            "iceberg_lakekeeper_uri",
            default="http://lakekeeper.ampere.svc.cluster.local:8181/catalog",
        ),
        "LAKEKEEPER_OAUTH_URI": Variable.get(
            "iceberg_lakekeeper_oauth_uri",
            default=(
                "https://login.microsoftonline.com/2ffd8fbf-1421-46ed-9e6b-"
                "021c955dbe34/oauth2/v2.0/token"
            ),
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
        "DBT_THREADS": Variable.get("iceberg_full_rebuild_dbt_threads", default="1"),
        "DUCKDB_MEMORY_LIMIT": Variable.get(
            "iceberg_full_rebuild_duckdb_memory_limit", default="7GB"
        ),
        "DUCKDB_WORKER_THREADS": Variable.get(
            "iceberg_full_rebuild_duckdb_threads", default="2"
        ),
        "DUCKDB_PRESERVE_INSERTION_ORDER": "false",
        "DUCKDB_MAX_TEMP_DIRECTORY_SIZE": Variable.get(
            "iceberg_full_rebuild_duckdb_max_temp_directory_size", default="12GB"
        ),
        "ICEBERG_PUBLISH_MODE": Variable.get(
            "iceberg_full_rebuild_publish_mode", default="staged"
        ),
        "ICEBERG_FULL_STAGE_MIN_FREE_GB": Variable.get(
            "iceberg_full_rebuild_min_scratch_gb", default="16"
        ),
        "SILVER_RUN_MODE": "full_history",
        "SILVER_LOOKBACK_DAYS": Variable.get(
            "iceberg_silver_lookback_days", default="7"
        ),
        "GOLD_RUN_MODE": "full_history",
        "GOLD_LOOKBACK_DAYS": Variable.get(
            "iceberg_gold_lookback_days", default="7"
        ),
        "LOGICAL_DATE": "{{ (dag_run.logical_date or dag_run.run_after).strftime('%Y-%m-%d') }}",
    }


def _scratch_volume() -> V1Volume | None:
    """Give staged runs pod-local scratch, with an optional dedicated PVC."""
    claim = Variable.get("iceberg_full_rebuild_scratch_pvc", default="").strip()
    if claim:
        return V1Volume(
            name="dbt-scratch",
            persistent_volume_claim=V1PersistentVolumeClaimVolumeSource(
                claim_name=claim
            ),
        )
    if Variable.get("iceberg_full_rebuild_publish_mode", default="staged") != "staged":
        return None
    return V1Volume(
        name="dbt-scratch",
        empty_dir=V1EmptyDirVolumeSource(size_limit="24Gi"),
    )


SCRATCH_VOLUME = _scratch_volume()
STAGED_LOCAL_SCRATCH = (
    SCRATCH_VOLUME is not None and SCRATCH_VOLUME.empty_dir is not None
)


with DAG(
    dag_id=DAG_ID,
    default_args=standard_default_args(),
    schedule=None,
    start_date=datetime(2025, 8, 24),
    tags=[
        "layer:silver_gold",
        "format:iceberg",
        "system:dbt",
        "mode:full-rebuild",
    ],
    catchup=False,
    max_active_runs=1,
) as dag:
    build = KubernetesPodOperator(
        task_id="run__silver_gold__dbt_full_rebuild",
        name="ampere-dbt-iceberg-silver-gold-full-rebuild",
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
                    V1VolumeMount(
                        name="local-ca-public",
                        mount_path="/etc/ampere-local-ca",
                        read_only=True,
                    ),
                    V1VolumeMount(
                        name="combined-ca-bundle", mount_path="/etc/ampere-ca-bundle"
                    ),
                ],
            )
        ],
        volumes=[
            V1Volume(
                name="local-ca-public",
                config_map=V1ConfigMapVolumeSource(name="local-ca-public"),
            ),
            V1Volume(name="combined-ca-bundle", empty_dir=V1EmptyDirVolumeSource()),
        ] + ([SCRATCH_VOLUME] if SCRATCH_VOLUME else []),
        volume_mounts=[
            V1VolumeMount(
                name="combined-ca-bundle",
                mount_path="/etc/ampere-ca-bundle",
                read_only=True,
            ),
            V1VolumeMount(
                name="combined-ca-bundle",
                mount_path="/etc/ssl/certs/ca-certificates.crt",
                sub_path="ca.crt",
                read_only=True,
            ),
        ] + ([V1VolumeMount(name="dbt-scratch", mount_path="/app/artifacts")]
             if SCRATCH_VOLUME else []),
        secrets=[
            _secret("MINIO_ACCESS_KEY", "minio-creds", "MINIO_ACCESS_KEY"),
            _secret("MINIO_SECRET_KEY", "minio-creds", "MINIO_SECRET_KEY"),
            _secret("client-id", "lakekeeper-dbt-client", "LAKEKEEPER_CLIENT_ID"),
            _secret(
                "client-secret", "lakekeeper-dbt-client", "LAKEKEEPER_CLIENT_SECRET"
            ),
        ],
        env_vars=_catalog_env(),
        # Every Iceberg model is a table, view, or table rebuild; full_history
        # removes the daily source filters, so dbt's incremental --full-refresh
        # flag is neither required nor applicable here.
        arguments=["build"],
        container_resources=V1ResourceRequirements(
            requests={
                "cpu": Variable.get(
                    "iceberg_full_rebuild_dbt_cpu_request", default="1"
                ),
                "memory": Variable.get(
                    "iceberg_full_rebuild_dbt_pod_memory_request", default="6Gi"
                ),
                **({"ephemeral-storage": "16Gi"} if STAGED_LOCAL_SCRATCH else {}),
            },
            limits={
                "cpu": Variable.get(
                    "iceberg_full_rebuild_dbt_cpu_limit", default="4"
                ),
                "memory": Variable.get(
                    "iceberg_full_rebuild_dbt_pod_memory_limit", default="11Gi"
                ),
                **({"ephemeral-storage": "24Gi"} if STAGED_LOCAL_SCRATCH else {}),
            },
        ),
        get_logs=True,
        is_delete_operator_pod=True,
    )

    trigger_curie_iceberg_cache_refresh = TriggerDagRunOperator(
        task_id="trigger__curie__cache_refresh__post_iceberg_gold",
        trigger_dag_id="ampere__curie__cache_refresh__post_iceberg_gold",
        logical_date="{{ (dag_run.logical_date or dag_run.run_after).isoformat() }}",
        reset_dag_run=True,
        wait_for_completion=False,
    )

    build >> trigger_curie_iceberg_cache_refresh
