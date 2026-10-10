"""Build independent Silver or Gold dbt DAGs for daily and full-history runs."""

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
    ICEBERG_MUTATION_POOL,
    load_silver_dag_config,
    resolve_release_image,
    standard_default_args,
)


def _secret(key: str, deployment: str, env_name: str) -> Secret:
    return Secret(deploy_type="env", deploy_target=env_name, secret=deployment, key=key)


def build_layer_dag(layer: str, *, full_rebuild: bool) -> DAG:
    """Create one serialized dbt layer DAG and its successful downstream handoff."""
    if layer not in {"silver", "gold"}:
        raise ValueError("Layer must be silver or gold")
    mode = "full_history" if full_rebuild else "daily_refresh"
    run_name = "full_rebuild" if full_rebuild else "daily"
    dag_id = f"ampere__iceberg__{layer}__dbt_duckdb__{run_name}"
    config = load_silver_dag_config()
    image = Variable.get(
        "iceberg_dbt_image",
        default=resolve_release_image("ghcr.io/antonminiazev/ampere-dbt-iceberg"),
    )
    if not image.startswith("ghcr.io/antonminiazev/ampere-dbt-iceberg:"):
        raise ValueError("iceberg_dbt_image must name the Iceberg repository")

    env = {
        "DUCKDB_CA_CERT_FILE": "/etc/ampere-ca-bundle/ca.crt",
        "MINIO_S3_ENDPOINT": config.minio_endpoint,
        "LAKEKEEPER_CATALOG_URI": Variable.get(
            "iceberg_lakekeeper_uri", default="http://lakekeeper.ampere.svc.cluster.local:8181/catalog"
        ),
        "LAKEKEEPER_OAUTH_URI": Variable.get(
            "iceberg_lakekeeper_oauth_uri",
            default="https://login.microsoftonline.com/2ffd8fbf-1421-46ed-9e6b-021c955dbe34/oauth2/v2.0/token",
        ),
        "LAKEKEEPER_SCOPE": Variable.get(
            "iceberg_lakekeeper_scope",
            default="api://4120baf5-d479-464e-8c83-a96b5d475fdf/.default",
        ),
        "ICEBERG_BRONZE_WAREHOUSE": Variable.get("iceberg_bronze_warehouse", default="bronze"),
        "ICEBERG_SILVER_WAREHOUSE": Variable.get("iceberg_silver_warehouse", default="silver"),
        "ICEBERG_GOLD_WAREHOUSE": Variable.get("iceberg_gold_warehouse", default="gold"),
        "ICEBERG_LAYER": layer,
        "ICEBERG_RUN_MODE": mode,
        f"{layer.upper()}_RUN_MODE": mode,
        "ICEBERG_PUBLISH_MODE": "staged",
        "LOGICAL_DATE": "{{ (dag_run.logical_date or dag_run.run_after).strftime('%Y-%m-%d') }}",
        "DUCKDB_PRESERVE_INSERTION_ORDER": "false",
    }
    if full_rebuild:
        env.update(
            {
                "DBT_THREADS": Variable.get("iceberg_full_rebuild_dbt_threads", default="1"),
                "DUCKDB_MEMORY_LIMIT": Variable.get("iceberg_full_rebuild_duckdb_memory_limit", default="7GB"),
                "DUCKDB_WORKER_THREADS": Variable.get("iceberg_full_rebuild_duckdb_threads", default="2"),
                "DUCKDB_MAX_TEMP_DIRECTORY_SIZE": Variable.get("iceberg_full_rebuild_duckdb_max_temp_directory_size", default="12GB"),
                "ICEBERG_FULL_STAGE_MIN_FREE_GB": Variable.get("iceberg_full_rebuild_min_scratch_gb", default="16"),
            }
        )
        scratch_claim = Variable.get("iceberg_full_rebuild_scratch_pvc", default="").strip()
        scratch_volume = (
            V1Volume(
                name="dbt-scratch",
                persistent_volume_claim=V1PersistentVolumeClaimVolumeSource(claim_name=scratch_claim),
            )
            if scratch_claim
            else V1Volume(name="dbt-scratch", empty_dir=V1EmptyDirVolumeSource(size_limit="24Gi"))
        )
        scratch_request = {} if scratch_claim else {"ephemeral-storage": "16Gi"}
        scratch_limit = {} if scratch_claim else {"ephemeral-storage": "24Gi"}
        resources = V1ResourceRequirements(
            requests={
                "cpu": Variable.get("iceberg_full_rebuild_dbt_cpu_request", default="2"),
                "memory": Variable.get("iceberg_full_rebuild_dbt_pod_memory_request", default="6Gi"),
                **scratch_request,
            },
            limits={
                "cpu": Variable.get("iceberg_full_rebuild_dbt_cpu_limit", default="4"),
                "memory": Variable.get("iceberg_full_rebuild_dbt_pod_memory_limit", default="11Gi"),
                **scratch_limit,
            },
        )
    else:
        daily_memory_default = "7GB" if layer == "silver" else "4GB"
        env.update(
            {
                "DBT_THREADS": Variable.get("iceberg_dbt_threads", default="2"),
                "DUCKDB_MEMORY_LIMIT": Variable.get(
                    "iceberg_dbt_duckdb_memory_limit", default=daily_memory_default
                ),
                "DUCKDB_WORKER_THREADS": Variable.get("iceberg_dbt_duckdb_threads", default="3"),
                "DUCKDB_MAX_TEMP_DIRECTORY_SIZE": Variable.get("iceberg_dbt_duckdb_max_temp_directory_size", default=""),
            }
        )
        scratch_volume = V1Volume(name="dbt-scratch", empty_dir=V1EmptyDirVolumeSource(size_limit="12Gi"))
        resources = V1ResourceRequirements(
            requests={
                "cpu": Variable.get("iceberg_dbt_cpu_request", default="2"),
                "memory": Variable.get("iceberg_dbt_pod_memory_request", default="2Gi"),
                "ephemeral-storage": "4Gi",
            },
            limits={
                "cpu": Variable.get("iceberg_dbt_cpu_limit", default="4"),
                "memory": Variable.get("iceberg_dbt_pod_memory_limit", default="10Gi"),
                "ephemeral-storage": "12Gi",
            },
        )

    with DAG(
        dag_id=dag_id,
        default_args=standard_default_args(),
        schedule=None,
        start_date=datetime(2025, 8, 24),
        tags=[f"layer:{layer}", "format:iceberg", "system:dbt", f"mode:{run_name}"],
        catchup=False,
        max_active_runs=1,
    ) as dag:
        build = KubernetesPodOperator(
            task_id=f"run__{layer}__dbt_{run_name}",
            name=f"ampere-dbt-iceberg-{layer}-{run_name}",
            namespace=config.namespace,
            image=image,
            image_pull_policy="Always",
            image_pull_secrets=[V1LocalObjectReference(name="ghcr-pull")],
            service_account_name=config.service_account,
            node_selector=config.node_selector,
            pool=ICEBERG_MUTATION_POOL,
            pool_slots=1,
            init_containers=[
                V1Container(
                    name="combine-ca-bundle",
                    image="alpine:3.22",
                    command=["sh", "-ec"],
                    args=["cat /etc/ssl/certs/ca-certificates.crt /etc/ampere-local-ca/ca.crt > /etc/ampere-ca-bundle/ca.crt"],
                    volume_mounts=[
                        V1VolumeMount(name="local-ca-public", mount_path="/etc/ampere-local-ca", read_only=True),
                        V1VolumeMount(name="combined-ca-bundle", mount_path="/etc/ampere-ca-bundle"),
                    ],
                )
            ],
            volumes=[
                V1Volume(name="local-ca-public", config_map=V1ConfigMapVolumeSource(name="local-ca-public")),
                V1Volume(name="combined-ca-bundle", empty_dir=V1EmptyDirVolumeSource()),
                scratch_volume,
            ],
            volume_mounts=[
                V1VolumeMount(name="combined-ca-bundle", mount_path="/etc/ampere-ca-bundle", read_only=True),
                V1VolumeMount(name="combined-ca-bundle", mount_path="/etc/ssl/certs/ca-certificates.crt", sub_path="ca.crt", read_only=True),
                V1VolumeMount(name="dbt-scratch", mount_path="/app/artifacts"),
            ],
            secrets=[
                _secret("MINIO_ACCESS_KEY", "minio-creds", "MINIO_ACCESS_KEY"),
                _secret("MINIO_SECRET_KEY", "minio-creds", "MINIO_SECRET_KEY"),
                _secret("client-id", "lakekeeper-dbt-client", "LAKEKEEPER_CLIENT_ID"),
                _secret("client-secret", "lakekeeper-dbt-client", "LAKEKEEPER_CLIENT_SECRET"),
            ],
            env_vars=env,
            arguments=["build"],
            container_resources=resources,
            get_logs=True,
            is_delete_operator_pod=True,
        )
        if layer == "silver":
            next_dag = f"ampere__iceberg__gold__dbt_duckdb__{run_name}"
            next_task = f"trigger__iceberg__gold__dbt_duckdb__{run_name}"
        else:
            next_dag = "ampere__curie__cache_refresh__post_iceberg_gold"
            next_task = "trigger__curie__cache_refresh__post_iceberg_gold"
        handoff = TriggerDagRunOperator(
            task_id=next_task,
            trigger_dag_id=next_dag,
            logical_date="{{ (dag_run.logical_date or dag_run.run_after).isoformat() }}",
            reset_dag_run=True,
            wait_for_completion=True,
        )
        build >> handoff
    return dag
