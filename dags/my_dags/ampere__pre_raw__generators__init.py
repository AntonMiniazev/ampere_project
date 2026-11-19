from datetime import datetime

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret
from kubernetes.client import V1LocalObjectReference, V1ResourceRequirements

from utils.ampere_dag_config import get_optional_variable, load_pre_raw_dag_config, resolve_minio_endpoint

DAG_ID = "ampere__pre_raw__generators__init"
DAG_CONFIG = load_pre_raw_dag_config(
    repository="ghcr.io/antonminiazev/init-source-preparation",
)

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 8, 24),
    "retries": 0,
}

pg_user = Secret(
    deploy_type="env",
    deploy_target="PGUSER",
    secret="pguser",
    key="pguser",
)

pg_pass = Secret(
    deploy_type="env",
    deploy_target="PGPASSWORD",
    secret="pgpass",
    key="pgpass",
)

minio_access_key = Secret(
    deploy_type="env", deploy_target="MINIO_ACCESS_KEY",
    secret="minio-creds", key="MINIO_ACCESS_KEY",
)
minio_secret_key = Secret(
    deploy_type="env", deploy_target="MINIO_SECRET_KEY",
    secret="minio-creds", key="MINIO_SECRET_KEY",
)

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule=None,
    catchup=False,
    max_active_runs=1,
    description=(
        "Reset and seed the selected PostgreSQL source dataset and stage its baseline."
    ),
    tags=["layer:pre_raw", "system:postgres", "mode:init"],
) as dag:
    env_vars = {
        # A destructive source reset is available only when explicitly requested.
        "SOURCE_INIT_MODE": "{{ dag_run.conf.get('source_init_mode', '') if dag_run.conf else '' }}",
        "SCHEMA_INIT": "{{ var.value.get('ampere_bootstrap_schema', 'source') }}",
        "AMPERE_AIRFLOW_RUN_ID": "{{ run_id }}",
        # An empty override lets the bootstrap use its packaged YAML policy.
        "PROJECT_START_DATE": "{{ dag_run.conf.get('project_start_date', '') if dag_run.conf else '' }}",
        "POLARS_MAX_THREADS": "2",
        "OPENBLAS_NUM_THREADS": "1",
        "OMP_NUM_THREADS": "1",
        "MKL_NUM_THREADS": "1",
        "AMPERE_SOURCE_CODE_VERSION": DAG_CONFIG.image,
        "MINIO_S3_ENDPOINT": resolve_minio_endpoint(),
        # An empty v2 bucket fails bootstrap validation, including when it matches Raw.
        "MINIO_STAGING_BUCKET": (
            "{%- set raw_bucket = var.value.get('minio_raw_bucket', '') | trim or 'ampere-iceberg-raw' -%}"
            "{%- if var.value.get('ampere_bootstrap_schema', 'source') == 'ampere_source' -%}"
            "{%- set v2_bucket = var.value.get('ampere_source_staging_bucket', '') | trim -%}"
            "{{ '' if v2_bucket == raw_bucket else v2_bucket }}"
            "{%- else -%}{{ raw_bucket }}{%- endif -%}"
        ),
        "MINIO_STAGING_PREFIX": get_optional_variable("ampere_staging_prefix") or "ampere/source-batches/v1",
        "AMPERE_STAGING_TARGET_FILE_BYTES": get_optional_variable("ampere_staging_target_file_bytes") or str(128 * 1024 * 1024),
    }
    init_client_num = get_optional_variable("init_client_num")
    if init_client_num is not None:
        env_vars["N_OF_INIT_CLIENTS"] = init_client_num
    init_delivery_resource = get_optional_variable("init_delivery_resource")
    if init_delivery_resource is not None:
        env_vars["N_DELIVERY_RESOURCE"] = init_delivery_resource

    # A clean reset creates the new typed baseline in MinIO.
    init_data_task = KubernetesPodOperator(
        task_id="run__pre_raw__init",
        name="init-source-preparation",
        startup_timeout_seconds=240,
        namespace=DAG_CONFIG.namespace,
        node_selector=DAG_CONFIG.node_selector,
        image=DAG_CONFIG.image,
        image_pull_policy="IfNotPresent",
        image_pull_secrets=[V1LocalObjectReference(name="ghcr-pull")],
        secrets=[pg_user, pg_pass, minio_access_key, minio_secret_key],
        env_vars=env_vars,
        container_resources=V1ResourceRequirements(
            requests={"cpu": "1", "memory": "2Gi"},
            limits={"cpu": "2", "memory": "4Gi"},
        ),
        get_logs=True,
        is_delete_operator_pod=True,
    )

    init_data_task
