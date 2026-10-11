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
        "Initial data source creation and dictionary population with data stored in "
        "excel tables. Executed one time only; daily generation happens in "
        "ampere__pre_raw__generators__daily."
    ),
    tags=["layer:pre_raw", "system:postgres", "mode:init"],
) as dag:
    env_vars = {
        # A destructive source reset is available only when explicitly requested.
        "SOURCE_INIT_MODE": "{{ dag_run.conf.get('source_init_mode', '') if dag_run.conf else '' }}",
        "AMPERE_AIRFLOW_RUN_ID": "{{ run_id }}",
        # Allow a one-off historical source timeline while preserving the
        # current-date default for ordinary initialization runs.
        "PROJECT_START_DATE": "{{ dag_run.conf['project_start_date'] if dag_run.conf and 'project_start_date' in dag_run.conf else (dag_run.logical_date or dag_run.run_after).strftime('%Y-%m-%d') }}",
        "POLARS_MAX_THREADS": "2",
        "OPENBLAS_NUM_THREADS": "1",
        "OMP_NUM_THREADS": "1",
        "MKL_NUM_THREADS": "1",
        "AMPERE_SOURCE_CODE_VERSION": DAG_CONFIG.image,
        "MINIO_S3_ENDPOINT": resolve_minio_endpoint(),
        "MINIO_STAGING_BUCKET": get_optional_variable("minio_raw_bucket") or "ampere-raw",
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
