from datetime import datetime

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret
from kubernetes.client import V1LocalObjectReference, V1ResourceRequirements

from utils.ampere_dag_config import get_optional_variable, load_pre_raw_dag_config

DAG_ID = "ampere__pre_raw__generators__init"
DAG_CONFIG = load_pre_raw_dag_config(
    image_name="source_preparation",
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

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule=None,
    catchup=False,
    description=(
        "Initial data source creation and dictionary population with data stored in "
        "excel tables. Executed one time only; daily generation happens in "
        "ampere__pre_raw__generators__daily."
    ),
    tags=["layer:pre_raw", "system:postgres", "mode:init"],
) as dag:
    env_vars = {
        # Use the DAG logical date as the anchor for initial registration dates.
        "PROJECT_START_DATE": "{{ (dag_run.logical_date or dag_run.run_after).strftime('%Y-%m-%d') }}",
    }
    init_client_num = get_optional_variable("init_client_num")
    if init_client_num is not None:
        env_vars["N_OF_INIT_CLIENTS"] = init_client_num
    init_delivery_resource = get_optional_variable("init_delivery_resource")
    if init_delivery_resource is not None:
        env_vars["N_DELIVERY_RESOURCE"] = init_delivery_resource

    # The init pod creates the source schema, dictionary tables, starter client
    # base, courier pool, and assortment. It is the one-time setup step that
    # makes the daily generator and downstream Spark DAGs possible.
    init_data_task = KubernetesPodOperator(
        task_id="run__pre_raw__init",
        name="init-source-preparation",
        startup_timeout_seconds=240,
        namespace=DAG_CONFIG.namespace,
        node_selector=DAG_CONFIG.node_selector,
        image=DAG_CONFIG.image,
        image_pull_policy="IfNotPresent",
        image_pull_secrets=[V1LocalObjectReference(name="ghcr-pull")],
        secrets=[pg_user, pg_pass],
        env_vars=env_vars,
        container_resources=V1ResourceRequirements(
            requests={"cpu": "200m", "memory": "512Mi"},
            limits={"cpu": "1", "memory": "1Gi"},
        ),
        get_logs=True,
        is_delete_operator_pod=True,
    )

    init_data_task
