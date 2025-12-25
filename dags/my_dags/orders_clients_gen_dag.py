from datetime import datetime
import json

from airflow import DAG
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret
from kubernetes.client import V1LocalObjectReference, V1ResourceRequirements

DAG_ID = "orders_clients_generation"

# Namespace where the pod runs; affects K8s placement and secret lookup.
NAMESPACE = Variable.get("cluster_namespace", default_var="ampere")
# Target node hostname; controls scheduling affinity.
NODE_SELECTOR = {
    "kubernetes.io/hostname": Variable.get(
        "source_prep_node", default_var="ampere-k8s-node3"
    )
}


# Image map from Airflow variable; allows pinning tags per pipeline.
def _load_image_map() -> dict:
    raw_value = Variable.get("ghcr_images", default_var="{}")
    if isinstance(raw_value, str):
        try:
            return json.loads(raw_value)
        except json.JSONDecodeError:
            return {}
    return raw_value


GHCR_IMAGES = _load_image_map()
# Tag-only map; always compose full image from the tag.
IMAGE_TAG = GHCR_IMAGES.get("orders_clients_generation", "latest")
IMAGE = f"ghcr.io/antonminiazev/order-data-generator:{IMAGE_TAG}"

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
    schedule="0 3 * * *",
    start_date=datetime(2025, 8, 24),
    tags=["prod", "init", "generator", "source_layer"],
    catchup=False,
    max_active_runs=1,
) as dag:
    generate_data = KubernetesPodOperator(
        task_id="generate_source_data",
        name="order-data-generator",
        namespace=NAMESPACE,
        node_selector=NODE_SELECTOR,
        image=IMAGE,
        image_pull_policy="IfNotPresent",
        image_pull_secrets=[V1LocalObjectReference(name="ghcr-pull")],
        secrets=[pg_user, pg_pass],
        cmds=["python", "-m", "order_data_generator"],
        # Use logical date as "today" for order generation.
        arguments=["--run-date", "{{ ds }}"],
        container_resources=V1ResourceRequirements(
            requests={"cpu": "250m", "memory": "1Gi"},
            limits={"cpu": "2", "memory": "3Gi"},
        ),
        get_logs=True,
        is_delete_operator_pod=True,
    )

    trigger_source_to_minio = TriggerDagRunOperator(
        task_id="trigger_source_to_minio",
        trigger_dag_id="source_to_minio",
        logical_date="{{ logical_date }}",
    )

    generate_data >> trigger_source_to_minio
