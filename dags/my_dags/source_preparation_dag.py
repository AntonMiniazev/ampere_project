from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret
from kubernetes.client import V1LocalObjectReference, V1ResourceRequirements

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "retries": 0,
}

# Namespace where the pod runs; affects K8s placement and secret lookup.
NAMESPACE = Variable.get("cluster_namespace", default_var="ampere")
# Target node hostname; controls scheduling affinity.
NODE_SELECTOR = {
    "kubernetes.io/hostname": Variable.get(
        "source_prep_node", default_var="ampere-k8s-node3"
    )
}
# Container image for init job; changing it switches the code version.
IMAGE = Variable.get(
    "init_source_preparation_image",
    default_var="ghcr.io/antonminiazev/init-source-preparation:latest",
)
# Pull policy for image refresh behavior (e.g., Always vs IfNotPresent).
IMAGE_PULL_POLICY = Variable.get(
    "image_pull_policy",
    default_var="IfNotPresent",
)


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
    dag_id="source_preparation_dag",
    default_args=default_args,
    schedule=None,
    catchup=False,
    description="Initial data source creation and dictionary population with data stored in excel tables. Executed one time only, after that data generated in orders_clients_generation.",
    tags=["init", "source_layer", "database", "prod"],
) as dag:
    init_data_task = KubernetesPodOperator(
        task_id="initialize_data_sources",
        name="init-source-preparation",
        namespace=NAMESPACE,
        node_selector=NODE_SELECTOR,
        image=IMAGE,
        image_pull_policy=IMAGE_PULL_POLICY,
        image_pull_secrets=[V1LocalObjectReference(name="ghcr-pull")],
        secrets=[pg_user, pg_pass],
        # Use logical date as base for generated registrations.
        env_vars={
            "PROJECT_START_DATE": "{{ ds }}",
        },
        container_resources=V1ResourceRequirements(
            requests={"cpu": "200m", "memory": "512Mi"},
            limits={"cpu": "1", "memory": "1Gi"},
        ),
        get_logs=True,
        is_delete_operator_pod=True,
    )

    init_data_task
