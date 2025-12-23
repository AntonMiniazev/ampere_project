from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret
from kubernetes.client import V1LocalObjectReference, V1ResourceRequirements

DAG_ID = "orders_clients_generation"

NAMESPACE = Variable.get("cluster_namespace", default_var="ampere")
IMAGE = Variable.get(
    "order_data_generator_image",
    default_var="ghcr.io/antonminiazev/order-data-generator:latest",
)

pg_user = Secret(
    deploy_type="env",
    deploy_target="PGUSER",
    secret="pguser",
    key="PGUSER",
)

pg_pass = Secret(
    deploy_type="env",
    deploy_target="PGPASSWORD",
    secret="pgpass",
    key="PGPASSWORD",
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
        image=IMAGE,
        image_pull_secrets=[V1LocalObjectReference(name="ghcr-pull")],
        secrets=[pg_user, pg_pass],
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

