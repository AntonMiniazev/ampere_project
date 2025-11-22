from datetime import datetime
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret
from kubernetes.client import V1LocalObjectReference, V1ResourceRequirements
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

DAG_ID = "dbt_processing"
NAMESPACE = "ampere"
IMAGE = "ghcr.io/antonminiazev/ampere_project:latest"

minio_access_key = Secret(
    deploy_type="env",
    deploy_target="MINIO_ACCESS_KEY",
    secret="minio-creds",
    key="MINIO_ACCESS_KEY",
)

minio_secret_key = Secret(
    deploy_type="env",
    deploy_target="MINIO_SECRET_KEY",
    secret="minio-creds",
    key="MINIO_SECRET_KEY",
)

default_args = {"owner": "ampere"}

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["dbt", "raw_layer", "processing"],
) as dag:

    dbt_processing = KubernetesPodOperator(
        task_id="dbt_build_processing",
        name="dbt-build-processing",
        namespace=NAMESPACE,
        image=IMAGE,
        image_pull_secrets=[V1LocalObjectReference(name="ghcr-pull")],
        startup_timeout_seconds=240,

        # Inject MINIO_* from 'minio-creds' and runtime env for the runner
        secrets=[minio_access_key, minio_secret_key],
        env_vars={
            # in-cluster MinIO Service DNS (host:port, no scheme)
            "MINIO_S3_ENDPOINT": "http://minio.ampere.svc.cluster.local:9000",
            # dbt command (select only processing layer)
            "DBT_CMD": "dbt build --project-dir /app/project --profiles-dir /app/profiles --selector processing_flow --no-partial-parse",
            "DBT_TARGET": "prod",
            "THREADS": "4",
            # optional local path for DuckDB file inside the container
            "DUCKDB_PATH": "/app/artifacts/ampere.duckdb",
        },

        #startup_timeout_seconds=600,
        container_resources=V1ResourceRequirements(
            requests={"cpu": "250m", "memory": "1Gi"},
            limits={"cpu": "2", "memory": "3Gi"},
        ),

        # Other defaults
        get_logs=True,
        is_delete_operator_pod=True,
        #on_finish_action = "keep_pod",

        node_selector={"kubernetes.io/hostname": "ampere-k8s-node2"},
    )


    trigger_business_logic_stage = TriggerDagRunOperator(
        task_id="trigger_dbt_business_logic_stage",
        trigger_dag_id="dbt_business_logic_stage",
        logical_date="{{ logical_date }}",
    )

    dbt_processing >> trigger_business_logic_stage