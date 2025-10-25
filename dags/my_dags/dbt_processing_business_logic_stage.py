from datetime import datetime
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret
from kubernetes.client import V1LocalObjectReference, V1ResourceRequirements

DAG_ID = "dbt_processing_business_logic_stage"
NAMESPACE = "ampere"
IMAGE = "ghcr.io/antonminiazev/ampere_project:latest"


# Map existing K8s Secret 'minio-creds' to env vars inside the container
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

mssql_pass = Secret(
    deploy_type="env", 
    deploy_target="MSSQL_PASSWORD",
    secret="mssql-sa-secret", 
    key="SA_PASSWORD"
)

default_args = {
    "owner": "ampere",
    "depends_on_past": False,
}

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 1, 1),
    schedule="15 4 * * *",
    catchup=False,
    default_args=default_args,
    tags=["dbt", "processing", "business_logic"],
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
            requests={"cpu": "100m", "memory": "256Mi"},
            limits={"cpu": "1", "memory": "1Gi"},
        ),

        # Other defaults
        get_logs=True,
        is_delete_operator_pod=True,

        node_selector={"kubernetes.io/hostname": "ampere-k8s-node2"},
    )

    dbt_business_logic_stage = KubernetesPodOperator(
        task_id="dbt_build_business_logic_stage",
        name="dbt-build-business-logic-stage",
        namespace=NAMESPACE,
        image=IMAGE,
        image_pull_secrets=[V1LocalObjectReference(name="ghcr-pull")],
        startup_timeout_seconds=240,
        secrets=[
            minio_access_key, minio_secret_key, mssql_pass
        ],
        env_vars={
            "MINIO_S3_ENDPOINT": "minio.ampere.svc.cluster.local:9000",
            "DBT_CMD": "dbt run --selector bl_export --vars '{\"enable_mssql_export\": true}'",
            "DBT_TARGET": "prod",
            "THREADS": "4",
            "DUCKDB_PATH": "/app/artifacts/ampere.duckdb",
            "EXPORT_CONFIG_PATH": "/app/project/models/business_logic/config/export_map.yaml",
            "MSSQL_HOST": "mssql-service.ampere.svc.cluster.local",
            "MSSQL_PORT": "1433",
            "MSSQL_DB":   "Ampere",            
        },
        container_resources=V1ResourceRequirements(
            requests={"cpu": "100m", "memory": "256Mi"},
            limits={"cpu": "1", "memory": "1Gi"},
        ),
        get_logs=True,
        is_delete_operator_pod=True,
        node_selector={"kubernetes.io/hostname": "ampere-k8s-node2"},
    )  

    dbt_processing >> dbt_business_logic_stage
