# comments in English only
from datetime import datetime
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret

DAG_ID = "dbt_processing"
NAMESPACE = "ampere"

# Image tag is templated from Airflow Variable 'git_sha'
IMAGE = "ghcr.io/antonminiazev/ampere_project:297d427"


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

default_args = {
    "owner": "ampere",
    "depends_on_past": False,
}

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 1, 1),
    schedule=None,  # run manually for now
    catchup=False,
    default_args=default_args,
    tags=["dbt", "processing"],
) as dag:

    dbt_processing = KubernetesPodOperator(
        task_id="dbt_build_processing",
        name="dbt-build-processing",
        namespace=NAMESPACE,
        image=IMAGE,
        # If your GHCR package is private, uncomment and ensure the pull secret exists:
        image_pull_secrets=["ghcr-pull"],

        # Inject MINIO_* from 'minio-creds' and runtime env for the runner
        secrets=[minio_access_key, minio_secret_key],
        env_vars={
            # in-cluster MinIO Service DNS (host:port, no scheme)
            "MINIO_S3_ENDPOINT": "minio.ampere.svc.cluster.local:9000",
            # dbt command (select only processing layer)
            "DBT_CMD": "dbt build -s +tag:layer:processing --fail-fast",
            "DBT_TARGET": "prod",
            "THREADS": "4",
            # optional local path for DuckDB file inside the container
            "DUCKDB_PATH": "/app/artifacts/ampere.duckdb",
        },

        # Other defaults
        get_logs=True,
        is_delete_operator_pod=True,
        # resources={"request_cpu": "500m", "request_memory": "512Mi",
        #            "limit_cpu": "2", "limit_memory": "2Gi"},

        node_selector={"kubernetes.io/hostname": "ampere-k8s-node2"},
    )

    dbt_processing
