from datetime import datetime
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret
from kubernetes.client import V1LocalObjectReference, V1ResourceRequirements

DAG_ID = "dbt_processing_dbg"
NAMESPACE = "ampere"
IMAGE = "ghcr.io/antonminiazev/ampere_project:latest"

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["dbt","debug"],
):
    dbt_processing_dbg = KubernetesPodOperator(
        task_id="dbg",
        name="dbg",
        namespace=NAMESPACE,
        image=IMAGE,
        image_pull_secrets=[V1LocalObjectReference(name="ghcr-pull")],
        is_delete_operator_pod=False,
        get_logs=True,
        startup_timeout_seconds=900,
        container_resources=V1ResourceRequirements(
            requests={"cpu": "50m","memory":"128Mi"},
            limits={"cpu":"1","memory":"1Gi"},
        ),
        cmds=["/bin/bash","-lc"],
        arguments=[
            # print debug info, then try to run the entrypoint manually
            "set -x; "
            "env | sort; "
            "echo '--- BINARIES ---'; "
            "which dbt || true; "
            "ls -l /usr/local/bin || true; "
            "echo '--- ENTRYPOINT SCRIPT ---'; "
            "test -f /usr/local/bin/run_dbt.sh && head -n 40 /usr/local/bin/run_dbt.sh || true; "
            "echo '--- EXEC ---'; "
            "/usr/local/bin/run_dbt.sh"
        ],
        env_vars={
            "MINIO_S3_ENDPOINT": "minio.ampere.svc.cluster.local:9000",
            "DBT_CMD": "dbt build --project-dir /app/project --profiles-dir /app/profiles --selector orders_flow --fail-fast",
            "DBT_TARGET": "prod",
            "THREADS": "4",
            "DUCKDB_PATH": "/app/artifacts/ampere.duckdb",
        },
        secrets=[
            Secret(deploy_type="env", deploy_target="MINIO_ACCESS_KEY", secret="minio-creds", key="MINIO_ACCESS_KEY"),
            Secret(deploy_type="env", deploy_target="MINIO_SECRET_KEY", secret="minio-creds", key="MINIO_SECRET_KEY"),
        ],
    )
