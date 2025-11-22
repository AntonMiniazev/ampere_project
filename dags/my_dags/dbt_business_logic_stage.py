from datetime import datetime
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret
from kubernetes.client import V1LocalObjectReference, V1ResourceRequirements
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

DAG_ID = "dbt_business_logic_stage"
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

mssql_pass = Secret(
    deploy_type="env",
    deploy_target="MSSQL_PASSWORD",
    secret="mssql-sa-secret",
    key="SA_PASSWORD",
)

default_args = {"owner": "ampere"}

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["dbt", "processing", "business_logic_stage"],
) as dag:

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
            "MINIO_S3_ENDPOINT": "http://minio.ampere.svc.cluster.local:9000",
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
            requests={"cpu": "250m", "memory": "1Gi"},
            limits={"cpu": "2", "memory": "3Gi"},
        ),
        get_logs=True,
        is_delete_operator_pod=True,
        node_selector={"kubernetes.io/hostname": "ampere-k8s-node2"},
    )  

    trigger_mssql_reporting_layer = TriggerDagRunOperator(
        task_id="trigger_mssql_reporting_layer",
        trigger_dag_id="mssql_reporting_layer",
        logical_date="{{ logical_date }}",
    )

    dbt_business_logic_stage >> trigger_mssql_reporting_layer