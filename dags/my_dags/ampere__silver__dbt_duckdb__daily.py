from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret
from airflow.providers.standard.operators.python import PythonOperator
from kubernetes.client import V1LocalObjectReference, V1ResourceRequirements

from utils.ampere_dag_config import load_silver_dag_config, standard_default_args

DAG_ID = "ampere__silver__dbt_duckdb__daily"
DAG_CONFIG = load_silver_dag_config()


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


with DAG(
    dag_id=DAG_ID,
    default_args=standard_default_args(),
    schedule=None,
    start_date=datetime(2025, 8, 24),
    tags=[
        "layer:silver",
        "system:dbt",
        "system:duckdb",
        "system:minio",
        "mode:daily",
    ],
    catchup=False,
    max_active_runs=DAG_CONFIG.max_active_runs,
) as dag:
    # Boundary marker before silver runtime container starts.
    start_task = PythonOperator(
        task_id="run__silver__start",
        python_callable=print,
        op_args=["##### startSilver #####"],
    )

    # Execute silver dbt runtime in one container so staging + marts share one
    # DuckDB workspace file for the full invocation.
    run_silver_dbt = KubernetesPodOperator(
        task_id="run__silver__dbt_build",
        name="ampere-silver-dbt",
        namespace=DAG_CONFIG.namespace,
        image=DAG_CONFIG.image,
        image_pull_policy=DAG_CONFIG.image_pull_policy,
        image_pull_secrets=[V1LocalObjectReference(name="ghcr-pull")],
        service_account_name=DAG_CONFIG.service_account,
        node_selector=DAG_CONFIG.node_selector,
        secrets=[minio_access_key, minio_secret_key],
        env_vars={
            "MINIO_S3_ENDPOINT": DAG_CONFIG.minio_endpoint,
            "MINIO_S3_USE_SSL": DAG_CONFIG.minio_use_ssl,
            "UC_API_URI": DAG_CONFIG.uc_api_uri,
            "UC_TOKEN": DAG_CONFIG.uc_token,
            "BRONZE_UC_CATALOG": DAG_CONFIG.bronze_uc_catalog,
            "BRONZE_UC_SCHEMA": DAG_CONFIG.bronze_uc_schema,
            "BRONZE_SOURCE_NAME": DAG_CONFIG.bronze_source_name,
            "BRONZE_SOURCE_SCHEMA": DAG_CONFIG.bronze_source_schema,
            "BRONZE_SOURCE_MAPPING_PATH": DAG_CONFIG.bronze_source_mapping_path,
            "BRONZE_SOURCE_MAPPING_MAX_AGE_HOURS": DAG_CONFIG.bronze_source_mapping_max_age_hours,
            "RUN_UC_MAPPING_GENERATION": DAG_CONFIG.run_uc_mapping_generation,
            "RUN_BRONZE_PREFLIGHT": DAG_CONFIG.run_bronze_preflight,
            "RUN_BRONZE_PREFLIGHT_DELTA_SCAN": DAG_CONFIG.run_bronze_preflight_delta_scan,
            "DBT_TARGET": DAG_CONFIG.dbt_target,
            "THREADS": DAG_CONFIG.dbt_threads,
        },
        arguments=[DAG_CONFIG.dbt_command],
        container_resources=V1ResourceRequirements(
            requests={"cpu": "500m", "memory": "2Gi"},
            limits={"cpu": "2", "memory": "6Gi"},
        ),
        get_logs=True,
        is_delete_operator_pod=True,
    )

    # Boundary marker after silver build and tests finish.
    done_task = PythonOperator(
        task_id="run__silver__done",
        python_callable=print,
        op_args=["##### doneSilver #####"],
    )

    start_task >> run_silver_dbt >> done_task
