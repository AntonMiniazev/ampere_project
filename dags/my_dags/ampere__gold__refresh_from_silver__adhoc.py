from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Variable
from kubernetes.client import V1LocalObjectReference, V1ResourceRequirements

from utils.ampere_dag_config import load_silver_dag_config, standard_default_args

DAG_ID = "ampere__gold__refresh_from_silver__adhoc"
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
        "layer:gold",
        "system:dbt",
        "system:duckdb",
        "system:minio",
        "mode:adhoc",
    ],
    catchup=False,
    max_active_runs=1,
) as dag:
    start_task = PythonOperator(
        task_id="run__gold__refresh_from_silver__start",
        python_callable=print,
        op_args=["##### startGoldRefreshFromSilver #####"],
    )

    run_gold_dbt = KubernetesPodOperator(
        task_id="run__gold__dbt_refresh_from_silver",
        name="ampere-dbt-gold-refresh-from-silver",
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
            "DBT_TARGET": Variable.get("gold_dbt_target", default=DAG_CONFIG.dbt_target),
            "THREADS": Variable.get("gold_dbt_threads", default="2"),
            "DUCKDB_MEMORY_LIMIT": Variable.get(
                "gold_duckdb_memory_limit", default=DAG_CONFIG.duckdb_memory_limit
            ),
            "DUCKDB_TEMP_DIRECTORY": Variable.get(
                "gold_duckdb_temp_directory", default=DAG_CONFIG.duckdb_temp_directory
            ),
            "RUN_BRONZE_SOURCE_PREPARE": "false",
            "SILVER_UC_CATALOG": DAG_CONFIG.bronze_uc_catalog,
            "SILVER_UC_SCHEMA": Variable.get("spark_uc_silver_schema", default="silver"),
            "SILVER_SOURCE_NAME": "silver",
            "SILVER_SOURCE_SCHEMA": "silver",

            "GOLD_EXTERNAL_ROOT": Variable.get(
                "gold_external_root", default="s3://ampere-gold/gold"
            ),
            "GOLD_DBT_ARTIFACT_ROOT": Variable.get(
                "gold_dbt_artifact_root", default="s3://ampere-gold-ops/dbt"
            ),
            "GOLD_UC_CATALOG": DAG_CONFIG.bronze_uc_catalog,
            "GOLD_UC_SCHEMA": Variable.get("spark_uc_gold_schema", default="gold"),
            "GOLD_RUN_MODE": "daily_refresh",
            "GOLD_LOOKBACK_DAYS": Variable.get(
                "gold_lookback_days", default=DAG_CONFIG.lookback_days
            ),
            "RUN_SILVER_PUBLISH": "false",
            "RUN_SILVER_UC_REGISTRATION": "false",
            "RUN_GOLD_PUBLISH": Variable.get("run_gold_publish", default="true")
            .strip()
            .lower(),
            "RUN_GOLD_UC_REGISTRATION": Variable.get(
                "run_gold_uc_registration", default="true"
            )
            .strip()
            .lower(),
            "RUN_DBT_ARTIFACT_UPLOAD": DAG_CONFIG.run_dbt_artifact_upload,
            "LOGICAL_DATE": "{{ ds }}",
        },
        arguments=[
            Variable.get(
                "gold_dbt_command",
                default="dbt build --select tag:gold",
            )
        ],
        container_resources=V1ResourceRequirements(
            requests={
                "cpu": Variable.get("gold_dbt_cpu_request", default=DAG_CONFIG.cpu_request),
                "memory": Variable.get(
                    "gold_dbt_memory_request", default=DAG_CONFIG.memory_request
                ),
            },
            limits={
                "cpu": Variable.get("gold_dbt_cpu_limit", default=DAG_CONFIG.cpu_limit),
                "memory": Variable.get("gold_dbt_memory_limit", default=DAG_CONFIG.memory_limit),
            },
        ),
        get_logs=True,
        is_delete_operator_pod=True,
    )

    done_task = PythonOperator(
        task_id="run__gold__refresh_from_silver__done",
        python_callable=print,
        op_args=["##### doneGoldRefreshFromSilver #####"],
    )

    start_task >> run_gold_dbt >> done_task
