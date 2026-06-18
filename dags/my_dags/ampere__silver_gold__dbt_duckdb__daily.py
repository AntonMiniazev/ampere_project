from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import Variable
from kubernetes.client import V1LocalObjectReference, V1ResourceRequirements

from utils.ampere_dag_config import load_silver_dag_config, standard_default_args

DAG_ID = "ampere__silver_gold__dbt_duckdb__daily"
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
        "layer:silver_gold",
        "layer:silver",
        "layer:gold",
        "system:dbt",
        "system:duckdb",
        "system:minio",
        "mode:daily",
    ],
    catchup=False,
    max_active_runs=DAG_CONFIG.max_active_runs,
) as dag:
    # Boundary marker before the combined silver and gold runtime starts.
    start_task = PythonOperator(
        task_id="run__silver_gold__start",
        python_callable=print,
        op_args=["##### startSilverGold #####"],
    )

    # Execute silver and gold in one container so gold can reuse freshly built
    # silver relations from the same DuckDB workspace.
    run_silver_dbt = KubernetesPodOperator(
        task_id="run__silver_gold__dbt_build",
        name="ampere-dbt-silver-gold",
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
            "DBT_TARGET": DAG_CONFIG.dbt_target,
            "THREADS": DAG_CONFIG.dbt_threads,
            "DUCKDB_MEMORY_LIMIT": DAG_CONFIG.duckdb_memory_limit,
            "DUCKDB_TEMP_DIRECTORY": DAG_CONFIG.duckdb_temp_directory,
            "SILVER_EXTERNAL_ROOT": DAG_CONFIG.silver_external_root,
            "SILVER_DBT_ARTIFACT_ROOT": DAG_CONFIG.silver_artifact_root,
            "SILVER_RUN_MODE": DAG_CONFIG.run_mode,
            "SILVER_LOOKBACK_DAYS": DAG_CONFIG.lookback_days,
            "RUN_SILVER_PUBLISH": DAG_CONFIG.run_silver_publish,
            "RUN_SILVER_UC_REGISTRATION": DAG_CONFIG.run_silver_uc_registration,

            "GOLD_EXTERNAL_ROOT": Variable.get(
                "gold_external_root", default="s3://ampere-gold/gold"
            ),
            "GOLD_DBT_ARTIFACT_ROOT": Variable.get(
                "gold_dbt_artifact_root", default="s3://ampere-gold-ops/dbt"
            ),
            "GOLD_RUN_MODE": DAG_CONFIG.run_mode,
            "GOLD_LOOKBACK_DAYS": DAG_CONFIG.lookback_days,
            "GOLD_UC_CATALOG": DAG_CONFIG.bronze_uc_catalog,
            "GOLD_UC_SCHEMA": Variable.get("spark_uc_gold_schema", default="gold"),
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
                "silver_daily_with_gold_dbt_command",
                default=DAG_CONFIG.dbt_command,
            )
        ],
        container_resources=V1ResourceRequirements(
            requests={
                "cpu": DAG_CONFIG.cpu_request,
                "memory": DAG_CONFIG.memory_request,
            },
            limits={
                "cpu": DAG_CONFIG.cpu_limit,
                "memory": DAG_CONFIG.memory_limit,
            },
        ),
        get_logs=True,
        is_delete_operator_pod=True,
    )

    # Boundary marker after silver and gold build/publish steps finish.
    done_task = PythonOperator(
        task_id="run__silver_gold__done",
        python_callable=print,
        op_args=["##### doneSilverGold #####"],
    )

    # Start Curie's dashboard cache refresh after Gold is published.
    trigger_curie_cache_refresh = TriggerDagRunOperator(
        task_id="trigger__curie__cache_refresh__post_gold",
        trigger_dag_id="ampere__curie__cache_refresh__post_gold",
        logical_date="{{ (dag_run.logical_date or dag_run.run_after).isoformat() }}",
        reset_dag_run=True,
        wait_for_completion=True,
    )

    start_task >> run_silver_dbt >> done_task >> trigger_curie_cache_refresh
