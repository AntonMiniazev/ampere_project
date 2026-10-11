from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret
from kubernetes.client import V1LocalObjectReference, V1ResourceRequirements

from utils.ampere_dag_config import load_pre_raw_dag_config

DAG_ID = "ampere__pre_raw__generators__backfill"
DAG_CONFIG = load_pre_raw_dag_config(
    repository="ghcr.io/antonminiazev/order-data-generator",
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
    dag_id=DAG_ID,
    schedule=None,
    start_date=datetime(2025, 8, 24),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=8),
    description=(
        "Generate up to 31 consecutive source-data days in one PostgreSQL-bound "
        "pod. Completed days are skipped on retry; this DAG does not start Raw or Bronze."
    ),
    tags=["layer:pre_raw", "system:postgres", "mode:backfill"],
) as dag:
    generate_range = KubernetesPodOperator(
        task_id="run__pre_raw__backfill",
        name="order-data-generator-backfill",
        namespace=DAG_CONFIG.namespace,
        node_selector=DAG_CONFIG.node_selector,
        image=DAG_CONFIG.image,
        image_pull_policy="IfNotPresent",
        image_pull_secrets=[V1LocalObjectReference(name="ghcr-pull")],
        secrets=[pg_user, pg_pass],
        env_vars={
            "PGOPTIONS": f"-c work_mem={DAG_CONFIG.pg_work_mem}",
            "AMPERE_SOURCE_CODE_VERSION": DAG_CONFIG.image,
            "POLARS_MAX_THREADS": "3",
            "OPENBLAS_NUM_THREADS": "1",
            "OMP_NUM_THREADS": "1",
            "MKL_NUM_THREADS": "1",
        },
        cmds=["python", "-m", "order_data_generator"],
        arguments=[
            "--start-date",
            "{{ dag_run.conf['start_date'] }}",
            "--end-date",
            "{{ dag_run.conf['end_date'] }}",
            "--seed",
            "{{ dag_run.conf.get('seed', 42) }}",
        ],
        container_resources=V1ResourceRequirements(
            requests={"cpu": "3", "memory": "4Gi"},
            limits={"cpu": "4", "memory": "6Gi"},
        ),
        execution_timeout=timedelta(hours=8),
        retries=1,
        retry_delay=timedelta(minutes=2),
        startup_timeout_seconds=240,
        get_logs=True,
        do_xcom_push=False,
        is_delete_operator_pod=True,
    )
