from datetime import datetime

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from kubernetes.client import V1LocalObjectReference, V1ResourceRequirements

from utils.ampere_dag_config import load_pre_raw_dag_config

DAG_ID = "ampere__pre_raw__generators__daily"
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
    schedule="15 4 * * *",
    start_date=datetime(2025, 8, 24),
    tags=["layer:pre_raw", "system:postgres", "mode:daily"],
    catchup=False,
    max_active_runs=1,
) as dag:
    # Step 1: run the daily Python generator in Kubernetes. This mutates the
    # PostgreSQL source schema by applying churn, adding new clients, and
    # creating fresh operational rows for the current logical date.
    generate_data = KubernetesPodOperator(
        task_id="run__pre_raw__daily",
        name="order-data-generator",
        depends_on_past=True,
        namespace=DAG_CONFIG.namespace,
        node_selector=DAG_CONFIG.node_selector,
        image=DAG_CONFIG.image,
        image_pull_policy="IfNotPresent",
        image_pull_secrets=[V1LocalObjectReference(name="ghcr-pull")],
        secrets=[pg_user, pg_pass],
        env_vars={"PGOPTIONS": f"-c work_mem={DAG_CONFIG.pg_work_mem}"},
        cmds=["python", "-m", "order_data_generator"],
        # Use Airflow's logical date as the business date for synthetic data.
        # Manual runs still work because `run_after` is used as a fallback.
        arguments=[
            "--run-date",
            "{{ (dag_run.logical_date or dag_run.run_after).strftime('%Y-%m-%d') }}",
        ],
        container_resources=V1ResourceRequirements(
            requests={"cpu": "500m", "memory": "1536Mi"},
            limits={"cpu": "4", "memory": "6Gi"},
        ),
        get_logs=True,
        is_delete_operator_pod=True,
    )

    # Step 2: once PostgreSQL has been updated, trigger the Spark extraction DAG
    # that reads the source schema and writes raw landing batches to object storage.
    trigger_raw_landing = TriggerDagRunOperator(
        task_id="trigger__raw_landing__postgres_to_landing__daily",
        trigger_dag_id="ampere__raw_landing__postgres_to_landing__daily",
        logical_date="{{ (dag_run.logical_date or dag_run.run_after).isoformat() }}",
        reset_dag_run=True,
        wait_for_completion=False,
    )

    generate_data >> trigger_raw_landing
