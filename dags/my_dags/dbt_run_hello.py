# Runs dbt model hello.sql from PVC checkout and validates the result in DuckDB

from datetime import datetime
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

# --- static cluster settings ---
NS = "ampere"
NODE_NAME = "ampere-k8s-node2"
PVC_CODE = "dbt-code-pvc"  # repo checkout (RO for run)
PVC_DATA = "dbt-data-pvc"  # target/logs/dbt_packages + /workspace/dbt_data/*.duckdb

# --- dbt paths ---
PROJECT_DIR = "/workspace/dbt_project/dbt"
PROFILES_DIR = "/workspace/dbt_work/profiles"

# --- volumes/mounts ---
vol_code = k8s.V1Volume(
    name="dbt-code",
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
        claim_name=PVC_CODE
    ),
)
vol_data = k8s.V1Volume(
    name="dbt-data",
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
        claim_name=PVC_DATA
    ),
)

mount_code_ro = k8s.V1VolumeMount(
    name="dbt-code", mount_path="/workspace/dbt_project", read_only=True
)
mount_work = k8s.V1VolumeMount(
    name="dbt-data", mount_path="/workspace/dbt_work", sub_path="work"
)
mount_data = k8s.V1VolumeMount(
    name="dbt-data", mount_path="/workspace/dbt_data", sub_path="data"
)

with DAG(
    dag_id="dbt_hello_run",
    start_date=datetime(2025, 8, 1),
    schedule=None,  # trigger manually after dbt_git_update has run
    catchup=False,
    default_args={"retries": 0},
    max_active_runs=1,
    tags=["dbt", "duckdb", "hello"],
) as dag:
    dbt_run = KubernetesPodOperator(
        task_id="dbt_run_hello",
        name="dbt-run-hello",
        namespace=NS,
        image="python:3.11-slim",
        cmds=["bash", "-lc"],
        arguments=[
            # Install runtime, run dbt, then validate result via DuckDB
            "set -eux; "
            "apt-get update && apt-get install -y --no-install-recommends git ca-certificates && rm -rf /var/lib/apt/lists/*; "
            "pip install --no-cache-dir 'dbt-duckdb==1.9.4' 'duckdb==1.3.2'; "
            f"echo '[GIT HEAD]' $(git -C /workspace/dbt_project rev-parse --short HEAD); "
            f"dbt run --project-dir {PROJECT_DIR} --profiles-dir {PROFILES_DIR} --select hello; "
            "python -c \"import duckdb; con=duckdb.connect('/workspace/dbt_data/warehouse.duckdb'); "
            "print('[TABLES]', con.sql('show tables').fetchall()); "
            "print('[SELECT hello]', con.sql('select * from main.hello').fetchall())\""
        ],
        env_vars={
            "DBT_TARGET_PATH": "/workspace/dbt_work/target",
            "DBT_LOG_PATH": "/workspace/dbt_work/logs",
            "DBT_PACKAGES_INSTALL_PATH": "/workspace/dbt_work/dbt_packages",
        },
        volumes=[vol_code, vol_data],
        volume_mounts=[mount_code_ro, mount_work, mount_data],
        node_selector={"kubernetes.io/hostname": NODE_NAME},
        is_delete_operator_pod=True,  # auto-clean pod after finish
        get_logs=True,
        in_cluster=True,
        config_file=None,
    )

    dbt_run
