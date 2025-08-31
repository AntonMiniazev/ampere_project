# -*- coding: utf-8 -*-
# Smoke-run to verify mounts and env vars before real dbt runs
from datetime import datetime
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

NS = "ampere"
PVC_CODE = "dbt-code-pvc"
PVC_DATA = "dbt-data-pvc"
NODE = "ampere-k8s-node2"

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
    dag_id="dbt_paths_smoke",
    start_date=datetime(2025, 8, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 0},
    tags=["dbt", "smoke"],
) as dag:
    smoke = KubernetesPodOperator(
        task_id="check_paths",
        name="dbt-paths-smoke",
        namespace=NS,
        image="alpine:3.19",
        cmds=["sh", "-lc"],
        arguments=[
            # Verify that code mount is RO and work/data mounts are RW
            "set -eux; "
            "mkdir -p /workspace/dbt_work/target /workspace/dbt_work/logs /workspace/dbt_work/dbt_packages; "
            "echo OK >/workspace/dbt_work/target/_write_test; "
            "echo 'DBT_TARGET_PATH='${DBT_TARGET_PATH}; "
            "echo 'DBT_LOG_PATH='${DBT_LOG_PATH}; "
            "echo 'DBT_PACKAGES_INSTALL_PATH='${DBT_PACKAGES_INSTALL_PATH}; "
            "ls -la /workspace/dbt_work /workspace/dbt_work/target /workspace/dbt_data; "
            "sh -lc 'echo SHOULD_FAIL >/workspace/dbt_project/.write_test' || echo 'RO OK: /workspace/dbt_project is read-only'; "
            "exit 0"
        ],
        env_vars={
            # dbt runtime paths
            "DBT_TARGET_PATH": "/workspace/dbt_work/target",
            "DBT_LOG_PATH": "/workspace/dbt_work/logs",
            "DBT_PACKAGES_INSTALL_PATH": "/workspace/dbt_work/dbt_packages",
        },
        volumes=[vol_code, vol_data],
        volume_mounts=[mount_code_ro, mount_work, mount_data],
        node_selector={"kubernetes.io/hostname": NODE},
        is_delete_operator_pod=True,
        get_logs=True,
        in_cluster=True,
        config_file=None,
    )

    smoke
