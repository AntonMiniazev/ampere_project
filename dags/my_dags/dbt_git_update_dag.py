# This DAG updates the dbt project working tree inside the PVC by running .ops/git_update.sh
from datetime import datetime
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
from kubernetes.client import models as k8s

NS = "ampere"
PVC_NAME = "dbt-code-pvc"
SSH_SECRET = "dbt-git-ssh"
NODE_NAME = "ampere-k8s-node2"

# K8s objects for mounts
vol_dbt_code = k8s.V1Volume(
    name="dbt-code",
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
        claim_name=PVC_NAME
    ),
)
vol_ssh = k8s.V1Volume(
    name="ssh", secret=k8s.V1SecretVolumeSource(secret_name=SSH_SECRET)
)

mount_dbt_code = k8s.V1VolumeMount(name="dbt-code", mount_path="/workspace/dbt_project")
mount_ssh_key = k8s.V1VolumeMount(
    name="ssh",
    mount_path="/root/.ssh/id_ed25519",
    sub_path="ssh-privatekey",
    read_only=True,
)
mount_ssh_known = k8s.V1VolumeMount(
    name="ssh",
    mount_path="/root/.ssh/known_hosts",
    sub_path="known_hosts",
    read_only=True,
)

with (
    DAG(
        dag_id="dbt_git_update",
        start_date=datetime(2025, 8, 1),
        schedule=None,  # manual by default; trigger with conf {"ref":"master"} or a commit SHA
        catchup=False,
        default_args={"retries": 0},
        max_active_runs=1,
        tags=["dbt", "git", "maintenance"],
    ) as dag
):
    git_update = KubernetesPodOperator(
        task_id="git_update",
        name="dbt-git-update",
        namespace=NS,
        image="alpine/git:latest",
        cmds=["sh", "-lc"],
        # Use dag_run.conf.ref if present, default to "master"
        arguments=[
            "GIT_SSH_COMMAND='ssh -i /root/.ssh/id_ed25519 -o IdentitiesOnly=yes' "
            "/workspace/dbt_project/.ops/git_update.sh {{ dag_run.conf.get('ref', 'master') }}"
        ],
        volumes=[vol_dbt_code, vol_ssh],
        volume_mounts=[mount_dbt_code, mount_ssh_key, mount_ssh_known],
        node_selector={"kubernetes.io/hostname": NODE_NAME},
        is_delete_operator_pod=True,
        get_logs=True,
        in_cluster=True,
        config_file=None,
    )

    git_update
