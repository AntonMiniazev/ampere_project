from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

dbt_processing = KubernetesPodOperator(
    task_id="dbt_processing",
    name="dbt-processing",
    namespace="ampere",
    image="an7on/ampere-dbt-runner:{{ var.value.git_sha }}",
    env_vars={
        "DBT_CMD": "dbt build --selector processing --fail-fast --project-dir /app/project --profiles-dir /app/profiles",
        "DBT_TARGET": "prod",
        "THREADS": "4",
    },
    get_logs=True,
    is_delete_operator_pod=True,
)
