from __future__ import annotations

from datetime import datetime
import logging
from urllib import error as urlerror
from urllib import request as urlrequest

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import BranchPythonOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.task.trigger_rule import TriggerRule

from utils.ampere_dag_config import (
    get_optional_nonnegative_int_variable,
    load_bronze_dag_config,
    minio_ssl_enabled,
    standard_default_args,
)
from utils.stream_group_config import build_bronze_stream_groups

DAG_ID = "ampere__bronze__landing_to_delta__daily"
DAG_CONFIG = load_bronze_dag_config(__file__)
SPARK_APP_TEMPLATE = "raw_to_bronze_template_uc.yaml"
REGISTRY_INIT_TEMPLATE = "bronze_registry_init_uc.yaml"


def _uc_request_headers() -> dict[str, str]:
    """Build HTTP headers for Unity Catalog metadata calls."""
    headers = {"Content-Type": "application/json"}
    auth_type = (DAG_CONFIG.uc_auth_type or "").strip().lower()
    token = (DAG_CONFIG.uc_token or "").strip()
    if token and auth_type in {"", "static"}:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def _registry_exists() -> bool:
    """Check whether the bronze apply registry table exists in Unity Catalog."""
    logger = logging.getLogger(DAG_ID)
    uc_api_uri = (DAG_CONFIG.uc_api_uri or "").strip().rstrip("/")
    if not uc_api_uri:
        logger.warning("UC API URI is missing; running registry validation task.")
        return False
    fq_table = (
        f"{DAG_CONFIG.uc_catalog}."
        f"{DAG_CONFIG.uc_ops_schema}."
        "bronze_apply_registry"
    )
    request = urlrequest.Request(
        f"{uc_api_uri}/api/2.1/unity-catalog/tables/{fq_table}",
        headers=_uc_request_headers(),
        method="GET",
    )
    try:
        with urlrequest.urlopen(request, timeout=20) as response:  # noqa: S310
            return response.status == 200
    except urlerror.HTTPError as exc:
        if exc.code == 404:
            return False
        logger.warning("UC registry check failed for %s: HTTP %s", fq_table, exc.code)
        return False
    except Exception as exc:  # noqa: BLE001
        logger.warning("UC registry check failed for %s: %s", fq_table, exc)
        return False


def _select_registry_task() -> str:
    """Choose the Airflow branch target for registry initialization."""
    return (
        "skip__sparkapp__registry_init"
        if _registry_exists()
        else "run__sparkapp__registry_init"
    )


def _base_params() -> dict:
    """Assemble the parameter block shared by all bronze Spark applications."""
    return {
        "namespace": DAG_CONFIG.spark_namespace,
        "image": DAG_CONFIG.image,
        "image_pull_policy": DAG_CONFIG.image_pull_policy,
        "service_account": DAG_CONFIG.service_account,
        "schema": DAG_CONFIG.schema,
        "raw_bucket": DAG_CONFIG.raw_bucket,
        "raw_prefix": DAG_CONFIG.raw_prefix,
        "source_system": DAG_CONFIG.source_system,
        "minio_endpoint": DAG_CONFIG.minio_endpoint,
        "minio_ssl_enabled": minio_ssl_enabled(DAG_CONFIG.minio_endpoint),
        "driver_cores": DAG_CONFIG.driver_cores,
        "driver_core_request": DAG_CONFIG.driver_core_request,
        "driver_memory": DAG_CONFIG.driver_memory,
        "driver_memory_overhead": DAG_CONFIG.driver_memory_overhead,
        "executor_cores": DAG_CONFIG.executor_cores,
        "executor_core_request": DAG_CONFIG.executor_core_request,
        "executor_memory": DAG_CONFIG.executor_memory,
        "executor_memory_overhead": DAG_CONFIG.executor_memory_overhead,
        "executor_instances": DAG_CONFIG.executor_instances,
        "executor_node_selector": DAG_CONFIG.executor_node_selector,
        "shuffle_partitions": DAG_CONFIG.shuffle_partitions,
        "uc_enabled": DAG_CONFIG.uc_enabled,
        "uc_catalog": DAG_CONFIG.uc_catalog,
        "uc_bronze_schema": DAG_CONFIG.uc_bronze_schema,
        "uc_ops_schema": DAG_CONFIG.uc_ops_schema,
        "uc_api_uri": DAG_CONFIG.uc_api_uri,
        "uc_token": DAG_CONFIG.uc_token,
        "uc_auth_type": DAG_CONFIG.uc_auth_type,
        "uc_catalog_impl": DAG_CONFIG.uc_catalog_impl,
    }


def _resolve_lookback_overrides() -> dict[str, int | None]:
    """Collect optional per-group lookback overrides for bronze processing."""
    events_override = get_optional_nonnegative_int_variable("spark_events_lookback_days")
    if events_override is None:
        events_override = get_optional_nonnegative_int_variable("spark_event_lookback_days")
    return {
        "facts": get_optional_nonnegative_int_variable("spark_facts_lookback_days"),
        "events": events_override,
        "mutable_dims": get_optional_nonnegative_int_variable(
            "spark_mutable_dims_lookback_days"
        ),
    }


def _group_pair_lookback_days(groups_config: list[dict]) -> int:
    """Return the widest lookback across the logical groups in one Spark job."""
    return max(
        (int(group.get("lookback_days", 0) or 0) for group in groups_config),
        default=0,
    )


with DAG(
    dag_id=DAG_ID,
    default_args=standard_default_args(),
    schedule=None,
    start_date=datetime(2025, 8, 24),
    tags=[
        "layer:bronze",
        "system:spark",
        "system:minio",
        "catalog:unity",
        "mode:daily",
    ],
    catchup=False,
    max_active_tasks=DAG_CONFIG.max_active_tasks,
    template_searchpath=DAG_CONFIG.template_paths,
) as dag:
    # Boundary marker before any bronze Spark job is submitted.
    start_batch_task = PythonOperator(
        task_id="run__sparkapp__start",
        python_callable=print,
        op_args=["##### startBatch #####"],
    )

    # Boundary marker after bronze processing has finished.
    done_task = PythonOperator(
        task_id="run__sparkapp__done",
        python_callable=print,
        op_args=["##### done #####"],
    )

    base_params = _base_params()

    # Expand the shared JSON config into the logical groups processed by bronze.
    stream_groups = build_bronze_stream_groups(_resolve_lookback_overrides())
    group_map = {group["group"]: group for group in stream_groups}
    for name, group in group_map.items():
        if name == "snapshots":
            group["shuffle_partitions"] = 1
        elif name in {"facts", "events"}:
            group["shuffle_partitions"] = DAG_CONFIG.shuffle_partitions_facts_events
            group["files_max_partition_bytes"] = (
                DAG_CONFIG.files_max_partition_bytes_facts_events
            )
            group["files_open_cost_bytes"] = (
                DAG_CONFIG.files_open_cost_bytes_facts_events
            )
            group["adaptive_coalesce"] = DAG_CONFIG.adaptive_coalesce_facts_events
        elif name == "mutable_dims":
            group["shuffle_partitions"] = DAG_CONFIG.shuffle_partitions_mutable_dims
        else:
            group["shuffle_partitions"] = DAG_CONFIG.shuffle_partitions

    registry_check = BranchPythonOperator(
        task_id="run__sparkapp__registry_check",
        python_callable=_select_registry_task,
    )
    skip_registry_task = EmptyOperator(
        task_id="skip__sparkapp__registry_init",
    )

    registry_params = {
        **base_params,
        "app_name": "bronze-registry-init",
        "executor_instances": 1,
    }

    init_registry_task = SparkKubernetesOperator(
        task_id="run__sparkapp__registry_init",
        namespace=DAG_CONFIG.spark_namespace,
        application_file=REGISTRY_INIT_TEMPLATE,
        params=registry_params,
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=False,
    )

    registry_ready = EmptyOperator(
        task_id="run__sparkapp__registry_ready",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    group_pairs = [
        ("snapshots-mutable-dims", ["snapshots", "mutable_dims"]),
        ("facts-events", ["facts", "events"]),
    ]
    snapshots_task = None
    facts_events_task = None
    for group_name, group_keys in group_pairs:
        groups_config = [group_map[key] for key in group_keys if key in group_map]
        if not groups_config:
            continue
        executor_instances = (
            DAG_CONFIG.executor_instances_snapshots
            if group_name == "snapshots-mutable-dims"
            else DAG_CONFIG.executor_instances_facts_events
        )
        params = {
            **base_params,
            "group": group_name,
            "tables": "",
            "table_config": {},
            "groups_config": groups_config,
            "stream": group_name,
            "mode": "snapshot",
            "partition_key": "snapshot_date",
            "event_date_column": "",
            "lookback_days": _group_pair_lookback_days(groups_config),
            "app_name": f"raw-to-bronze-{group_name}",
            "executor_instances": executor_instances,
        }
        task = SparkKubernetesOperator(
            task_id=f"run__sparkapp__group_{group_name}",
            namespace=DAG_CONFIG.spark_namespace,
            application_file=SPARK_APP_TEMPLATE,
            params=params,
            kubernetes_conn_id="kubernetes_default",
            do_xcom_push=False,
        )
        if group_name == "snapshots-mutable-dims":
            snapshots_task = task
        else:
            facts_events_task = task

    start_batch_task >> registry_check
    registry_check >> skip_registry_task >> registry_ready
    registry_check >> init_registry_task >> registry_ready
    if snapshots_task and facts_events_task:
        registry_ready >> snapshots_task >> facts_events_task >> done_task
    elif snapshots_task:
        registry_ready >> snapshots_task >> done_task
    elif facts_events_task:
        registry_ready >> facts_events_task >> done_task
