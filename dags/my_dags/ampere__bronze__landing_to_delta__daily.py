from __future__ import annotations

from datetime import datetime
import logging

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
    get_optional_variable,
    load_bronze_dag_config,
    minio_ssl_enabled,
    standard_default_args,
)
from utils.stream_group_config import build_bronze_stream_groups

DAG_ID = "ampere__bronze__landing_to_delta__daily"
DAG_CONFIG = load_bronze_dag_config(__file__)
SPARK_APP_TEMPLATE = "raw_to_bronze_template_uc.yaml"
REGISTRY_INIT_TEMPLATE = "bronze_registry_init_uc.yaml"


def _parse_s3_path(path_str: str) -> tuple[str, str]:
    """Split an s3:// or s3a:// path into bucket and key prefix."""
    if path_str.startswith("s3a://"):
        bucket_key = path_str[len("s3a://") :]
    elif path_str.startswith("s3://"):
        bucket_key = path_str[len("s3://") :]
    else:
        raise ValueError(f"Unsupported registry path: {path_str}")
    bucket, _, key = bucket_key.partition("/")
    if not bucket:
        raise ValueError(f"Missing bucket in registry path: {path_str}")
    return bucket, key


def _registry_exists_in_minio() -> bool:
    """Check whether the registry Delta log already exists in MinIO."""
    logger = logging.getLogger(DAG_ID)
    bucket, key_prefix = _parse_s3_path(DAG_CONFIG.registry_location)
    delta_prefix = key_prefix.strip("/")
    if delta_prefix:
        delta_prefix = f"{delta_prefix}/_delta_log/"
    else:
        delta_prefix = "_delta_log/"

    try:
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    except Exception as exc:  # noqa: BLE001
        logger.warning("S3Hook unavailable for registry check: %s", exc)
    else:
        try:
            s3 = S3Hook(aws_conn_id=DAG_CONFIG.minio_conn_id)
            client = s3.get_conn()
            response = client.list_objects_v2(
                Bucket=bucket,
                Prefix=delta_prefix,
                MaxKeys=1,
            )
            return bool(response.get("Contents"))
        except Exception as exc:  # noqa: BLE001
            logger.warning("Registry check via S3Hook failed: %s", exc)

    access_key = get_optional_variable("minio_access_key")
    secret_key = get_optional_variable("minio_secret_key")
    if not access_key or not secret_key:
        logger.warning("Missing MinIO creds; running registry init.")
        return False

    try:
        import boto3
        from botocore.client import Config
    except Exception as exc:  # noqa: BLE001
        logger.warning("boto3 unavailable for registry check: %s", exc)
        return False

    client = boto3.client(
        "s3",
        endpoint_url=DAG_CONFIG.minio_endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )
    response = client.list_objects_v2(
        Bucket=bucket,
        Prefix=delta_prefix,
        MaxKeys=1,
    )
    return bool(response.get("Contents"))


def _select_registry_task() -> str:
    """Skip registry creation when the Delta log already exists in MinIO."""
    return (
        "skip__sparkapp__registry_init"
        if _registry_exists_in_minio()
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
        "registry_location": DAG_CONFIG.registry_location,
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
        executor_memory = (
            DAG_CONFIG.executor_memory
            if group_name == "snapshots-mutable-dims"
            else DAG_CONFIG.executor_memory_facts_events
        )
        executor_memory_overhead = (
            DAG_CONFIG.executor_memory_overhead
            if group_name == "snapshots-mutable-dims"
            else DAG_CONFIG.executor_memory_overhead_facts_events
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
            "executor_memory": executor_memory,
            "executor_memory_overhead": executor_memory_overhead,
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
