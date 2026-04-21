from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import json
from pathlib import Path
from typing import Any

from airflow.sdk import Variable

DEFAULT_PROJECT_START_DATE = datetime(2025, 8, 24)
DEFAULT_NAMESPACE = "ampere"
DEFAULT_SPARK_IMAGE = "ghcr.io/antonminiazev/ampere-spark:latest"
DEFAULT_SPARK_SERVICE_ACCOUNT = "spark-operator-spark"
DEFAULT_MINIO_ENDPOINT = "http://minio.ampere.svc.cluster.local:9000"


def load_json_variable(name: str, default: Any) -> Any:
    """Read a JSON-like Airflow variable while tolerating invalid string payloads."""
    raw_value = Variable.get(name, default=default)
    if isinstance(raw_value, str):
        try:
            return json.loads(raw_value)
        except json.JSONDecodeError:
            return default
    return raw_value


def get_optional_variable(name: str) -> str | None:
    """Return a trimmed Airflow variable value or None when unset or blank."""
    value = Variable.get(name, default=None)
    if value is None:
        return None
    value = str(value).strip()
    return value or None


def get_optional_nonnegative_int_variable(name: str) -> int | None:
    """Return an optional non-negative integer Airflow variable."""
    value = get_optional_variable(name)
    if value is None:
        return None
    return max(int(value), 0)


def resolve_tagged_image(image_name: str, repository: str) -> str:
    """Resolve a tag from the shared GHCR image map into a full image reference."""
    image_map = load_json_variable("ghcr_images", {})
    image_tag = image_map.get(image_name, "latest")
    return f"{repository}:{image_tag}"


def resolve_spark_image() -> str:
    """Resolve the shared Spark image variable from either a full ref or a short tag."""
    value = get_optional_variable("ampere-spark-image")
    if not value:
        return DEFAULT_SPARK_IMAGE
    if "/" in value:
        return value
    return f"ghcr.io/antonminiazev/ampere-spark:{value}"


def minio_ssl_enabled(endpoint: str) -> str:
    """Translate an endpoint URL into SparkApplication's string SSL flag."""
    return "true" if endpoint.startswith("https://") else "false"


def strip_url_scheme(value: str) -> str:
    """Return host[:port] without an optional http/https scheme prefix."""
    text = (value or "").strip()
    if text.startswith("http://"):
        return text[len("http://") :]
    if text.startswith("https://"):
        return text[len("https://") :]
    return text


def spark_template_paths(anchor_file: str | Path) -> list[str]:
    """Return the Airflow template lookup paths for SparkApplication YAML files."""
    anchor_path = Path(anchor_file).resolve()
    return [
        str(anchor_path.parent),
        str(anchor_path.parents[1] / "sparkapplications"),
    ]


def standard_default_args(
    *,
    depends_on_past: bool = False,
    retries: int = 0,
) -> dict[str, Any]:
    """Return the default Airflow args shared by the ampere DAGs."""
    return {
        "owner": "airflow",
        "depends_on_past": depends_on_past,
        "start_date": DEFAULT_PROJECT_START_DATE,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "max_active_runs": 1,
        "retries": retries,
    }


@dataclass(frozen=True)
class PreRawDagConfig:
    namespace: str
    node_selector: dict[str, str]
    image: str
    pg_work_mem: str


def load_pre_raw_dag_config(image_name: str, repository: str) -> PreRawDagConfig:
    """Load shared KubernetesPodOperator settings for pre-raw generator DAGs."""
    return PreRawDagConfig(
        namespace=Variable.get("cluster_namespace", default=DEFAULT_NAMESPACE),
        node_selector={
            "kubernetes.io/hostname": Variable.get(
                "source_prep_node", default="ampere-k8s-node4"
            )
        },
        image=resolve_tagged_image(image_name, repository),
        pg_work_mem=Variable.get("pg_work_mem", default="64MB"),
    )


@dataclass(frozen=True)
class RawLandingDagConfig:
    spark_namespace: str
    service_account: str
    image: str
    image_pull_policy: str
    pg_host: str
    pg_port: str
    pg_database: str
    schema: str
    source_system: str
    minio_endpoint: str
    minio_bucket: str
    output_prefix: str
    driver_cores: int
    driver_core_request: str
    driver_memory: str
    driver_memory_overhead: str
    executor_cores: int
    executor_core_request: str
    executor_memory: str
    executor_memory_overhead: str
    executor_instances: int
    executor_instances_snapshots: int
    executor_instances_facts_events: int
    executor_memory_facts_events: str
    executor_memory_overhead_facts_events: str
    jdbc_fetchsize: int
    shuffle_partitions: int
    max_active_tasks: int
    template_paths: list[str]


def load_raw_landing_dag_config(anchor_file: str | Path) -> RawLandingDagConfig:
    """Load shared raw-landing DAG constants from Airflow variables.

    Produced config fields:
    - spark_namespace: Kubernetes namespace where SparkApplication objects run. Default `ampere`.
    - service_account: Spark driver service account used by the operator pods. Default `spark-operator-spark`.
    - image: Spark container image used for the raw landing app. Default `ghcr.io/antonminiazev/ampere-spark:latest` when `ampere-spark-image` is unset.
    - image_pull_policy: Kubernetes image pull policy for the Spark pods. Default `IfNotPresent`.
    - pg_host: PostgreSQL host used by JDBC extraction. Default `postgres-service`.
    - pg_port: PostgreSQL port used by JDBC extraction. Default `5432`.
    - pg_database: PostgreSQL database name used by JDBC extraction. Default `ampere_db`.
    - schema: Source PostgreSQL schema to extract from. Default `source`.
    - source_system: Source-system id written into raw metadata and paths. Default `postgres-pre-raw`.
    - minio_endpoint: MinIO/S3 endpoint used by Spark S3A IO. Default `http://minio.ampere.svc.cluster.local:9000`.
    - minio_bucket: Raw landing bucket name. Default `ampere-raw`.
    - output_prefix: Prefix under the raw bucket where extracts are written. Default `postgres-pre-raw`.
    - driver_cores: Spark driver CPU core count. Default `1`.
    - driver_core_request: Kubernetes CPU request for the Spark driver. Default `250m`.
    - driver_memory: Spark driver memory setting. Default `2500m`.
    - driver_memory_overhead: Extra Kubernetes memory overhead for the driver. Default `512m`.
    - executor_cores: Spark executor CPU core count. Default `1`.
    - executor_core_request: Kubernetes CPU request for each executor. Default `250m`.
    - executor_memory: Spark executor memory setting. Default `1536m`.
    - executor_memory_overhead: Extra Kubernetes memory overhead for each executor. Default `384m`.
    - executor_instances: Default executor count for raw jobs. Default `4`.
    - executor_instances_snapshots: Executor count override for snapshots group. Default `2`.
    - executor_instances_facts_events: Executor count override for facts/events group. Default `1`.
    - executor_memory_facts_events: Executor memory override for the facts/events SparkApplication. Default `2048m`.
    - executor_memory_overhead_facts_events: Executor memory overhead override for facts/events SparkApplication. Default `512m`.
    - jdbc_fetchsize: JDBC fetch batch size for PostgreSQL reads. Default `10000`.
    - shuffle_partitions: Default spark.sql.shuffle.partitions value. Default `4`.
    - max_active_tasks: Airflow max_active_tasks limit for the DAG. Default `1`.
    - template_paths: Template search paths for SparkApplication YAML rendering. Default is derived from `anchor_file` plus `dags/sparkapplications`.
    """
    return RawLandingDagConfig(
        spark_namespace=Variable.get("spark_namespace", default=DEFAULT_NAMESPACE),
        service_account=Variable.get(
            "spark_service_account",
            default=DEFAULT_SPARK_SERVICE_ACCOUNT,
        ),
        image=resolve_spark_image(),
        image_pull_policy=Variable.get("image_pull_policy", default="IfNotPresent"),
        pg_host=Variable.get("pg_host", default="postgres-service"),
        pg_port=Variable.get("pg_port", default="5432"),
        pg_database=Variable.get("pg_database", default="ampere_db"),
        schema=Variable.get("pg_schema", default="source"),
        source_system=Variable.get("raw_source_system", default="postgres-pre-raw"),
        minio_endpoint=Variable.get(
            "minio_s3_endpoint",
            default=DEFAULT_MINIO_ENDPOINT,
        ),
        minio_bucket=Variable.get("minio_raw_bucket", default="ampere-raw"),
        output_prefix=Variable.get("raw_output_prefix", default="postgres-pre-raw"),
        driver_cores=int(Variable.get("spark_driver_cores", default="1")),
        driver_core_request=Variable.get("spark_driver_core_request", default="250m"),
        driver_memory=Variable.get("spark_driver_memory", default="2500m"),
        driver_memory_overhead=Variable.get(
            "spark_driver_memory_overhead", default="512m"
        ),
        executor_cores=int(Variable.get("spark_executor_cores", default="1")),
        executor_core_request=Variable.get(
            "spark_executor_core_request", default="250m"
        ),
        executor_memory=Variable.get("spark_executor_memory", default="1536m"),
        executor_memory_overhead=Variable.get(
            "spark_executor_memory_overhead", default="384m"
        ),
        executor_instances=int(Variable.get("spark_executor_instances", default="4")),
        executor_instances_snapshots=int(
            Variable.get("spark_executor_instances_snapshots", default="2")
        ),
        executor_instances_facts_events=int(
            Variable.get("spark_executor_instances_facts_events", default="1")
        ),
        executor_memory_facts_events=Variable.get(
            "spark_executor_memory_facts_events", default="2048m"
        ),
        executor_memory_overhead_facts_events=Variable.get(
            "spark_executor_memory_overhead_facts_events", default="512m"
        ),
        jdbc_fetchsize=max(int(Variable.get("spark_jdbc_fetchsize", default="10000")), 1),
        shuffle_partitions=int(
            Variable.get("spark_sql_shuffle_partitions", default="4")
        ),
        max_active_tasks=int(
            Variable.get("spark_source_to_raw_max_active_tasks", default="2")
        ),
        template_paths=spark_template_paths(anchor_file),
    )


@dataclass(frozen=True)
class BronzeDagConfig:
    spark_namespace: str
    service_account: str
    image: str
    image_pull_policy: str
    minio_endpoint: str
    minio_conn_id: str
    schema: str
    raw_bucket: str
    raw_prefix: str
    source_system: str
    driver_cores: int
    driver_core_request: str
    driver_memory: str
    driver_memory_overhead: str
    executor_cores: int
    executor_core_request: str
    executor_memory: str
    executor_memory_overhead: str
    executor_instances: int
    executor_instances_snapshots: int
    executor_instances_facts_events: int
    executor_memory_snapshots: str
    executor_memory_facts_events: str
    executor_memory_overhead_facts_events: str
    executor_node_selector: str
    shuffle_partitions: int
    shuffle_partitions_facts_events: int
    shuffle_partitions_mutable_dims: int
    files_max_partition_bytes_facts_events: str
    files_open_cost_bytes_facts_events: str
    adaptive_coalesce_facts_events: str
    max_active_tasks: int
    uc_enabled: str
    uc_catalog: str
    uc_bronze_schema: str
    uc_ops_schema: str
    registry_location: str
    uc_api_uri: str
    uc_token: str
    uc_auth_type: str
    uc_catalog_impl: str
    template_paths: list[str]


def load_bronze_dag_config(anchor_file: str | Path) -> BronzeDagConfig:
    """Load shared bronze DAG constants from Airflow variables.

    Produced config fields:
    - spark_namespace: Kubernetes namespace where SparkApplication objects run. Default `ampere`.
    - service_account: Spark driver service account used by the operator pods. Default `spark-operator-spark`.
    - image: Spark container image used for the bronze app. Default `ghcr.io/antonminiazev/ampere-spark:latest` when `ampere-spark-image` is unset.
    - image_pull_policy: Kubernetes image pull policy for the Spark pods. Default `IfNotPresent`.
    - minio_endpoint: MinIO/S3 endpoint used by Spark S3A IO. Default `http://minio.ampere.svc.cluster.local:9000`.
    - minio_conn_id: Airflow connection id used for MinIO registry existence checks. Default `minio_conn`.
    - schema: Source schema name used in raw and bronze metadata. Default `source`.
    - raw_bucket: Raw landing bucket name. Default `ampere-raw`.
    - raw_prefix: Prefix under the raw bucket where landing batches live. Default `postgres-pre-raw`.
    - source_system: Source-system id used in manifests and registry rows. Default `postgres-pre-raw`.
    - driver_cores: Spark driver CPU core count. Default `1`.
    - driver_core_request: Kubernetes CPU request for the Spark driver. Default `400m`.
    - driver_memory: Spark driver memory setting. Default `2500m`.
    - driver_memory_overhead: Extra Kubernetes memory overhead for the driver. Default `512m`.
    - executor_cores: Spark executor CPU core count. Default `1`.
    - executor_core_request: Kubernetes CPU request for each executor. Default `300m`.
    - executor_memory: Spark executor memory setting. Default `1536m`.
    - executor_memory_overhead: Extra Kubernetes memory overhead for each executor. Default `384m`.
    - executor_instances: Default executor count for bronze jobs. Default `3`.
    - executor_instances_snapshots: Executor count override for snapshots group. Default `2`.
    - executor_instances_facts_events: Executor count override for facts/events group. Default `2`.
    - executor_memory_snapshots: Executor memory override for the snapshots/mutable_dims SparkApplication. Default `2560m`.
    - executor_memory_facts_events: Executor memory override for the facts/events SparkApplication. Default `2048m`.
    - executor_memory_overhead_facts_events: Executor memory overhead override for the facts/events SparkApplication. Default `512m`.
    - executor_node_selector: Kubernetes node hostname used for executor placement. Default `ampere-k8s-node4`.
    - shuffle_partitions: Default spark.sql.shuffle.partitions value. Default `1`.
    - shuffle_partitions_facts_events: Shuffle partition override for facts/events. Default `8`.
    - shuffle_partitions_mutable_dims: Shuffle partition override for mutable dims. Default `1`.
    - files_max_partition_bytes_facts_events: File split size override for facts/events scans. Default `8m`.
    - files_open_cost_bytes_facts_events: File open cost override for facts/events scans. Default `4m`.
    - adaptive_coalesce_facts_events: Adaptive partition coalescing flag for facts/events. Default `false`.
    - max_active_tasks: Airflow max_active_tasks limit for the DAG. Default `2`.
    - uc_enabled: Unity Catalog enablement flag passed into the Spark app. Default `true`.
    - uc_catalog: Unity Catalog catalog name for bronze objects. Default `ampere`.
    - uc_bronze_schema: Unity Catalog schema name for bronze tables. Default `bronze`.
    - uc_ops_schema: Unity Catalog schema name for operational tables. Default `ops`.
    - registry_location: Storage location used only for registry bootstrap/recovery. Default `s3a://ampere-bronze/bronze/ops/bronze_apply_registry`.
    - uc_api_uri: Unity Catalog API endpoint. Default `http://unity-catalog-unitycatalog-server.unity-catalog.svc.cluster.local:8080`.
    - uc_token: Unity Catalog auth token used by Spark config. Default `local-dev-token`.
    - uc_auth_type: Unity Catalog auth mode used by Spark config. Default `static`.
    - uc_catalog_impl: Spark catalog implementation class for Unity Catalog. Default `io.unitycatalog.spark.UCSingleCatalog`.
    - template_paths: Template search paths for SparkApplication YAML rendering. Default is derived from `anchor_file` plus `dags/sparkapplications`.
    """
    bronze_bucket = Variable.get("minio_bronze_bucket", default="ampere-bronze")
    bronze_prefix = Variable.get("bronze_output_prefix", default="bronze").strip("/")
    default_registry_location = (
        f"s3a://{bronze_bucket}/{bronze_prefix}/ops/bronze_apply_registry"
        if bronze_prefix
        else f"s3a://{bronze_bucket}/ops/bronze_apply_registry"
    )
    return BronzeDagConfig(
        spark_namespace=Variable.get("spark_namespace", default=DEFAULT_NAMESPACE),
        service_account=Variable.get(
            "spark_service_account",
            default=DEFAULT_SPARK_SERVICE_ACCOUNT,
        ),
        image=resolve_spark_image(),
        image_pull_policy=Variable.get("image_pull_policy", default="IfNotPresent"),
        minio_endpoint=Variable.get(
            "minio_s3_endpoint",
            default=DEFAULT_MINIO_ENDPOINT,
        ),
        minio_conn_id=Variable.get("minio_conn_id", default="minio_conn"),
        schema=Variable.get("pg_schema", default="source"),
        raw_bucket=Variable.get("minio_raw_bucket", default="ampere-raw"),
        raw_prefix=Variable.get("raw_output_prefix", default="postgres-pre-raw"),
        source_system=Variable.get("raw_source_system", default="postgres-pre-raw"),
        driver_cores=int(Variable.get("spark_driver_cores", default="1")),
        driver_core_request=Variable.get("spark_driver_core_request", default="400m"),
        driver_memory=Variable.get("spark_driver_memory", default="2500m"),
        driver_memory_overhead=Variable.get(
            "spark_driver_memory_overhead", default="512m"
        ),
        executor_cores=int(Variable.get("spark_executor_cores", default="1")),
        executor_core_request=Variable.get(
            "spark_executor_core_request", default="300m"
        ),
        executor_memory=Variable.get("spark_executor_memory", default="1536m"),
        executor_memory_overhead=Variable.get(
            "spark_executor_memory_overhead", default="384m"
        ),
        executor_instances=int(Variable.get("spark_executor_instances", default="3")),
        executor_instances_snapshots=int(
            Variable.get("spark_executor_instances_snapshots", default="2")
        ),
        executor_instances_facts_events=int(
            Variable.get("spark_executor_instances_facts_events", default="2")
        ),
        executor_memory_snapshots=Variable.get(
            "spark_executor_memory_snapshots", default="2560m"
        ),
        executor_memory_facts_events=Variable.get(
            "spark_executor_memory_facts_events", default="2048m"
        ),
        executor_memory_overhead_facts_events=Variable.get(
            "spark_executor_memory_overhead_facts_events", default="512m"
        ),
        executor_node_selector=Variable.get(
            "spark_executor_node_selector", default="ampere-k8s-node4"
        ),
        shuffle_partitions=int(
            Variable.get("spark_sql_shuffle_partitions", default="1")
        ),
        shuffle_partitions_facts_events=int(
            Variable.get("spark_sql_shuffle_partitions_facts_events", default="8")
        ),
        shuffle_partitions_mutable_dims=int(
            Variable.get("spark_sql_shuffle_partitions_mutable_dims", default="1")
        ),
        files_max_partition_bytes_facts_events=Variable.get(
            "spark_sql_files_max_partition_bytes_facts_events", default="8m"
        ),
        files_open_cost_bytes_facts_events=Variable.get(
            "spark_sql_files_open_cost_bytes_facts_events", default="4m"
        ),
        adaptive_coalesce_facts_events=Variable.get(
            "spark_sql_adaptive_coalesce_facts_events", default="false"
        ),
        max_active_tasks=int(
            Variable.get("spark_raw_to_bronze_max_active_tasks", default="2")
        ),
        uc_enabled=Variable.get("spark_uc_enabled", default="true").strip().lower(),
        uc_catalog=Variable.get("spark_uc_catalog", default="ampere"),
        uc_bronze_schema=Variable.get("spark_uc_bronze_schema", default="bronze"),
        uc_ops_schema=Variable.get("spark_uc_ops_schema", default="ops"),
        registry_location=Variable.get(
            "spark_uc_bronze_registry_location",
            default=default_registry_location,
        ),
        uc_api_uri=Variable.get(
            "spark_uc_api_uri",
            default="http://unity-catalog-unitycatalog-server.unity-catalog.svc.cluster.local:8080",
        ),
        uc_token=Variable.get("spark_uc_token", default="local-dev-token"),
        uc_auth_type=Variable.get("spark_uc_auth_type", default="static"),
        uc_catalog_impl=Variable.get(
            "spark_uc_catalog_impl",
            default="io.unitycatalog.spark.UCSingleCatalog",
        ),
        template_paths=spark_template_paths(anchor_file),
    )


@dataclass(frozen=True)
class SilverDagConfig:
    namespace: str
    image: str
    image_pull_policy: str
    service_account: str
    node_selector: dict[str, str]
    minio_endpoint: str
    minio_use_ssl: str
    uc_api_uri: str
    uc_token: str
    bronze_uc_catalog: str
    bronze_uc_schema: str
    bronze_source_name: str
    bronze_source_schema: str
    bronze_source_mapping_path: str
    bronze_source_mapping_max_age_hours: str
    run_uc_mapping_generation: str
    run_bronze_preflight: str
    run_bronze_preflight_delta_scan: str
    dbt_target: str
    dbt_threads: str
    dbt_command: str
    max_active_runs: int


def load_silver_dag_config() -> SilverDagConfig:
    """Load shared silver dbt-runtime DAG constants from Airflow variables."""
    raw_minio_endpoint = Variable.get("minio_s3_endpoint", default="s3.minio.local")
    minio_endpoint = strip_url_scheme(raw_minio_endpoint)
    minio_use_ssl = Variable.get("minio_s3_use_ssl", default="true").strip().lower()
    return SilverDagConfig(
        namespace=Variable.get("cluster_namespace", default=DEFAULT_NAMESPACE),
        image=Variable.get(
            "ampere_silver_dbt_image",
            default="ghcr.io/antonminiazev/ampere-silver-dbt:latest",
        ),
        image_pull_policy=Variable.get("image_pull_policy", default="IfNotPresent"),
        service_account=Variable.get(
            "spark_service_account",
            default=DEFAULT_SPARK_SERVICE_ACCOUNT,
        ),
        node_selector={
            "kubernetes.io/hostname": Variable.get(
                "silver_runtime_node",
                default="ampere-k8s-node3",
            )
        },
        minio_endpoint=minio_endpoint,
        minio_use_ssl=minio_use_ssl,
        uc_api_uri=Variable.get(
            "spark_uc_api_uri",
            default="http://unity-catalog-unitycatalog-server.unity-catalog.svc.cluster.local:8080",
        ),
        uc_token=Variable.get("spark_uc_token", default="local-dev-token"),
        bronze_uc_catalog=Variable.get("spark_uc_catalog", default="ampere"),
        bronze_uc_schema=Variable.get("spark_uc_bronze_schema", default="bronze"),
        bronze_source_name=Variable.get("bronze_source_name", default="bronze"),
        bronze_source_schema=Variable.get("bronze_source_schema", default="bronze"),
        bronze_source_mapping_path=Variable.get(
            "bronze_source_mapping_path",
            default="/app/artifacts/bronze_source_mapping.json",
        ),
        bronze_source_mapping_max_age_hours=Variable.get(
            "bronze_source_mapping_max_age_hours",
            default="24",
        ),
        run_uc_mapping_generation=Variable.get(
            "run_uc_mapping_generation",
            default="true",
        ).strip().lower(),
        run_bronze_preflight=Variable.get(
            "run_bronze_preflight",
            default="true",
        ).strip().lower(),
        run_bronze_preflight_delta_scan=Variable.get(
            "run_bronze_preflight_delta_scan",
            default="true",
        ).strip().lower(),
        dbt_target=Variable.get("silver_dbt_target", default="prod"),
        dbt_threads=Variable.get("silver_dbt_threads", default="4"),
        dbt_command=Variable.get(
            "silver_dbt_command",
            default=(
                "dbt build --selector silver_staging "
                "silver_dimensions_facts silver_publishable"
            ),
        ),
        max_active_runs=int(Variable.get("silver_dag_max_active_runs", default="1")),
    )
