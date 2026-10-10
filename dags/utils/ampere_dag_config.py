from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from airflow.hooks.base import BaseHook
from airflow.sdk import Variable

DEFAULT_PROJECT_START_DATE = datetime(2025, 8, 24)
DEFAULT_NAMESPACE = "ampere"
DEFAULT_RELEASE_VERSION = "latest"
DEFAULT_SPARK_SERVICE_ACCOUNT = "spark-operator-spark"
DEFAULT_MINIO_ENDPOINT = "http://minio.ampere.svc.cluster.local:9000"
DEFAULT_ETL_NODE = "ampere-k8s-node4"
ICEBERG_MUTATION_POOL = "iceberg_pipeline_mutation"


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


def get_release_version() -> str:
    """Return the shared release version used as the default image tag."""
    return Variable.get(
        "ampere_release_version", default=DEFAULT_RELEASE_VERSION
    ).strip()


def resolve_release_image(
    repository: str,
) -> str:
    """Resolve an image from the shared Airflow release version."""
    release_version = get_release_version()
    return f"{repository}:{release_version or DEFAULT_RELEASE_VERSION}"


def resolve_spark_image() -> str:
    """Resolve the Iceberg Spark image for Raw and Bronze."""
    repository = "ghcr.io/antonminiazev/ampere-spark-iceberg"
    image = get_optional_variable("iceberg_spark_image") or resolve_release_image(repository)
    if not image.startswith(f"{repository}:"):
        raise ValueError("iceberg_spark_image must name the Iceberg repository")
    return image


def resolve_spark_connect_client_image() -> str:
    """Resolve the lightweight image used by Spark Connect client pods."""
    repository = "ghcr.io/antonminiazev/ampere-spark-connect-client"
    image = get_optional_variable("iceberg_spark_connect_client_image") or resolve_release_image(
        repository
    )
    if not image.startswith(f"{repository}:"):
        raise ValueError(
            "iceberg_spark_connect_client_image must name the Spark Connect client repository"
        )
    return image


def minio_ssl_enabled(endpoint: str) -> str:
    """Translate an endpoint URL into SparkApplication's string SSL flag."""
    return "true" if endpoint.startswith("https://") else "false"


def resolve_minio_endpoint(conn_id: str = "minio_conn") -> str:
    """Resolve the MinIO endpoint URL from the shared Airflow connection."""
    connection = BaseHook.get_connection(conn_id)
    endpoint = (connection.extra_dejson or {}).get("endpoint_url")
    if endpoint:
        return str(endpoint).strip().rstrip("/")
    return DEFAULT_MINIO_ENDPOINT


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


def load_pre_raw_dag_config(repository: str) -> PreRawDagConfig:
    """Load shared KubernetesPodOperator settings for pre-raw generator DAGs."""
    return PreRawDagConfig(
        namespace=Variable.get("cluster_namespace", default=DEFAULT_NAMESPACE),
        node_selector={
            "kubernetes.io/hostname": DEFAULT_ETL_NODE,
        },
        image=resolve_release_image(repository),
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
    - image: Spark container image used for raw landing. Defaults to `ghcr.io/antonminiazev/ampere-spark-iceberg:<ampere_release_version>`.
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
    - executor_instances_facts_events: Executor count override for facts/events group. Default `2`.
    - executor_memory_facts_events: Executor memory override for the facts/events SparkApplication. Default `1536m`.
    - executor_memory_overhead_facts_events: Executor memory overhead override for facts/events SparkApplication. Default `512m`.
    - jdbc_fetchsize: JDBC fetch batch size for PostgreSQL reads. Default `10000`.
    - shuffle_partitions: Default spark.sql.shuffle.partitions value. Default `4`.
    - max_active_tasks: Airflow max_active_tasks limit for the DAG. Default `1`.
    - template_paths: Template search paths for SparkApplication YAML rendering. Default is derived from `anchor_file` plus `dags/sparkapplications`.
    """
    minio_conn_id = Variable.get("minio_conn_id", default="minio_conn")
    minio_endpoint = resolve_minio_endpoint(minio_conn_id)
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
        minio_endpoint=minio_endpoint,
        minio_bucket=Variable.get("minio_raw_bucket", default="ampere-raw"),
        output_prefix=Variable.get("raw_output_prefix", default="postgres-pre-raw"),
        driver_cores=int(Variable.get("spark_driver_cores", default="1")),
        driver_core_request=Variable.get("spark_driver_core_request", default="400m"),
        driver_memory=Variable.get("spark_driver_memory", default="2500m"),
        driver_memory_overhead=Variable.get(
            "spark_driver_memory_overhead", default="768m"
        ),
        executor_cores=int(Variable.get("spark_executor_cores", default="1")),
        executor_core_request=Variable.get(
            "spark_executor_core_request", default="400m"
        ),
        executor_memory=Variable.get("spark_executor_memory", default="2000m"),
        executor_memory_overhead=Variable.get(
            "spark_executor_memory_overhead", default="512m"
        ),
        executor_instances=int(Variable.get("spark_executor_instances", default="4")),
        executor_instances_snapshots=int(
            Variable.get("spark_executor_instances_snapshots", default="3")
        ),
        executor_instances_facts_events=int(
            Variable.get("spark_executor_instances_facts_events", default="3")
        ),
        executor_memory_facts_events=Variable.get(
            "spark_executor_memory_facts_events", default="1536m"
        ),
        executor_memory_overhead_facts_events=Variable.get(
            "spark_executor_memory_overhead_facts_events", default="512m"
        ),
        jdbc_fetchsize=max(
            int(Variable.get("spark_jdbc_fetchsize", default="10000")), 1
        ),
        shuffle_partitions=int(
            Variable.get("spark_sql_shuffle_partitions", default="4")
        ),
        max_active_tasks=int(
            Variable.get("spark_source_to_raw_max_active_tasks", default="3")
        ),
        template_paths=spark_template_paths(anchor_file),
    )


@dataclass(frozen=True)
class BronzeDagConfig:
    spark_namespace: str
    service_account: str
    minio_endpoint: str
    schema: str
    raw_bucket: str
    raw_prefix: str
    source_system: str
    driver_cores: int
    driver_core_request: str
    driver_memory: str
    driver_memory_overhead: str
    driver_node_selector: str
    executor_cores: int
    executor_core_request: str
    executor_cores_facts_events: int
    executor_core_request_facts_events: str
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
    template_paths: list[str]


def load_bronze_dag_config(anchor_file: str | Path) -> BronzeDagConfig:
    """Load Spark sizing and Raw source settings for Iceberg Bronze."""
    minio_conn_id = Variable.get("minio_conn_id", default="minio_conn")
    return BronzeDagConfig(
        spark_namespace=Variable.get("spark_namespace", default=DEFAULT_NAMESPACE),
        service_account=Variable.get("spark_service_account", default=DEFAULT_SPARK_SERVICE_ACCOUNT),
        minio_endpoint=resolve_minio_endpoint(minio_conn_id),
        schema=Variable.get("pg_schema", default="source"),
        raw_bucket=Variable.get("minio_raw_bucket", default="ampere-raw"),
        raw_prefix=Variable.get("raw_output_prefix", default="postgres-pre-raw"),
        source_system=Variable.get("raw_source_system", default="postgres-pre-raw"),
        driver_cores=int(Variable.get("spark_driver_cores", default="1")),
        driver_core_request=Variable.get("spark_driver_core_request", default="400m"),
        driver_memory=Variable.get("spark_bronze_driver_memory", default="2000m"),
        driver_memory_overhead=Variable.get("spark_bronze_driver_memory_overhead", default="512"),
        driver_node_selector=Variable.get("spark_bronze_driver_node_selector", default="ampere-k8s-node2"),
        executor_cores=int(Variable.get("spark_executor_cores", default="1")),
        executor_core_request=Variable.get("spark_executor_core_request", default="300m"),
        executor_cores_facts_events=int(Variable.get("spark_bronze_executor_cores_facts_events", default="2")),
        executor_core_request_facts_events=Variable.get("spark_bronze_executor_core_request_facts_events", default="1"),
        executor_memory=Variable.get("spark_executor_memory", default="1536m"),
        executor_memory_overhead=Variable.get("spark_executor_memory_overhead", default="384m"),
        executor_instances=int(Variable.get("spark_executor_instances", default="3")),
        executor_instances_snapshots=int(Variable.get("spark_executor_instances_snapshots", default="2")),
        executor_instances_facts_events=int(Variable.get("spark_executor_instances_facts_events", default="2")),
        executor_memory_snapshots=Variable.get("spark_executor_memory_snapshots", default="2560m"),
        executor_memory_facts_events=Variable.get("spark_executor_memory_facts_events", default="3072m"),
        executor_memory_overhead_facts_events=Variable.get("spark_executor_memory_overhead_facts_events", default="768m"),
        executor_node_selector=Variable.get("spark_executor_node_selector", default="ampere-k8s-node4"),
        shuffle_partitions=int(Variable.get("spark_sql_shuffle_partitions", default="2")),
        shuffle_partitions_facts_events=int(Variable.get("spark_sql_shuffle_partitions_facts_events", default="8")),
        shuffle_partitions_mutable_dims=int(Variable.get("spark_sql_shuffle_partitions_mutable_dims", default="2")),
        files_max_partition_bytes_facts_events=Variable.get("spark_sql_files_max_partition_bytes_facts_events", default="8m"),
        files_open_cost_bytes_facts_events=Variable.get("spark_sql_files_open_cost_bytes_facts_events", default="4m"),
        adaptive_coalesce_facts_events=Variable.get("spark_sql_adaptive_coalesce_facts_events", default="false"),
        template_paths=spark_template_paths(anchor_file),
    )


@dataclass(frozen=True)
class SilverDagConfig:
    namespace: str
    service_account: str
    node_selector: dict[str, str]
    minio_endpoint: str


def load_silver_dag_config() -> SilverDagConfig:
    """Load shared pod placement and MinIO settings for Iceberg dbt DAGs."""
    return SilverDagConfig(
        namespace=Variable.get("cluster_namespace", default=DEFAULT_NAMESPACE),
        service_account=Variable.get("spark_service_account", default=DEFAULT_SPARK_SERVICE_ACCOUNT),
        node_selector={"kubernetes.io/hostname": DEFAULT_ETL_NODE},
        minio_endpoint=strip_url_scheme(resolve_minio_endpoint()),
    )
