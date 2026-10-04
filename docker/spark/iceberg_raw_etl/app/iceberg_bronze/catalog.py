"""Lakekeeper Bronze catalog setup driven by the existing table contract."""

from __future__ import annotations

import json
import logging
import os
import re
from functools import lru_cache
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, functions as F

DEFAULT_CONTRACT = Path("/opt/spark/app/bronze_contract.json")
VALID_SQL_TYPE = re.compile(
    r"(?:boolean|date|timestamp|smallint|int|string|decimal\(\d+,\d+\))",
    re.IGNORECASE,
)


def quote_ident(value: str) -> str:
    """Quote a contract identifier for Spark SQL."""
    return "`" + value.replace("`", "``") + "`"


def parse_bool_flag(value: object, default: bool = False) -> bool:
    """Interpret a CLI or contract boolean without Python's string truthiness."""
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


@lru_cache(maxsize=1)
def _bronze_tables() -> dict[str, dict]:
    """Index the canonical Bronze contract by table name."""
    path = Path(os.getenv("ICEBERG_CONTRACT_PATH", str(DEFAULT_CONTRACT)))
    contract = json.loads(path.read_text(encoding="utf-8"))
    tables = contract["catalog"]["layers"]["bronze"]["tables"]
    return {entry["table_name"]: entry for entry in tables}


def _table_spec(schema: str, table: str) -> dict:
    """Reject targets absent from the canonical Bronze contract."""
    spec = _bronze_tables().get(table)
    if spec is None or spec["schema_name"] != schema:
        raise ValueError(f"Missing Iceberg contract for {schema}.{table}")
    return spec


def configure_lakekeeper_catalog(
    spark: SparkSession,
    *,
    catalog: str,
    warehouse: str,
    uri: str,
    oauth_uri: str,
    scope: str,
    client_id: str,
    client_secret: str,
    minio_endpoint: str,
) -> None:
    """Configure the REST catalog before Spark first resolves its name."""
    if not all((warehouse, uri, oauth_uri, scope, client_id, client_secret)):
        raise ValueError("Lakekeeper warehouse, URL, OAuth settings and client are required")
    prefix = f"spark.sql.catalog.{catalog}"
    settings = {
        prefix: "org.apache.iceberg.spark.SparkCatalog",
        f"{prefix}.type": "rest",
        f"{prefix}.uri": uri,
        f"{prefix}.warehouse": warehouse,
        f"{prefix}.rest.auth.type": "oauth2",
        f"{prefix}.credential": f"{client_id}:{client_secret}",
        f"{prefix}.oauth2-server-uri": oauth_uri,
        f"{prefix}.scope": scope,
        f"{prefix}.token-exchange-enabled": "false",
        f"{prefix}.header.X-Iceberg-Access-Delegation": "client-managed",
        f"{prefix}.io-impl": "org.apache.iceberg.hadoop.HadoopFileIO",
    }
    for key, value in settings.items():
        spark.conf.set(key, value)
    hadoop = spark._jsc.hadoopConfiguration()
    hadoop.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop.set("fs.AbstractFileSystem.s3.impl", "org.apache.hadoop.fs.s3a.S3A")
    hadoop.set("fs.s3a.endpoint", minio_endpoint)


def ensure_iceberg_table(
    spark: SparkSession,
    *,
    catalog: str,
    schema: str,
    table: str,
    logger: logging.Logger,
) -> str:
    """Create an isolated Iceberg target with the current Bronze column model."""
    spec = _table_spec(schema, table)
    fq_schema = f"{quote_ident(catalog)}.{quote_ident(schema)}"
    fqtn = f"{fq_schema}.{quote_ident(table)}"
    column_specs = sorted(spec["columns"], key=lambda item: item["position"])
    if schema == "ops" and table == "bronze_apply_registry":
        # The UC external-table contract has no columns for this operational
        # table; its writer schema is the authoritative registry definition.
        registry_path = Path(__file__).with_name("bronze_apply_registry_schema.json")
        column_specs = [
            {"name": field["name"], "type_text": field["type"]}
            for field in json.loads(registry_path.read_text(encoding="utf-8"))["fields"]
        ]
    columns = []
    for column in column_specs:
        data_type = str(column["type_text"]).strip().lower()
        if not VALID_SQL_TYPE.fullmatch(data_type):
            raise ValueError(f"Unsupported contract type for {table}: {data_type}")
        columns.append(f"{quote_ident(column['name'])} {data_type}")
    if not columns:
        raise ValueError(f"No contract columns for {schema}.{table}")
    partition_key = (
        spec.get("stream_group", {}).get("group_config", {}).get("partition_key")
    )
    if schema == "ops":
        partition_key = "source_table"
    column_names = {column["name"] for column in column_specs}
    # Mutable dimensions use extract_date only to locate Raw batches. Their
    # Bronze contract has no extract_date column, so keep them unpartitioned.
    if partition_key and partition_key not in column_names:
        if partition_key == "extract_date":
            partition_key = None
        else:
            raise ValueError(f"Partition column {partition_key} absent from {schema}.{table}")
    partition_sql = (
        f" PARTITIONED BY ({quote_ident(partition_key)})" if partition_key else ""
    )
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {fq_schema}")
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {fqtn} ({', '.join(columns)}) "
        f"USING iceberg{partition_sql} "
        "TBLPROPERTIES ('format-version' = '2')"
    )
    logger.info("Ensured Iceberg target %s", fqtn)
    return fqtn


def align_df_to_iceberg_schema(
    spark: SparkSession,
    df: DataFrame,
    *,
    catalog: str,
    schema: str,
    table: str,
    logger: logging.Logger,
) -> DataFrame:
    """Cast and order a Raw batch to the existing Bronze contract."""
    del spark
    spec = _table_spec(schema, table)
    expressions = []
    input_columns = set(df.columns)
    for column in sorted(spec["columns"], key=lambda item: item["position"]):
        name = column["name"]
        dtype = column["type_text"]
        expression = F.col(quote_ident(name)) if name in input_columns else F.lit(None)
        expressions.append(expression.cast(dtype).alias(name))
    logger.info("Aligned %s.%s.%s to %s Bronze columns", catalog, schema, table, len(expressions))
    return df.select(*expressions)
