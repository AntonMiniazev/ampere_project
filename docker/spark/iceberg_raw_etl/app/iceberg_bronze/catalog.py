"""Lakekeeper Bronze catalog setup driven by the existing table contract."""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession, functions as F
from tools.contracts.ampere_contract import ResolvedTable, load_contract
from tools.contracts.spark_conformance import quote_identifier, validate_spark_table

CONTRACT = load_contract()


quote_ident = quote_identifier


def parse_bool_flag(value: object, default: bool = False) -> bool:
    """Interpret a CLI or contract boolean without Python's string truthiness."""
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _table_spec(schema: str, table: str) -> ResolvedTable:
    """Resolve a Bronze table from the shared v3 contract."""
    spec = CONTRACT.table("bronze", table)
    if spec.namespace != schema:
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
    """Require a pre-initialized Bronze table to match contract v3."""
    spec = _table_spec(schema, table)
    fq_schema = f"{quote_ident(catalog)}.{quote_ident(schema)}"
    fqtn = f"{fq_schema}.{quote_ident(table)}"
    try:
        validate_spark_table(spark, catalog, spec)
    except RuntimeError as exc:
        raise RuntimeError(
            f"Contract v{CONTRACT.version} expects initialized table {fqtn}; "
            "run ampere__iceberg__catalog__init first"
        ) from exc
    logger.info("Validated contract v%s table %s", CONTRACT.version, fqtn)
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
    for column in spec.columns:
        name = column["name"]
        dtype = column["type_text"]
        expression = F.col(quote_ident(name)) if name in input_columns else F.lit(None)
        expressions.append(expression.cast(dtype).alias(name))
    logger.info("Aligned %s.%s.%s to %s Bronze columns", catalog, schema, table, len(expressions))
    return df.select(*expressions)
