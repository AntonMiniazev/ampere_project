import argparse
import logging
import os
import sys
from datetime import date, datetime
from typing import Optional

from pyspark.sql import SparkSession

APP_NAME = "source-to-raw-etl"


def setup_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=level,
        format=("%(asctime)s | %(levelname)s | %(name)s | %(message)s"),
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout),
        ],
        force=True,
    )


def _get_env(name: str, default: Optional[str] = None) -> Optional[str]:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return value


def _parse_date(value: str) -> str:
    datetime.strptime(value, "%Y-%m-%d")
    return value


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract source tables to MinIO-backed parquet."
    )
    parser.add_argument("--table", required=True, help="Source table name")
    parser.add_argument("--schema", default="source", help="Source schema name")
    parser.add_argument("--run-date", type=_parse_date, help="YYYY-MM-DD")
    parser.add_argument("--bucket", default="ampere-raw", help="MinIO bucket name")
    parser.add_argument(
        "--output-prefix", default="source", help="Output prefix in the bucket"
    )
    parser.add_argument("--app-name", default=APP_NAME, help="Spark app name")
    return parser.parse_args()


def _configure_s3(
    spark: SparkSession,
    endpoint: str,
    access_key: str,
    secret_key: str,
) -> None:
    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.endpoint", endpoint)
    hadoop_conf.set("fs.s3a.access.key", access_key)
    hadoop_conf.set("fs.s3a.secret.key", secret_key)
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set(
        "fs.s3a.connection.ssl.enabled",
        "true" if endpoint.startswith("https://") else "false",
    )
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set(
        "fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    )


def main() -> None:
    setup_logging()
    logger = logging.getLogger(APP_NAME)

    args = _parse_args()
    run_date = (
        args.run_date
        or _get_env("RUN_DATE")
        or date.today().isoformat()
    )

    pg_host = _get_env("PGHOST", "postgres-service")
    pg_port = _get_env("PGPORT", "5432")
    pg_db = _get_env("PGDATABASE", "ampere_db")
    pg_user = _get_env("PGUSER")
    pg_password = _get_env("PGPASSWORD")
    if not pg_user or not pg_password:
        raise ValueError("Missing PGUSER/PGPASSWORD for source database access.")

    minio_endpoint = _get_env(
        "MINIO_S3_ENDPOINT", "http://minio.ampere.svc.cluster.local:9000"
    )
    minio_access_key = _get_env("MINIO_ACCESS_KEY")
    minio_secret_key = _get_env("MINIO_SECRET_KEY")
    if not minio_access_key or not minio_secret_key:
        raise ValueError("Missing MINIO_ACCESS_KEY/MINIO_SECRET_KEY for MinIO.")

    logger.info(
        "Starting ETL for %s.%s (run_date=%s)",
        args.schema,
        args.table,
        run_date,
    )
    logger.info(
        "Postgres target host=%s port=%s db=%s user=%s",
        pg_host,
        pg_port,
        pg_db,
        pg_user,
    )
    logger.info("MinIO endpoint=%s bucket=%s", minio_endpoint, args.bucket)

    spark = SparkSession.builder.appName(args.app_name).getOrCreate()
    _configure_s3(spark, minio_endpoint, minio_access_key, minio_secret_key)

    jdbc_url = f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_db}"
    source_table = f"{args.schema}.{args.table}"

    logger.info("Reading %s via JDBC", source_table)
    df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", source_table)
        .option("user", pg_user)
        .option("password", pg_password)
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    output_path = (
        f"s3a://{args.bucket}/"
        f"{args.output_prefix}/{args.schema}/{args.table}/"
        f"load_date={run_date}/"
    )
    logger.info("Writing parquet to %s", output_path)
    df.write.mode("overwrite").parquet(output_path)
    logger.info("ETL completed for %s", source_table)

    spark.stop()


if __name__ == "__main__":
    main()
