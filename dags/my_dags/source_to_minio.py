from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from db.ddl_init import table_queries  # your file with table definitions
from generators.config import database_init, schema_init
import pandas as pd
import io
import uuid


RAW_BUCKET = "ampere-prod-raw"
minio_conn = "minio_conn"
mssql_conn = "mssql_conn"
table_names = list(table_queries.keys())


@task(task_id="get_source_table")
def export_table(database_init: str, schema_init: str, table_name: str, **context):
    # Read from SQL Server
    mssql = MsSqlHook(mssql_conn_id=mssql_conn)
    sql = f"SELECT * FROM [{schema_init}].[{table_name}]"
    df = mssql.get_pandas_df(sql)

    # Convert DataFrame to parquet in-memory
    # Note: pandas requires pyarrow in your env
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)

    snapshot_type = "full"
    ds = context["logical_date"].date()

    # Build S3 key
    # s3://bucket/sqlserver/<schema>/<table>/snapshot_type=.../load_date=YYYY-MM-DD/part-<uuid>.parquet
    part = str(uuid.uuid4())[:8]
    key = (
        f"{'source'}/{schema_init}/{table_name}"
        f"/snapshot_type={snapshot_type}"
        f"/load_date={ds}/part-{part}.parquet"
    )

    # Upload to MinIO via S3Hook
    s3 = S3Hook(aws_conn_id=minio_conn)
    s3.load_bytes(
        bytes_data=buf.getvalue(),
        key=key,
        bucket_name=RAW_BUCKET,
        replace=True,
    )
    return {"table": table_name, "rows": int(len(df)), "key": key}


@task
def summarize(results: list[dict]):
    # results is a list of dicts returned by export_table
    total = sum(r["rows"] for r in results)
    print(f"Total rows exported: {total}")


@task(task_id="upload_to_minio")
def upload_to_minio(table_queries: dict):
    for tname in table_queries.keys():
        summarize(export_table.expand(table_name=tname))


with DAG(
    dag_id="source_to_minio",
    schedule="0 4 * * *",
    start_date=datetime(2025, 8, 10),
    catchup=True,
    max_active_runs=1,
    tags=["source_layer", "s3", "transfer", "prod"],
) as dag:
    run_dag = upload_to_minio(table_queries)
    run_dag
