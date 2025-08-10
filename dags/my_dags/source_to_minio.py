from datetime import datetime
from airflow import DAG
from airflow.sdk import task
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


@task(task_id="get_source_table")
def get_source_table(database_init: str, schema_init: str, table_name: str, **context):
    # Read from SQL Server
    mssql = MsSqlHook(mssql_conn_id="mssql_default")
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
    return {"table": table_name, "rows": len(df), "key": key}


@task(task_id="upload_to_minio")
def upload_to_minio(table_queries: dict):
    results = []
    for tname in table_queries.keys():
        res = get_source_table(database_init, schema_init, tname)
        results.append(res)
    return results


with DAG(
    dag_id="source_to_minio",
    schedule="0 4 * * *",
    start_date=datetime(2025, 8, 10),
    tags=["source_layer", "s3", "transfer", "prod"],
    catchup=True,
    max_active_runs=1,
) as dag:
    etl_proc = upload_to_minio(table_queries)

    etl_proc
