from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from db.ddl_init import table_queries
from generators.config import database_init, schema_init
import io
import uuid

RAW_BUCKET = "ampere-prod-raw"
minio_conn = "minio_conn"
mssql_conn = "mssql_conn"
TABLE_NAMES = list(table_queries.keys())


@task(task_id="get_source_table")
def export_table(database_init: str, schema_init: str, table_name: str, **context):
    # Import inside the task to speed up DAG parsing
    from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    import pandas as pd

    # Read from SQL Server
    mssql = MsSqlHook(mssql_conn_id=mssql_conn)
    sql = f"SELECT * FROM [{schema_init}].[{table_name}]"
    df = mssql.get_pandas_df(sql)

    # Convert DataFrame to parquet in-memory
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)  # Requires pyarrow
    buf.seek(0)

    snapshot_type = "full"
    ds = context["logical_date"].date()

    # Build S3 key
    key = (
        f"source/{schema_init}/{table_name}"
        f"/snapshot_type={snapshot_type}"
        f"/load_date={ds}/part-{str(uuid.uuid4())[:8]}.parquet"
    )

    # Upload to MinIO
    s3 = S3Hook(aws_conn_id=minio_conn)
    s3.load_bytes(
        bytes_data=buf.getvalue(),
        key=key,
        bucket_name=RAW_BUCKET,
        replace=True,
    )

    print(f"Uploaded table {table_name} with {len(df)} rows to {key}")


with DAG(
    dag_id="source_to_minio",
    schedule="0 4 * * *",
    start_date=datetime(2025, 8, 16),
    catchup=True,
    max_active_runs=1,
    tags=["source_layer", "s3", "transfer", "prod"],
) as dag:
    prev_task = None
    for tname in TABLE_NAMES:
        t = export_table(database_init, schema_init, tname)
        if prev_task:
            prev_task >> t
        prev_task = t
