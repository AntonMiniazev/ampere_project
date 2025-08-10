from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from db.ddl_init import table_queries  # your file with table definitions
from generators.config import database_init, schema_init
import io
import uuid


RAW_BUCKET = "ampere-prod-raw"
minio_conn = "minio_conn"
mssql_conn = "mssql_conn"
TABLE_NAMES = list(table_queries.keys())
CHUNK_SIZE = 3  # process 3 tables at a time


def chunk(lst, size):
    # Simple chunking helper
    for i in range(0, len(lst), size):
        yield lst[i : i + size]


@task(task_id="get_source_table")
def export_table(database_init: str, schema_init: str, table_name: str, **context):
    from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    import pandas as pd

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


@task(task_id="merge_results")
def merge_results(results_lists: list[list[dict]]):
    # Flatten list-of-lists of dicts into a single list
    flat = []
    for sub in results_lists:
        flat.extend(sub)
    return flat


@task(task_id="summarize")
def summarize(results: list[dict]):
    total_rows = sum(int(r.get("rows", 0)) for r in results)
    tables = ", ".join(r.get("table", "?") for r in results)
    print(f"Exported rows total: {total_rows}; tables: {tables}")


with DAG(
    dag_id="source_to_minio",
    schedule="0 4 * * *",
    start_date=datetime(2025, 8, 10),
    catchup=True,
    max_active_runs=1,
    tags=["source_layer", "s3", "transfer", "prod"],
) as dag:
    chunk_results = []
    prev_group = None

    for idx, names in enumerate(chunk(TABLE_NAMES, CHUNK_SIZE), start=1):
        group = export_table.partial(
            database_init=database_init,
            schema_init=schema_init,
        ).expand(table_name=names)

        if prev_group:
            prev_group >> group

        chunk_results.append(group)
        prev_group = group

    merged = merge_results(chunk_results)
    summarize(merged)
