from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from db.ddl_init import table_queries
from generators.config import database_init, schema_init
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import io
import uuid

RAW_BUCKET = "ampere-prod-raw"
minio_conn = "minio_conn"
mssql_conn = "mssql_conn"
TABLE_NAMES = list(table_queries.keys())


@task
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
    df.to_parquet(buf, index=False)
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
     schedule=None,
    start_date=datetime(2025, 8, 1),
    catchup=False,
    max_active_runs=1,
    tags=["prod", "s3", "transfer", "source_layer", "raw_layer"],
) as dag:
    prev_export_task = None

    # The loop creates one export task per table.
    # All tasks form a strict linear chain so that tables are exported sequentially.
    for tname in TABLE_NAMES:
        current_export_task = export_table.override(task_id=f"export_{tname}")(
            database_init,
            schema_init,
            tname
        )

        # Chain current task after the previous one to enforce sequential export
        if prev_export_task:
            prev_export_task >> current_export_task

        prev_export_task = current_export_task

    # After the last table is exported, trigger the next DAG in the pipeline
    trigger_processing = TriggerDagRunOperator(
        task_id="trigger_dbt_processing",
        trigger_dag_id="dbt_processing",
        logical_date="{{ logical_date }}",
    )

    prev_export_task >> trigger_processing
