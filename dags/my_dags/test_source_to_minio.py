# dags/source_to_minio.py
# All comments MUST be in English.
from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.context import get_current_context
from db.ddl_init import table_queries
from generators.config import database_init, schema_init
import pandas as pd
import io
import uuid

RAW_BUCKET = "ampere-prod-raw"
MINIO_CONN_ID = "minio_conn"
MSSQL_CONN_ID = "mssql_conn"


@dag(
    dag_id="source_to_minio",
    schedule="0 4 * * *",
    start_date=datetime(2025, 8, 10),
    catchup=True,
    max_active_runs=1,
    tags=["source_layer", "s3", "transfer", "prod"],
)
def source_to_minio():
    @task
    def export_table(table_name: str):
        # Get runtime context for logical date
        ctx = get_current_context()
        ds = ctx["logical_date"].date()  # YYYY-MM-DD

        # Read from SQL Server
        mssql = MsSqlHook(mssql_conn_id=MSSQL_CONN_ID)
        sql = f"SELECT * FROM [{schema_init}].[{table_name}]"
        df = mssql.get_pandas_df(sql)

        # Convert to parquet in-memory (requires pyarrow)
        buf = io.BytesIO()
        df.to_parquet(buf, index=False)
        buf.seek(0)

        # Build S3 key: s3://bucket/source/<schema>/<table>/snapshot_type=full/load_date=YYYY-MM-DD/part-<uuid>.parquet
        part = str(uuid.uuid4())[:8]
        key = (
            f"source/{schema_init}/{table_name}"
            f"/snapshot_type=full"
            f"/load_date={ds}/part-{part}.parquet"
        )

        # Upload to MinIO
        s3 = S3Hook(aws_conn_id=MINIO_CONN_ID)
        s3.load_bytes(
            bytes_data=buf.getvalue(), key=key, bucket_name=RAW_BUCKET, replace=True
        )

        # Return only small, serializable metadata if you really need XCom
        return {"table": table_name, "rows": int(len(df)), "key": key}

    # Dynamic task mapping over table names (creates N parallel tasks)
    table_names = list(table_queries.keys())
    export_table.expand(table_name=table_names)


source_to_minio()
