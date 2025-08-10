# dags/source_to_minio_raw.py
from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from db.ddl_init import table_queries  # your file with table definitions
import pandas as pd
import io
import uuid
import os

RAW_BUCKET = "ampere-prod-raw"
SOURCE_NAME = "sqlserver"


@dag(
    schedule="@daily",
    start_date=datetime(2025, 8, 1),
    catchup=False,
    default_args={"owner": "ampere"},
    tags=["raw", "minio", "sqlserver"],
)
def source_to_minio_raw():
    @task
    def export_table(table_name: str, table_meta: dict, schema_init: str, ds: str):
        # Build snapshot_type from ddl_init table "type"
        # dict/init -> full, gen -> inc
        ttype = table_meta.get("type", "gen")
        snapshot_type = "full" if ttype in ("dict", "init") else "inc"

        # Read from SQL Server
        mssql = MsSqlHook(mssql_conn_id="mssql_default")
        # Simple SELECT *; you can replace with column list or WHERE for inc
        sql = f"SELECT * FROM [{schema_init}].[{table_name}]"
        df = mssql.get_pandas_df(sql)

        # Convert DataFrame to parquet in-memory
        # Note: pandas requires pyarrow in your env
        buf = io.BytesIO()
        df.to_parquet(buf, index=False)
        buf.seek(0)

        # Build S3 key
        # s3://bucket/sqlserver/<schema>/<table>/snapshot_type=.../load_date=YYYY-MM-DD/part-<uuid>.parquet
        part = str(uuid.uuid4())[:8]
        key = (
            f"{SOURCE_NAME}/{schema_init}/{table_name}"
            f"/snapshot_type={snapshot_type}"
            f"/load_date={ds}/part-{part}.parquet"
        )

        # Upload to MinIO via S3Hook
        s3 = S3Hook(aws_conn_id="minio_s3")
        s3.load_bytes(
            bytes_data=buf.getvalue(),
            key=key,
            bucket_name=RAW_BUCKET,
            replace=True,
        )
        return {"table": table_name, "rows": len(df), "key": key}

    @task
    def decide_and_run(ds: str):
        # Import here to avoid global import costs
        from generators.config import schema_init  # your schema name

        results = []
        for tname, meta in table_queries.items():
            # You may put routing rules: skip empty; or switch full/inc by weekday
            res = export_table(tname, meta, schema_init, ds)
            results.append(res)
        return results

    decide_and_run()


source_to_minio_raw()
