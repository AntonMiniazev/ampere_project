# All comments inside code are in English
from airflow import DAG
from airflow.decorators import task
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from datetime import datetime
import pandas as pd
import os
import math
import tempfile
import pendulum

# === USER CONFIG (adjust if needed) ===
MSSQL_CONN_ID = "mssql_conn"  # Airflow connection to MS SQL
MINIO_CONN_ID = "minio_conn"  # Airflow connection of type 'S3' pointing to MinIO
BUCKET = "ampere-prod-raw"
SCHEMA = "test"
TABLE = "order_product"
BASE_PREFIX = f"{SCHEMA}/{TABLE}/snapshot_type=full"  # s3://bucket/test/order_product/snapshot_type=full
CHUNKSIZE = 500_000  # adjust for your memory/cpu
FILE_FORMAT = "parquet"  # 'parquet' or 'csv'
TIMEZONE = "Europe/Belgrade"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
}

with DAG(
    dag_id="export_order_product_full_to_minio",
    description="Full snapshot of test.order_product to MinIO with daily partition (load_date=YYYY-MM-DD)",
    start_date=datetime(2025, 1, 1),
    schedule=None,  # run on demand; you can put '0 3 * * *' for daily
    catchup=False,
    default_args=default_args,
    tags=["export", "mssql", "minio", "full-snapshot"],
) as dag:

    @task
    def compute_prefix():
        # Use project timezone to compute today's date
        today = pendulum.now(TIMEZONE).to_date_string()  # YYYY-MM-DD
        prefix = f"{BASE_PREFIX}/load_date={today}/"
        return {"today": today, "prefix": prefix}

    @task
    def drop_today_partition(prefix_info: dict):
        # Delete today's partition in MinIO if exists
        s3 = S3Hook(aws_conn_id=MINIO_CONN_ID)
        bucket = BUCKET
        prefix = prefix_info["prefix"]

        # List and delete in batches
        keys = s3.list_keys(bucket_name=bucket, prefix=prefix) or []
        if keys:
            # Delete up to 1000 per call; S3Hook has a helper delete_objects
            s3.delete_objects(bucket, keys)

        return prefix_info

    @task
    def export_full(prefix_info: dict):
        """
        Extract entire table from MS SQL in chunks and upload as multiple files to MinIO.
        Writes files as 'part-00000.parquet' (or csv) under today's partition path.
        """
        s3 = S3Hook(aws_conn_id=MINIO_CONN_ID)
        mssql = MsSqlHook(mssql_conn_id=MSSQL_CONN_ID)

        bucket = BUCKET
        prefix = prefix_info["prefix"]

        # Build a simple full-select; if you need column order, specify explicitly
        sql = f"SELECT * FROM {SCHEMA}.{TABLE};"

        # Use a temp directory for chunk files before upload
        total_rows = 0
        part_idx = 0

        # pandas read_sql with chunksize uses server-side cursor; MsSqlHook provides a raw DBAPI conn
        conn = mssql.get_conn()
        try:
            for df in pd.read_sql(sql, conn, chunksize=CHUNKSIZE):
                if df.empty:
                    continue

                # Prepare local file path
                if FILE_FORMAT == "parquet":
                    filename = f"part-{part_idx:05d}.parquet"
                else:
                    filename = f"part-{part_idx:05d}.csv"

                with tempfile.TemporaryDirectory() as tmpdir:
                    local_path = os.path.join(tmpdir, filename)

                    # Write chunk locally
                    if FILE_FORMAT == "parquet":
                        # Use pyarrow if available; fallback to fastparquet if configured
                        df.to_parquet(local_path, index=False)
                    else:
                        # Write as CSV without index; adjust sep/encoding as needed
                        df.to_csv(local_path, index=False)

                    # Upload to MinIO (S3-compatible)
                    key = prefix + filename
                    s3.load_file(
                        filename=local_path,
                        key=key,
                        bucket_name=bucket,
                        replace=True,
                    )

                total_rows += len(df)
                part_idx += 1

        finally:
            conn.close()

        return {
            "written_rows": total_rows,
            "parts": part_idx,
            "target": f"s3://{bucket}/{prefix}",
        }

    @task
    def summarize(result: dict):
        # Log a concise summary to task logs
        print(
            f"Exported {result['written_rows']} rows into {result['parts']} file(s) at {result['target']}"
        )

    # Orchestration
    prefix_info = compute_prefix()
    cleaned = drop_today_partition(prefix_info)
    res = export_full(cleaned)
    summarize(res)
