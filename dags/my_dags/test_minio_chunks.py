# All comments inside code are in English
from airflow import DAG
from airflow.decorators import task
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
import pandas as pd
import os
import tempfile
import pendulum

# === CONFIG ===
MSSQL_CONN_ID = "mssql_conn"  # Airflow connection to MS SQL
MINIO_CONN_ID = "minio_conn"  # Airflow connection of type S3 (MinIO endpoint)
BUCKET = "ampere-prod-raw"
DB = "Source"
SCHEMA = "test"
TABLE = "order_product"
ID_COLUMN = (
    "order_id"  # IMPORTANT: integer PK/identity column for deterministic chunking
)
CHUNK_SIZE = 200_000  # ~200k rows per chunk
FILE_FORMAT = "parquet"  # 'parquet' or 'csv'
TIMEZONE = "Europe/Belgrade"

default_args = {"owner": "airflow", "retries": 0}

with DAG(
    dag_id="export_order_product_full_parallel",
    description="Parallel full snapshot of test.order_product to MinIO with daily partition",
    start_date=datetime(2025, 8, 1),
    schedule=None,  # set to cron if needed
    catchup=False,
    default_args=default_args,
    tags=["export", "mssql", "minio", "full-snapshot", "parallel"],
) as dag:

    @task
    def compute_prefix():
        # Build today's S3 prefix: s3://BUCKET/test/order_product/snapshot_type=full/load_date=YYYY-MM-DD/
        today = pendulum.now(TIMEZONE).to_date_string()
        base_prefix = f"{SCHEMA}/{TABLE}/snapshot_type=full"
        prefix = f"{base_prefix}/load_date={today}/"
        return {"today": today, "prefix": prefix, "base_prefix": base_prefix}

    @task
    def drop_today_partition(prefix_info: dict):
        # Delete today's partition in MinIO to make the run idempotent
        s3 = S3Hook(aws_conn_id=MINIO_CONN_ID)
        bucket = BUCKET
        prefix = prefix_info["prefix"]

        keys = s3.list_keys(bucket_name=bucket, prefix=prefix) or []
        if keys:
            s3.delete_objects(bucket, keys)
        return prefix_info

    @task
    def get_id_bounds() -> dict:
        """
        Fetch MIN(ID) and MAX(ID) excluding NULLs.
        Returns {"empty": True, ...} when no valid rows exist to keep the DAG green.
        """
        mssql = MsSqlHook(mssql_conn_id=MSSQL_CONN_ID)
        sql = (
            f"SELECT "
            f"MIN(CAST([{ID_COLUMN}] AS BIGINT)) AS min_id, "
            f"MAX(CAST([{ID_COLUMN}] AS BIGINT)) AS max_id "
            f"FROM [{DB}].[{SCHEMA}].[{TABLE}] "
            f"WHERE [{ID_COLUMN}] IS NOT NULL;"
        )
        df = mssql.get_pandas_df(sql)

        if df.empty or pd.isna(df.loc[0, "min_id"]) or pd.isna(df.loc[0, "max_id"]):
            return {"empty": True, "min_id": None, "max_id": None}

        min_id = int(df.loc[0, "min_id"])
        max_id = int(df.loc[0, "max_id"])
        return {"empty": False, "min_id": min_id, "max_id": max_id}

    @task
    def build_chunks(bounds: dict) -> list[dict]:
        """
        Build list of chunk descriptors like:
        {"start_id": X, "end_id": Y, "idx": i}
        Returns [] if no data -> dynamic mapping emits zero tasks.
        """
        if bounds.get("empty"):
            return []

        start = bounds["min_id"]
        end = bounds["max_id"]

        chunks: list[dict] = []
        idx = 0
        lo = start
        while lo <= end:
            hi = min(lo + CHUNK_SIZE - 1, end)
            chunks.append({"start_id": lo, "end_id": hi, "idx": idx})
            idx += 1
            lo = hi + 1
        return chunks

    @task
    def export_chunk(prefix_info: dict, chunk: dict) -> dict | None:
        """
        Export a single ID range [start_id, end_id] to one file in MinIO.
        Returns None when the range yields zero rows (e.g., gaps in identity values).
        """
        s3 = S3Hook(aws_conn_id=MINIO_CONN_ID)
        mssql = MsSqlHook(mssql_conn_id=MSSQL_CONN_ID)

        bucket = BUCKET
        prefix = prefix_info["prefix"]
        start_id, end_id, idx = chunk["start_id"], chunk["end_id"], chunk["idx"]

        # Deterministic filename per chunk (easy to debug and deduplicate)
        ext = "parquet" if FILE_FORMAT == "parquet" else "csv"
        filename = f"part-{idx:05d}_{start_id}-{end_id}.{ext}"

        # Read the chunk deterministically by ID range (ensure ID is NOT NULL)
        # NOTE: Ensure ID_COLUMN is indexed for performance
        sql = (
            f"SELECT * FROM [{DB}].[{SCHEMA}].[{TABLE}] "
            f"WHERE [{ID_COLUMN}] IS NOT NULL AND [{ID_COLUMN}] BETWEEN {start_id} AND {end_id} "
            f"ORDER BY [{ID_COLUMN}] ASC"
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            local_path = os.path.join(tmpdir, filename)

            # Fetch data
            df = mssql.get_pandas_df(sql)

            # Skip upload if no rows in this slice (sparse ranges are fine)
            if df.empty:
                return None

            # Write locally
            if FILE_FORMAT == "parquet":
                df.to_parquet(local_path, index=False)
            else:
                df.to_csv(local_path, index=False)

            # Upload to MinIO
            key = prefix + filename
            s3.load_file(filename=local_path, key=key, bucket_name=bucket, replace=True)

        return {"rows": int(len(df)), "file": filename, "key": key}

    @task
    def summarize(results: list[dict] | None, prefix_info: dict):
        # Summarize safely even when no chunks produced files
        safe = [r for r in (results or []) if r]
        total = sum(int(r.get("rows", 0)) for r in safe)
        parts = len(safe)

        if parts == 0:
            print(
                f"No data exported to s3://{BUCKET}/{prefix_info['prefix']} (empty table or gaps only)."
            )
        else:
            print(
                f"Exported {total} rows into {parts} file(s) at s3://{BUCKET}/{prefix_info['prefix']}"
            )

    # === Orchestration ===
    prefix = compute_prefix()
    cleaned = drop_today_partition(prefix)
    bounds = get_id_bounds()
    chunks = build_chunks(bounds)

    # Map chunk exports in parallel (Airflow dynamic task mapping). If chunks == [], nothing runs.
    mapped_results = export_chunk.partial(prefix_info=cleaned).expand(chunk=chunks)

    summarize(mapped_results, cleaned)
