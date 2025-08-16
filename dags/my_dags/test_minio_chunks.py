from airflow import DAG
from airflow.decorators import task
from airflow.providers.odbc.hooks.odbc import OdbcHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
import polars as pl
import os
import tempfile
import pendulum
import time
from sqlalchemy.engine import Connection
from contextlib import closing
from db.db_io import exec_sql

# v3
# === CONFIG ===
MSSQL_CONN_ID = "mssql_odbc_conn"  # Airflow connection to MS SQL
MINIO_CONN_ID = "minio_conn"  # Airflow connection of type S3 (MinIO endpoint)
BUCKET = "ampere-prod-raw"
DB = "Source"
SCHEMA = "test"
TABLE = "order_product_small"
ID_COLUMN = "order_id"  # Integer PK/identity column for deterministic chunking
CHUNK_SIZE = 10000  # ~10k orders per chunk
FILE_FORMAT = "parquet"  # 'parquet' or 'csv'
TIMEZONE = "Europe/Belgrade"

default_args = {"owner": "airflow", "retries": 0}


def get_odbc_conn():
    # Rely on ODBC connection + Extra; no driver/connect_kwargs here
    hook = OdbcHook(
        odbc_conn_id=MSSQL_CONN_ID,
        driver="ODBC Driver 18 for SQL Server",
        connect_kwargs={"Encrypt": "no", "TrustServerCertificate": "yes"},
    )
    return hook.get_conn()


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
    def compute_prefix() -> str:
        # Build today's S3 prefix: s3://BUCKET/test/order_product/snapshot_type=full/load_date=YYYY-MM-DD/
        today = pendulum.now(TIMEZONE).to_date_string()
        base_prefix = f"{SCHEMA}/{TABLE}/snapshot_type=full"
        prefix = f"{base_prefix}/load_date={today}/"
        # Return compact string to avoid XCom validation issues
        return prefix

    @task
    def drop_today_partition(prefix: str) -> str:
        # Delete today's partition in MinIO to make the run idempotent
        s3 = S3Hook(aws_conn_id=MINIO_CONN_ID)
        keys = s3.list_keys(bucket_name=BUCKET, prefix=prefix) or []
        if keys:
            s3.delete_objects(BUCKET, keys)
        return prefix

    @task
    def get_id_bounds() -> str:
        """
        Fetch MIN(ID) and MAX(ID) excluding NULLs.
        Return a compact string:
          - "EMPTY" if no usable rows
          - "min,max" otherwise
        """

        sql = (
            f"SELECT "
            f"MIN(CAST([{ID_COLUMN}] AS BIGINT)) AS min_id, "
            f"MAX(CAST([{ID_COLUMN}] AS BIGINT)) AS max_id "
            f"FROM [{DB}].[{SCHEMA}].[{TABLE}] "
            f"WHERE [{ID_COLUMN}] IS NOT NULL;"
        )
        with closing(get_odbc_conn()) as conn:
            df = pl.read_database(query=sql, connection=conn)

        if (
            df.is_empty()
            or df.select("min_id").head(1).item() == None
            or df.select("max_id").head(1).item() == None
        ):
            return "EMPTY"

        min_id = int(df.select("min_id").head(1).item())
        max_id = int(df.select("max_id").head(1).item())
        return f"{min_id},{max_id}"

    @task
    def build_chunks(bounds_str: str) -> list[dict]:
        """
        Create chunk descriptors: {"start_id": X, "end_id": Y, "idx": i}
        When bounds_str == 'EMPTY' -> return [] (no mapped tasks).
        """
        if bounds_str == "EMPTY":
            return []

        # Parse "min,max"
        try:
            min_s, max_s = bounds_str.split(",", 1)
            start = int(min_s)
            end = int(max_s)
        except Exception as e:
            # Fail fast with clear reason
            raise ValueError(f"Invalid bounds string: {bounds_str}") from e

        chunks: list[dict] = []
        idx = 0
        lo = start
        while lo <= end:
            hi = min(lo + CHUNK_SIZE - 1, end)
            chunks.append({"start_id": lo, "end_id": hi, "idx": idx})
            idx += 1
            lo = hi + 1
        return chunks

    @task(
        max_active_tis_per_dag=8,
        retries=2,
        retry_delay=pendulum.duration(minutes=2),
        retry_exponential_backoff=True,
    )
    def export_chunk(prefix: str, chunk: dict) -> str | None:
        start = time.perf_counter()  # check
        """
        Export a single ID range [start_id, end_id] to one file in MinIO.
        Returns a compact result string "rows=<n>;key=<s3_key>" or None when the slice is empty.
        """
        s3 = S3Hook(aws_conn_id=MINIO_CONN_ID)

        point = time.perf_counter() - start  # check
        print(f"Connection in {point:.3f}")

        start_id, end_id, idx = chunk["start_id"], chunk["end_id"], chunk["idx"]

        # Deterministic filename per chunk
        ext = "parquet" if FILE_FORMAT == "parquet" else "csv"
        filename = f"part-{idx:05d}_{start_id}-{end_id}.{ext}"

        # Correct three-part name: [DB].[SCHEMA].[TABLE]
        sql = (
            f"SELECT * FROM [{DB}].[{SCHEMA}].[{TABLE}] "
            f"WHERE [{ID_COLUMN}] IS NOT NULL AND [{ID_COLUMN}] BETWEEN {start_id} AND {end_id} "
            f"ORDER BY [{ID_COLUMN}] ASC"
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            start = time.perf_counter()  # check

            local_path = os.path.join(tmpdir, filename)

            df = exec_sql(sql)
            df = pl.from_pandas(df)

            point = time.perf_counter() - start  # check
            print(f"Extracted from SQL Server in {point:.3f}")

            if df.is_empty():
                return None

            start = time.perf_counter()  # check
            if FILE_FORMAT == "parquet":
                df.write_parquet(local_path)
            else:
                df.write_csv(local_path)

            point = time.perf_counter() - start  # check
            print(f"Local write in {point:.3f}")

            start = time.perf_counter()  # check
            key = prefix + filename
            s3.load_file(filename=local_path, key=key, bucket_name=BUCKET, replace=True)

            point = time.perf_counter() - start  # check
            print(f"Loaded to MinIO in {point:.3f}")

        return f"rows={len(df)};key={key}"

    @task
    def summarize(results: list[str] | None, prefix: str) -> None:
        """
        Summarize export. Accept compact strings to avoid XCom schema issues.
        """
        safe = [r for r in (results or []) if r]
        total_rows = 0
        for r in safe:
            # Parse "rows=<n>;key=<...>"
            try:
                parts = dict(p.split("=", 1) for p in r.split(";"))
                total_rows += int(parts.get("rows", "0"))
            except Exception:
                # Ignore malformed entries; keep run going
                pass

        if not safe:
            print(
                f"No data exported to s3://{BUCKET}/{prefix} (empty table or gaps only)."
            )
        else:
            print(
                f"Exported {total_rows} rows into {len(safe)} file(s) at s3://{BUCKET}/{prefix}"
            )

    # === Orchestration ===
    prefix = compute_prefix()
    cleaned_prefix = drop_today_partition(prefix)
    bounds_str = get_id_bounds()
    chunks = build_chunks(bounds_str)

    # Dynamic task mapping; if chunks == [], nothing is scheduled
    mapped_results = export_chunk.partial(prefix=cleaned_prefix).expand(chunk=chunks)

    summarize(mapped_results, cleaned_prefix)
