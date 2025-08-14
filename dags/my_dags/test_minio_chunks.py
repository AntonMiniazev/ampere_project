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

# Optional: use pyarrow stream writer to keep memory stable
import pyarrow as pa
import pyarrow.parquet as pq

# === CONFIG ===
MSSQL_CONN_ID = "mssql_conn"  # Airflow connection to MS SQL
MINIO_CONN_ID = "minio_conn"  # Airflow connection of type S3 (MinIO/compatible)
BUCKET = "ampere-prod-raw"
DB = "Source"
SCHEMA = "test"
TABLE = "order_product_small"  # your 1M-row test table
ID_COLUMN = "order_id"  # integer PK/identity, monotonically increasing

# Tune these if needed
CHUNK_SIZE = 20_000  # target rows per ID-slice (kept; streaming reduces memory spikes)
STREAM_FETCHMANY = 5_000  # rows per fetch from DB cursor
FILE_FORMAT = "parquet"  # 'parquet' or 'csv' (parquet preferred)
TIMEZONE = "Europe/Belgrade"

# Parquet writer options (pyarrow)
PARQUET_COMPRESSION = "snappy"
# Leave row_group_size <= STREAM_FETCHMANY to avoid large in-memory batches
PARQUET_ROW_GROUP_SIZE = 5_000

# Column selection:
# - If COLUMNS is not None, only those columns will be exported (in given order).
# - If COLUMNS is None and AUTO_EXCLUDE_WIDE=True, we auto-detect and exclude MAX/wide types.
COLUMNS: list[str] | None = None
AUTO_EXCLUDE_WIDE = True  # exclude varchar(max), nvarchar(max), varbinary(max), image, text/ntext, xml, geography, geometry

default_args = {"owner": "airflow", "retries": 0}

with DAG(
    dag_id="export_order_product_full_parallel",
    description="Parallel full snapshot to MinIO with daily partition (streaming, low-memory).",
    start_date=datetime(2025, 8, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["export", "mssql", "minio", "full-snapshot", "parallel", "streaming"],
) as dag:

    @task
    def compute_prefix() -> str:
        # Build today's S3 prefix: s3://BUCKET/<schema>/<table>/snapshot_type=full/load_date=YYYY-MM-DD/
        today = pendulum.now(TIMEZONE).to_date_string()
        base_prefix = f"{SCHEMA}/{TABLE}/snapshot_type=full"
        prefix = f"{base_prefix}/load_date={today}/"
        return prefix  # keep XCom small

    @task
    def drop_today_partition(prefix: str) -> str:
        # Delete today's partition in MinIO to make the run idempotent
        s3 = S3Hook(aws_conn_id=MINIO_CONN_ID)
        keys = s3.list_keys(bucket_name=BUCKET, prefix=prefix) or []
        if keys:
            s3.delete_objects(BUCKET, keys)
        return prefix

    @task
    def discover_select_list() -> str:
        """
        Build a comma-separated SELECT list to avoid pulling huge MAX/wide columns.
        Returns a CSV of bracket-quoted column names, or '*' if we should select all.
        We return a string to avoid API-server XCom validation issues.
        """
        if COLUMNS is not None and len(COLUMNS) > 0:
            return ",".join(f"[{c}]" for c in COLUMNS)

        if not AUTO_EXCLUDE_WIDE:
            return "*"

        mssql = MsSqlHook(mssql_conn_id=MSSQL_CONN_ID)
        sql = f"""
        SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
        FROM [{DB}].INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
        ORDER BY ORDINAL_POSITION;
        """
        df = mssql.get_pandas_df(sql, parameters=(SCHEMA, TABLE))

        if df.empty:
            # Fallback to star if metadata not available for some reason
            return "*"

        # Exclude very wide / LOB-like types which cause huge memory use
        exclude_types = {"image", "text", "ntext", "xml", "geography", "geometry"}
        safe_cols = []
        for _, row in df.iterrows():
            col = row["COLUMN_NAME"]
            dt = str(row["DATA_TYPE"]).lower()
            maxlen = row["CHARACTER_MAXIMUM_LENGTH"]
            # Exclude var* and nvarchar with MAX (-1), and varbinary(MAX)
            if dt in exclude_types:
                continue
            if dt in {"varchar", "nvarchar", "varbinary"} and (
                maxlen is None or int(maxlen) < 0
            ):
                continue
            safe_cols.append(f"[{col}]")

        # If everything got excluded (unlikely), fallback to star
        return ",".join(safe_cols) if safe_cols else "*"

    @task
    def get_id_bounds() -> str:
        """
        Fetch MIN(ID) and MAX(ID) excluding NULLs.
        Return:
          - "EMPTY" if no usable rows
          - "min,max" otherwise
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
            return "EMPTY"

        min_id = int(df.loc[0, "min_id"])
        max_id = int(df.loc[0, "max_id"])
        return f"{min_id},{max_id}"

    @task
    def build_chunks(bounds_str: str) -> list[dict]:
        """
        Create chunk descriptors: {"start_id": X, "end_id": Y, "idx": i}
        When bounds_str == 'EMPTY' -> return [] (no mapped tasks).
        """
        if bounds_str == "EMPTY":
            return []

        try:
            min_s, max_s = bounds_str.split(",", 1)
            start = int(min_s)
            end = int(max_s)
        except Exception as e:
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

    @task(retries=1, retry_delay=pendulum.duration(minutes=2))
    def export_chunk(prefix: str, select_list_csv: str, chunk: dict) -> str | None:
        """
        Stream a single ID range [start_id, end_id] to Parquet/CSV in MinIO.

        Memory-safe approach:
        - Use a DB cursor and fetchmany() in small batches (STREAM_FETCHMANY)
        - Write to Parquet incrementally with ParquetWriter (pyarrow)
        - Avoid keeping the entire chunk in RAM
        """
        s3 = S3Hook(aws_conn_id=MINIO_CONN_ID)
        mssql = MsSqlHook(mssql_conn_id=MSSQL_CONN_ID)

        start_id, end_id, idx = chunk["start_id"], chunk["end_id"], chunk["idx"]
        ext = "parquet" if FILE_FORMAT == "parquet" else "csv"
        filename = f"part-{idx:05d}_{start_id}-{end_id}.{ext}"
        key = prefix + filename

        # Build SELECT list
        select_clause = (
            select_list_csv if select_list_csv and select_list_csv.strip() else "*"
        )

        # Compose SQL (server will stream rows; ORDER BY ensures deterministic order)
        sql = (
            f"SET NOCOUNT ON; "
            f"SELECT {select_clause} "
            f"FROM [{DB}].[{SCHEMA}].[{TABLE}] "
            f"WHERE [{ID_COLUMN}] IS NOT NULL AND [{ID_COLUMN}] BETWEEN {start_id} AND {end_id} "
            f"ORDER BY [{ID_COLUMN}] ASC"
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            local_path = os.path.join(tmpdir, filename)

            conn = mssql.get_conn()
            try:
                # Prepare cursor and fetch in small batches
                cursor = conn.cursor()
                cursor.arraysize = STREAM_FETCHMANY
                cursor.execute(sql)

                # Get column names from cursor description
                colnames = [d[0] for d in cursor.description]

                # If no rows at all in this slice, skip
                first_batch = cursor.fetchmany(STREAM_FETCHMANY)
                if not first_batch:
                    cursor.close()
                    return None

                if FILE_FORMAT == "csv":
                    # Stream to CSV line by line to keep memory low
                    # Note: This path is slower but memory-frugal
                    import csv

                    with open(local_path, "w", newline="", encoding="utf-8") as f:
                        writer = csv.writer(f)
                        writer.writerow(colnames)
                        writer.writerows(first_batch)
                        while True:
                            rows = cursor.fetchmany(STREAM_FETCHMANY)
                            if not rows:
                                break
                            writer.writerows(rows)

                else:
                    # Stream to Parquet using pyarrow ParquetWriter
                    # Convert batches to pyarrow Table to avoid large pandas DataFrames
                    def batch_to_table(rows):
                        # Convert rows (list of tuples) to a pyarrow Table without pandas overhead
                        arrays = list(zip(*rows))
                        if not arrays:
                            # No rows
                            return None
                        pa_arrays = [pa.array(col) for col in arrays]
                        return pa.Table.from_arrays(pa_arrays, names=colnames)

                    # First batch -> create writer with schema
                    table = batch_to_table(first_batch)
                    if table is None or table.num_rows == 0:
                        cursor.close()
                        return None

                    writer = pq.ParquetWriter(
                        local_path, table.schema, compression=PARQUET_COMPRESSION
                    )
                    try:
                        # Write first batch (optionally split into row groups)
                        # We slice into row_group_size chunks to keep page sizes small
                        offset = 0
                        while offset < table.num_rows:
                            end = min(offset + PARQUET_ROW_GROUP_SIZE, table.num_rows)
                            writer.write_table(table.slice(offset, end - offset))
                            offset = end

                        # Subsequent batches
                        while True:
                            rows = cursor.fetchmany(STREAM_FETCHMANY)
                            if not rows:
                                break
                            table = batch_to_table(rows)
                            if table is None or table.num_rows == 0:
                                break
                            offset = 0
                            while offset < table.num_rows:
                                end = min(
                                    offset + PARQUET_ROW_GROUP_SIZE, table.num_rows
                                )
                                writer.write_table(table.slice(offset, end - offset))
                                offset = end
                    finally:
                        writer.close()

                cursor.close()

                # Upload to MinIO
                s3.load_file(
                    filename=local_path, key=key, bucket_name=BUCKET, replace=True
                )

            finally:
                try:
                    conn.close()
                except Exception:
                    pass

        # Return compact result string to keep XCom tiny
        return f"rows~{start_id}-{end_id};key={key}"

    @task
    def summarize(results: list[str] | None, prefix: str) -> None:
        """
        Summarize export. Accept compact strings to avoid XCom schema issues.
        """
        safe = [r for r in (results or []) if r]
        parts = len(safe)

        if not safe:
            print(
                f"No data exported to s3://{BUCKET}/{prefix} (empty ranges or gaps only)."
            )
            return

        # We do not have per-chunk row counts in streaming path without scanning again.
        # Log file keys for quick checks:
        for r in safe[:5]:
            print(f"Sample result: {r}")
        if parts > 5:
            print(f"... and {parts - 5} more file(s).")

    # === Orchestration ===
    prefix = compute_prefix()
    cleaned_prefix = drop_today_partition(prefix)
    select_list_csv = discover_select_list()
    bounds_str = get_id_bounds()
    chunks = build_chunks(bounds_str)

    # Dynamic task mapping; if chunks == [], nothing is scheduled
    mapped_results = export_chunk.partial(
        prefix=cleaned_prefix, select_list_csv=select_list_csv
    ).expand(chunk=chunks)

    summarize(mapped_results, cleaned_prefix)
