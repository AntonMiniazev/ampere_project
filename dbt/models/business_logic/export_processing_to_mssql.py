# scripts/export_processing_to_mssql.py
import os
import re
from datetime import datetime
from typing import Iterable, Tuple, Dict, Any, List, Optional
from sqlalchemy.dialects import mssql as mssql_types
import fsspec
import pyarrow.dataset as ds
import pyarrow.fs as pafs
import polars as pl
import sqlalchemy as sa
from sqlalchemy.engine import URL
import yaml


def model(dbt, session):
    # Do not set enabled here; control it from dbt_project.yml
    dbt.config(
        tags=["bl_export"],
        materialized="table",  # harmless tiny log table
        alias="mssql_export_log",
    )

    # --- Config via env ---
    CONFIG_PATH = os.getenv(
        "EXPORT_CONFIG_PATH", "/workspaces/ampere_project/dbt/models/business_logic/config/export_map.yaml")
    BUCKET = os.getenv("SILVER_BUCKET", "ampere-prod-silver")
    DIM_PREFIX = "dimension"
    FACT_PREFIX = "fact"

    MSSQL_HOST = os.getenv("MSSQL_HOST", "192.168.1.29")
    MSSQL_PORT = int(os.getenv("MSSQL_PORT", "14330"))
    MSSQL_DB = os.getenv("MSSQL_DB", "Ampere")
    MSSQL_USER = os.getenv("MSSQL_USER", "sa")
    MSSQL_PASS = os.getenv("SA_PASSWORD") or os.getenv("MSSQL_PASSWORD")
    STAGE_SCHEMA = os.getenv("STAGE_SCHEMA", "stg")
    BATCH_SIZE = int(os.getenv("BATCH_SIZE", "200000"))

    MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
    MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
    MINIO_ENDPOINT = os.getenv("MINIO_S3_ENDPOINT", "http://minio.local:9000")

    # -------------------- Helpers: connections & FS --------------------

    def build_engine():
        # Use ODBC Driver 18; disable encryption for local lab
        url = URL.create(
            "mssql+pyodbc",
            username=MSSQL_USER,
            password=MSSQL_PASS,
            host=MSSQL_HOST,
            port=MSSQL_PORT,
            database=MSSQL_DB,
            query={
                "driver": "ODBC Driver 18 for SQL Server",
                "Encrypt": "no",
                "TrustServerCertificate": "yes",
            },
        )
        return sa.create_engine(url, fast_executemany=True)

    def s3_filesystems():
        # fsspec for listing and pyarrow for scanning
        fs = fsspec.filesystem(
            "s3",
            key=MINIO_ACCESS_KEY, secret=MINIO_SECRET_KEY,
            client_kwargs={"endpoint_url": MINIO_ENDPOINT},
            config_kwargs={"signature_version": "s3v4"},
            use_listings_cache=False,
        )
        s3 = pafs.S3FileSystem(
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            endpoint_override=MINIO_ENDPOINT,
            scheme="http" if MINIO_ENDPOINT.startswith("http://") else "https",
        )
        return fs, s3

    # -------------------- MSSQL DDL helpers --------------------

    def ensure_schema(conn, schema_name: str):
        conn.exec_driver_sql(
            f"IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='{schema_name}') "
            f"EXEC('CREATE SCHEMA [{schema_name}]');"
        )

    def truncate_table(conn, schema_name: str, table_name: str):
        conn.exec_driver_sql(
            f"IF OBJECT_ID('{schema_name}.{table_name}','U') IS NOT NULL "
            f"TRUNCATE TABLE [{schema_name}].[{table_name}];"
        )

    def drop_table(conn, schema_name: str, table_name: str):
        conn.exec_driver_sql(
            f"IF OBJECT_ID('{schema_name}.{table_name}','U') IS NOT NULL "
            f"DROP TABLE [{schema_name}].[{table_name}];"
        )

    # -------------------- YAML config --------------------

    def load_export_config(path: str) -> Dict[str, Any]:
        # Load YAML; empty dicts if sections missing
        with open(path, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}
        cfg.setdefault("defaults", {})
        cfg.setdefault("dimensions", {})
        cfg.setdefault("facts", {})
        return cfg

    def enabled_tables(section: Dict[str, Any]) -> List[str]:
        # Return only tables with enabled: true
        out = []
        for name, spec in section.items():
            if isinstance(spec, dict) and spec.get("enabled", False):
                out.append(name)
        return out

    def convert_to_sa_type(dtype: str, item: Dict[str, Any]):
        # Converting dtypes from YAML to SQLAlchemy types
        if dtype == 'DECIMAL':
            p = item.get("precision")
            s = item.get("scale")
            dtype_sa = getattr(mssql_types, dtype.upper())(p, s)
        elif dtype == 'DATETIME2':
            p = item.get("precision")
            dtype_sa = getattr(mssql_types, dtype.upper())(p)
        else:
            dtype_sa = getattr(mssql_types, dtype.upper())
        return dtype_sa

    def table_columns_spec(section: Dict[str, Any], table: str) -> Dict[str, Any]:
        # Return mapping {column_name: type} for a given table
        spec = section.get(table) or {}
        cols = spec.get("columns") or []
        table_config: Dict[str, Any] = {}
        for item in cols:
            if isinstance(item, dict):
                for col, dtype in item.items():
                    if col is None or col == 'precision' or col == 'scale':
                        continue
                    name = str(col).strip()
                    table_config[name] = convert_to_sa_type(dtype, item)
            elif isinstance(item, str):
                # Allow bare column names if ever provided; type unknown
                table_config[item.strip()] = None
        return table_config

    def fact_load_date(section: Dict[str, Any], table: str) -> Optional[str]:
        # "latest" or explicit 'YYYY-MM-DD' or None (=> latest)
        spec = section.get(table, {}) or {}
        val = spec.get("load_date")
        return val

    def latest_table_partition(fs, allowed_tbl: str, tbl_prefix) -> str:
        # list bucket/fact/<fact>/load_date=YYYY-MM-DD/
        base = f"{BUCKET}/{tbl_prefix}/{allowed_tbl}"
        if not fs.exists(base):
            raise RuntimeError(f"No fact folder for {allowed_tbl}")
        pat = re.compile(r"load_date=(\d{4}-\d{2}-\d{2})/?$")
        dates = []
        for p in fs.ls(base, detail=False):
            m = pat.search(p)
            if m:
                try:
                    dates.append(datetime.strptime(
                        m.group(1), "%Y-%m-%d").date())
                except ValueError:
                    pass
        if not dates:
            raise RuntimeError(f"No partitions for {allowed_tbl}")
        return max(dates).isoformat()

    def resolve_table_partition(fs, allowed_tbl: str, cfg_date: Optional[str], tbl_prefix) -> str:
        # if cfg_date is 'latest' or None -> pick latest; else validate date
        if not cfg_date or str(cfg_date).lower() == "latest":
            return latest_table_partition(fs, allowed_tbl, tbl_prefix)
        # validate provided date
        _ = datetime.strptime(cfg_date, "%Y-%m-%d").date()
        # ensure folder exists
        base = f"{BUCKET}/{tbl_prefix}/{allowed_tbl}/load_date={cfg_date}"
        if not fs.exists(base):
            raise RuntimeError(f"Partition not found: {base}")
        return cfg_date

    def list_tables(fs, allow: List[str], cfg: Dict[str, Any], tbl_prefix) -> Iterable[Tuple[str, str]]:
        # yields (table_name, parquet_path for resolved partition)
        for allowed_tbl in allow:
            ld = resolve_table_partition(
                fs, allowed_tbl, fact_load_date(cfg, allowed_tbl), tbl_prefix)
            parquet_path = f"{BUCKET}/{tbl_prefix}/{allowed_tbl}/load_date={ld}/{allowed_tbl}.parquet"
            if fs.exists(parquet_path):
                yield (allowed_tbl, parquet_path)

    # -------------------- Load logic --------------------

    def load_parquet_to_stage(
        s3, table_name: str, parquet_path: str, conn, replace_mode: bool,
        columns_spec: List[Dict[str, str]], batch_size: int
    ):
        # Replace mode: drop table to avoid legacy wrong scales, then append fresh data
        if replace_mode:
            drop_table(conn, STAGE_SCHEMA, table_name)

        dataset = ds.dataset(parquet_path, format="parquet", filesystem=s3)
        scanner = dataset.scanner(batch_size=batch_size)

        replace_or_append = "replace"
        total = 0
        for batch in scanner.to_batches():
            df = pl.from_arrow(batch)
            df.to_pandas().to_sql(name=f"{table_name}", con=conn, schema=f"{STAGE_SCHEMA}",
                                  if_exists=replace_or_append, dtype=columns_spec)
            replace_or_append = "append"
            total += len(df)
        return total

    def main():
        cfg = load_export_config(CONFIG_PATH)

        # Optional override of batch size from YAML defaults
        global BATCH_SIZE
        if "batch_size" in cfg.get("defaults", {}):
            try:
                BATCH_SIZE = int(cfg["defaults"]["batch_size"])
            except Exception:
                pass

        fs, s3 = s3_filesystems()
        engine = build_engine()

        dims_allow = enabled_tables(cfg["dimensions"])
        facts_allow = enabled_tables(cfg["facts"])

        with engine.begin() as conn:
            ensure_schema(conn, STAGE_SCHEMA)

            # Dimensions: full refresh (truncate, keep structure if exists)
            for dim, p in list_tables(fs, dims_allow, cfg["dimensions"], tbl_prefix=DIM_PREFIX):
                print(f"[dim] exporting {dim} from {p}")
                truncate_table(conn, STAGE_SCHEMA, f"{dim}")
                rows = load_parquet_to_stage(
                    s3,
                    table_name=dim,
                    parquet_path=p,
                    conn=conn,
                    replace_mode=False,  # truncate above keeps structure
                    columns_spec=table_columns_spec(cfg["dimensions"], dim),
                    batch_size=BATCH_SIZE,
                )
                print(f"[dim] {dim}: {rows} rows")

            # Facts: latest or specified partition (drop-and-append for safety)
            for fact, p in list_tables(fs, facts_allow, cfg["facts"], tbl_prefix=FACT_PREFIX):
                print(f"[fact] exporting {fact} from {p}")
                rows = load_parquet_to_stage(
                    s3,
                    table_name=fact,
                    parquet_path=p,
                    conn=conn,
                    replace_mode=True,  # drop each run to avoid legacy wrong scales
                    columns_spec=table_columns_spec(cfg["facts"], fact),
                    batch_size=BATCH_SIZE,
                )
                print(f"[fact] {fact}: {rows} rows")

        print("[ok] export done")

    main()

    return pl.DataFrame(
        {"ran_at_utc": [datetime.now().isoformat(timespec="seconds")]}
    ).to_pandas()
