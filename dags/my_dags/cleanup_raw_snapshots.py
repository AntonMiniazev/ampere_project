# dags/cleanup_raw_snapshots.py
from __future__ import annotations
from db.ddl_init import table_queries

from datetime import datetime, timedelta, timezone, date
from typing import List, Set, Dict, Any
import re
import json
import logging

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable, TaskInstance
from airflow.operators.python import get_current_context
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# --- Config (raw) ---
RAW_BUCKET = "ampere-prod-raw"
RAW_BASE_PREFIX = "source/core"
RAW_SNAPSHOT_TYPE = "full"

# --- Config (silver) ---
SILVER_BUCKET = "ampere-prod-silver"
# Comma-separated layers to scan; supports 'facts', 'dimensions', and 'dims'
SILVER_LAYERS = Variable.get("cleanup_silver_layers", default_var="facts,dimensions,dims")
SILVER_LAYERS = [x.strip() for x in SILVER_LAYERS.split(",") if x.strip()]

# --- Shared config ---
MINIO_CONN_ID = "minio_conn"
LOG_BUCKET = "ampere-prod-logs"
LOG_BASE_PREFIX = "airflow/cleanup_raw_snapshots"
DEFAULT_DRY_RUN = Variable.get("cleanup_raw_snapshots_dry_run", default_var="true").lower() == "true"

# Known raw tables from project
RAW_TABLE_NAMES = list(table_queries.keys())

logger = logging.getLogger("airflow.task")

# ---------- Date helpers ----------
def _parse_dates_from_keys(keys: List[str]) -> Set[date]:
    pat = re.compile(r"load_date=(\d{4}-\d{2}-\d{2})/")
    out: Set[date] = set()
    for k in keys or []:
        m = pat.search(k)
        if m:
            out.add(datetime.strptime(m.group(1), "%Y-%m-%d").date())
    return out

def _last_day_of_month(d: date) -> date:
    first_next = (d.replace(day=1) + timedelta(days=32)).replace(day=1)
    return first_next - timedelta(days=1)

def _month_start(d: date) -> date:
    return d.replace(day=1)

def _monday_of_week(d: date) -> date:
    return d - timedelta(days=d.weekday())

def _keep_dates_by_rules(all_dates: Set[date], today: date) -> Set[date]:
    keep: Set[date] = set()
    cur_month_start = _month_start(today)
    cur_week_monday = _monday_of_week(today)

    # Rule 1: for months before current month, keep only month-end (or max available)
    historical = [d for d in all_dates if d < cur_month_start]
    by_month: Dict[tuple[int, int], List[date]] = {}
    for d in historical:
        by_month.setdefault((d.year, d.month), []).append(d)
    for (y, m), ds in by_month.items():
        last_dom = _last_day_of_month(date(y, m, 1))
        keep.add(last_dom if last_dom in ds else max(ds))

    # Rule 2: current month: keep current week entirely, and Sundays of previous weeks
    cur_month_dates = [d for d in all_dates if d >= cur_month_start]
    for d in cur_month_dates:
        if d >= cur_week_monday:
            keep.add(d)
        elif d.weekday() == 6:  # Sunday
            keep.add(d)
    return keep

# ---------- S3 helpers ----------
def _list_all_keys_for_raw(s3: S3Hook, table_name: str) -> List[str]:
    prefix = f"{RAW_BASE_PREFIX}/{table_name}/snapshot_type={RAW_SNAPSHOT_TYPE}/"
    return s3.list_keys(bucket_name=RAW_BUCKET, prefix=prefix) or []

def _list_keys_for_raw_load_date(s3: S3Hook, table_name: str, d: date) -> List[str]:
    prefix = f"{RAW_BASE_PREFIX}/{table_name}/snapshot_type={RAW_SNAPSHOT_TYPE}/load_date={d.isoformat()}/"
    return s3.list_keys(bucket_name=RAW_BUCKET, prefix=prefix) or []

def _list_all_keys_for_silver(s3: S3Hook, layer: str, table_name: str) -> List[str]:
    prefix = f"{layer}/{table_name}/"
    return s3.list_keys(bucket_name=SILVER_BUCKET, prefix=prefix) or []

def _list_keys_for_silver_load_date(s3: S3Hook, layer: str, table_name: str, d: date) -> List[str]:
    prefix = f"{layer}/{table_name}/load_date={d.isoformat()}/"
    return s3.list_keys(bucket_name=SILVER_BUCKET, prefix=prefix) or []

def _discover_silver_tables(s3: S3Hook, layers: List[str]) -> List[Dict[str, str]]:
    """
    Discover silver tables by listing keys under each layer and extracting <table> folder name.
    Returns list of {"layer": <layer>, "table": <table_name>} without duplicates.
    """
    found: set[tuple[str, str]] = set()
    for layer in layers:
        prefix = f"{layer}/"
        keys = s3.list_keys(bucket_name=SILVER_BUCKET, prefix=prefix) or []
        # Extract "<layer>/<table>/" from keys like "<layer>/<table>/load_date=YYYY-MM-DD/..."
        for k in keys:
            parts = k.split("/")
            if len(parts) >= 2 and parts[0] == layer:
                table = parts[1]
                if table:  # avoid empty strings
                    found.add((layer, table))
    return [{"layer": lyr, "table": tbl} for (lyr, tbl) in sorted(found)]

def _summary_log_key() -> str:
    ctx = get_current_context()
    dag_id = ctx["dag"].dag_id
    run_id = ctx["run_id"]
    ds = ctx["ds"]
    ti: TaskInstance = ctx["ti"]
    try_number = ti.try_number if isinstance(ti, TaskInstance) else 1
    safe_run_id = run_id.replace(":", "_").replace("+", "_").replace("/", "_")
    return f"{LOG_BASE_PREFIX}/dag={dag_id}/run={safe_run_id}/ds={ds}_try={try_number:02d}.json"

# ---------- DAG ----------
with DAG(
    dag_id="cleanup_raw_minio_snapshots",
    schedule="0 3 * * *",  # daily at 03:00 Europe/Belgrade
    start_date=datetime(2025, 9, 13, tzinfo=timezone.utc),
    catchup=False,
    tags=["source_layer", "s3", "transfer", "prod", "silver"],
) as dag:

    @task
    def build_work_items(dry_run: bool | None = None) -> List[Dict[str, Any]]:
        """
        Build a unified list of work items for raw and silver.
        Raw: known table list from project.
        Silver: discovered tables for configured layers.
        """
        if dry_run is None:
            dry_run = DEFAULT_DRY_RUN

        s3 = S3Hook(aws_conn_id=MINIO_CONN_ID)

        # Raw items
        items: List[Dict[str, Any]] = [
            {"kind": "raw", "bucket": RAW_BUCKET, "table": t, "layer": None, "dry_run": dry_run}
            for t in RAW_TABLE_NAMES
        ]

        # Silver items (discover)
        silver_pairs = _discover_silver_tables(s3, SILVER_LAYERS)
        for pair in silver_pairs:
            items.append(
                {"kind": "silver", "bucket": SILVER_BUCKET, "table": pair["table"], "layer": pair["layer"], "dry_run": dry_run}
            )

        logger.info("Built %d work items (raw=%d, silver=%d)",
                    len(items), len(RAW_TABLE_NAMES), len(silver_pairs))
        return items

    @task
    def compute_deletions(item: Dict[str, Any], ds: str | None = None) -> dict:
        """
        Compute delete candidates for a single item (raw or silver).
        Returns dict with 'bucket','kind','layer','table','delete_dates','keys','dry_run'.
        """
        ctx = get_current_context()
        if ds is None:
            ds = ctx["ds"]

        s3 = S3Hook(aws_conn_id=MINIO_CONN_ID)
        kind = item["kind"]
        table = item["table"]
        layer = item.get("layer")
        dry_run = item.get("dry_run", DEFAULT_DRY_RUN)

        # List all keys for the item and collect distinct load_dates
        if kind == "raw":
            all_keys = _list_all_keys_for_raw(s3, table)
        else:
            all_keys = _list_all_keys_for_silver(s3, layer, table)
        all_dates = _parse_dates_from_keys(all_keys)

        if not all_dates:
            logger.info("[%s/%s] no dates found", kind, table)
            return {"bucket": item["bucket"], "kind": kind, "layer": layer, "table": table,
                    "delete_dates": [], "keys": [], "dry_run": dry_run}

        today = datetime.strptime(ds, "%Y-%m-%d").date()
        keep_dates = _keep_dates_by_rules(all_dates, today)
        delete_dates = sorted(list(all_dates - keep_dates))

        # Expand to concrete object keys for each delete date
        keys_to_delete: List[str] = []
        for d in delete_dates:
            if kind == "raw":
                keys_to_delete.extend(_list_keys_for_raw_load_date(s3, table, d))
            else:
                keys_to_delete.extend(_list_keys_for_silver_load_date(s3, layer, table, d))

        logger.info("[%s/%s%s] keep=%d delete=%d (keys=%d)",
                    kind, table, f' ({layer})' if layer else "",
                    len(keep_dates), len(delete_dates), len(keys_to_delete))

        return {
            "bucket": item["bucket"],
            "kind": kind,
            "layer": layer,
            "table": table,
            "delete_dates": [d.isoformat() for d in delete_dates],
            "keys": keys_to_delete,
            "dry_run": dry_run,
        }

    @task
    def apply_deletions(plan: dict) -> dict:
        """
        Delete objects for a single plan (without chunking).
        Returns compact summary for final log.
        """
        s3 = S3Hook(aws_conn_id=MINIO_CONN_ID)
        bucket: str = plan["bucket"]
        table: str = plan["table"]
        kind: str = plan["kind"]
        layer: str | None = plan.get("layer")
        keys: List[str] = plan["keys"]
        dry_run: bool = plan["dry_run"]

        if not keys:
            logger.info("[%s/%s%s] nothing to delete", kind, table, f' ({layer})' if layer else "")
            return {"bucket": bucket, "kind": kind, "layer": layer, "table": table,
                    "deleted_dates": plan["delete_dates"], "deleted_keys_count": 0}

        if dry_run:
            logger.warning("[%s/%s%s] DRY-RUN: would delete %d objects",
                           kind, table, f' ({layer})' if layer else "", len(keys))
            return {"bucket": bucket, "kind": kind, "layer": layer, "table": table,
                    "deleted_dates": plan["delete_dates"], "deleted_keys_count": 0, "dry_run": True}

        # Single call deletion
        s3.delete_objects(bucket=bucket, keys=keys)
        logger.info("[%s/%s%s] deleted %d objects",
                    kind, table, f' ({layer})' if layer else "", len(keys))
        return {"bucket": bucket, "kind": kind, "layer": layer, "table": table,
                "deleted_dates": plan["delete_dates"], "deleted_keys_count": len(keys)}

    @task
    def finalize_run_log(per_item_results: List[Dict[str, Any]] | Any, ds: str | None = None) -> str:
        """
        Write one summary JSON log per DAG run into MinIO with meta and per-item results.
        """
        # Materialize LazyXComSequence if needed
        if not isinstance(per_item_results, list):
            try:
                per_item_results = list(per_item_results)
            except TypeError:
                per_item_results = [per_item_results]

        ctx = get_current_context()
        if ds is None:
            ds = ctx["ds"]

        s3 = S3Hook(aws_conn_id=MINIO_CONN_ID)

        meta = {
            "dag_id": ctx["dag"].dag_id,
            "run_id": ctx["run_id"],
            "ds": ds,
            "try_number": ctx["ti"].try_number,
            "utc_written_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        }

        # Keep only JSON-safe primitives and selected fields
        safe_results: List[Dict[str, Any]] = []
        for item in per_item_results:
            safe_item = {
                "bucket": item.get("bucket"),
                "kind": item.get("kind"),
                "layer": item.get("layer"),
                "table": item.get("table"),
                "deleted_dates": item.get("deleted_dates", []),
                "deleted_keys_count": int(item.get("deleted_keys_count", 0)),
            }
            if item.get("dry_run"):
                safe_item["dry_run"] = True
            safe_results.append(safe_item)

        payload = {"meta": meta, "results": safe_results}

        key = _summary_log_key()
        s3.load_string(
            string_data=json.dumps(payload, ensure_ascii=False, indent=2),
            key=key,
            bucket_name=LOG_BUCKET,
            replace=True,
            encrypt=False,
        )
        uri = f"s3://{LOG_BUCKET}/{key}"
        logger.info("Summary log written to %s", uri)
        return uri

    # Graph: build items -> map compute -> map delete -> finalize once
    items = build_work_items()
    plans = compute_deletions.expand(item=items)
    results = apply_deletions.expand(plan=plans)
    summary_uri = finalize_run_log(results)
