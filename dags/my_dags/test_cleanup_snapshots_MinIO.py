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

# --- Config ---
# RAW location ampere-prod-raw/source/core/<table_name>/snapshot_type=<snapshot_type>/load_date=YYYY-MM-DD/*.parquet
# Silver location ampere-prod-silver/<dimension or fact>/<table_name>/load_date=YYYY-MM-DD/*.parquet
RAW_BUCKET = "ampere-prod-raw"
SILVER_BUCKET = "ampere-prod-silver"
RAW_PREFIX = "source/core"
SNAPSHOT_TYPE = "full"
MINIO_CONN_ID = "minio_conn"
LOG_BUCKET = "ampere-prod-logs"
LOG_PREFIX = "airflow/cleanup_raw_snapshots"

DEFAULT_DRY_RUN = Variable.get(
    "cleanup_raw_snapshots_dry_run", default_var="true").lower() == "true"

# Project tables
TABLE_NAMES = list(table_queries.keys())

logger = logging.getLogger("airflow.task")


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

    historical = [d for d in all_dates if d < cur_month_start]
    by_month: Dict[tuple[int, int], List[date]] = {}
    for d in historical:
        by_month.setdefault((d.year, d.month), []).append(d)
    for (y, m), ds in by_month.items():
        last_dom = _last_day_of_month(date(y, m, 1))
        keep.add(last_dom if last_dom in ds else max(ds))

    cur_month_dates = [d for d in all_dates if d >= cur_month_start]
    for d in cur_month_dates:
        if d >= cur_week_monday:
            keep.add(d)  # keep current week entirely
        elif d.weekday() == 6:
            keep.add(d)  # keep Sundays of previous weeks
    return keep

# --- S3 helpers ---


def _list_all_data_keys_for_table(s3: S3Hook, table_name: str, bucket: str, dim_or_fact=None) -> List[str]:
    if bucket == "ampere-prod-raw":
        prefix = f"{RAW_PREFIX}/{table_name}/snapshot_type={SNAPSHOT_TYPE}/"
    elif bucket == "ampere-prod-silver":
        prefix = f"{dim_or_fact}/{table_name}/"
    return s3.list_keys(bucket_name=bucket, prefix=prefix) or []


def _list_keys_for_load_date(s3: S3Hook, table_name: str, load_date: date, bucket: str, dim_or_fact=None) -> List[str]:
    if bucket == "ampere-prod-raw":
        prefix = f"{RAW_PREFIX}/{table_name}/snapshot_type={SNAPSHOT_TYPE}/load_date={load_date.isoformat()}/"
    elif bucket == "ampere-prod-silver":
        prefix = f"{dim_or_fact}/{table_name}/load_date={load_date.isoformat()}/"
    return s3.list_keys(bucket_name=bucket, prefix=prefix) or []


def _summary_log_key() -> str:
    ctx = get_current_context()
    dag_id = ctx["dag"].dag_id
    run_id = ctx["run_id"]
    ds = ctx["ds"]
    ti: TaskInstance = ctx["ti"]
    try_number = ti.try_number if isinstance(ti, TaskInstance) else 1
    safe_run_id = run_id.replace(":", "_").replace("+", "_").replace("/", "_")
    return f"{LOG_PREFIX}/dag={dag_id}/run={safe_run_id}/ds={ds}_try={try_number:02d}.json"


# --- DAG ---
with DAG(
    dag_id="cleanup_raw_minio_snapshots",
    schedule="0 3 * * *",
    start_date=datetime(2025, 9, 13, tzinfo=timezone.utc),
    catchup=False,
    tags=["source_layer", "s3", "transfer", "prod"],
) as dag:

    @task
    def list_all_tables(bucket: str, dim_or_fact: str | None = None) -> List[str]:
        # Returns a unique list of table names, not raw keys
        s3 = S3Hook(aws_conn_id=MINIO_CONN_ID)
        if bucket == RAW_BUCKET:
            # keys like: source/core/<table>/snapshot_type=full/...
            keys = s3.list_keys(bucket_name=bucket,
                                prefix=f"{RAW_PREFIX}/") or []
            seen = set()
            for k in keys:
                parts = k.split("/")
                if len(parts) >= 3 and parts[0] == "source" and parts[1] == "core":
                    seen.add(parts[2])
            tables = sorted(seen)
        elif bucket == SILVER_BUCKET:
            layer = _norm_layer(dim_or_fact)
            keys = s3.list_keys(bucket_name=bucket, prefix=f"{layer}/") or []
            # keys like: <layer>/<table>/load_date=YYYY-MM-DD/...
            seen = set()
            for k in keys:
                parts = k.split("/")
                if len(parts) >= 2 and parts[0] == layer:
                    seen.add(parts[1])
            tables = sorted(seen)
        else:
            raise ValueError(f"Unknown bucket: {bucket}")

        logger.info("Discovered tables in %s (%s): %s",
                    bucket, dim_or_fact, tables)
        return tables

    @task
    def compute_deletions(table_name: str, bucket, dim_or_fact, ds: str | None = None, dry_run: bool | None = None) -> dict:
        # Resolve context-derived params
        ctx = get_current_context()
        if ds is None:
            ds = ctx["ds"]
        if dry_run is None:
            dry_run = DEFAULT_DRY_RUN

        s3 = S3Hook(aws_conn_id=MINIO_CONN_ID)

        all_keys = _list_all_data_keys_for_table(
            s3, table_name, bucket, dim_or_fact)
        all_dates = _parse_dates_from_keys(all_keys)
        if not all_dates:
            logger.info("[%s] no dates found", table_name)
            return {"table": table_name, "delete_dates": [], "keys": [], "dry_run": dry_run}

        today = datetime.strptime(ds, "%Y-%m-%d").date()
        keep_dates = _keep_dates_by_rules(all_dates, today)
        delete_dates = sorted(list(all_dates - keep_dates))

        keys_to_delete: List[str] = []
        for d in delete_dates:
            keys_to_delete.extend(_list_keys_for_load_date(
                s3, table_name, d, bucket, dim_or_fact))

        logger.info("[%s] keep=%d delete=%d (keys=%d)", table_name, len(
            keep_dates), len(delete_dates), len(keys_to_delete))
        return {
            "bucket": bucket,
            "table": table_name,
            "delete_dates": [d.isoformat() for d in delete_dates],
            "keys": keys_to_delete,
            "dry_run": dry_run,
        }

    @task
    def apply_deletions(plan: dict) -> dict:
        # Deletes objects; returns per-table summary for final log
        s3 = S3Hook(aws_conn_id=MINIO_CONN_ID)
        table_name: str = plan["table"]
        keys: List[str] = plan["keys"]
        dry_run: bool = plan["dry_run"]
        bucket: str = plan["bucket"]

        if not keys:
            logger.info("[%s] nothing to delete", table_name)
            return {"table": table_name, "deleted_dates": plan["delete_dates"], "deleted_keys_count": 0}

        if dry_run:
            logger.warning(
                "[%s] DRY-RUN: would delete %d objects", table_name, len(keys))
            return {"table": table_name, "deleted_dates": plan["delete_dates"], "deleted_keys_count": 0, "dry_run": True}

        s3.delete_objects(bucket=bucket, keys=keys)
        logger.info("[%s] deleted %d objects", table_name, len(keys))
        return {"bucket": bucket, "table": table_name, "deleted_dates": plan["delete_dates"], "deleted_keys_count": len(keys)}

    @task
    def finalize_run_log(per_table_results: List[Dict[str, Any]], ds: str | None = None) -> str:

        if not isinstance(per_table_results, list):
            try:
                # forces XCom pull for mapped results
                per_table_results = list(per_table_results)
            except TypeError:
                # Fallback: wrap single value
                per_table_results = [per_table_results]

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

        # Ensure all items are JSON-serializable (dicts with only primitives)
        safe_results: List[Dict[str, Any]] = []
        for item in per_table_results:
            # item is expected to be a dict from apply_deletions()
            safe_item = {
                "bucket": item.get("bucket"),
                "table": item.get("table"),
                "deleted_dates": item.get("deleted_dates", []),
                "deleted_keys_count": int(item.get("deleted_keys_count", 0)),
            }
            if item.get("dry_run"):
                safe_item["dry_run"] = True
            safe_results.append(safe_item)

        payload = {
            "meta": meta,
            "results": safe_results,
        }

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

    # 1) Extract table lists
    raw_tables = list_all_tables.override(task_id="list_raw")(
        bucket=RAW_BUCKET)
    silver_tables_dim = list_all_tables.override(task_id="list_silver_dimensions")(
        bucket=SILVER_BUCKET, dim_or_fact="dimension")
    silver_tables_fact = list_all_tables.override(task_id="list_silver_facts")(
        bucket=SILVER_BUCKET, dim_or_fact="fact")

    # 2) For each list compute deletion
    raw_plans = compute_deletions.partial(
        bucket=RAW_BUCKET,    dim_or_fact=None).expand(table_name=raw_tables)
    dims_plans = compute_deletions.partial(
        bucket=SILVER_BUCKET, dim_or_fact="dimension").expand(table_name=silver_tables_dim)
    facts_plans = compute_deletions.partial(
        bucket=SILVER_BUCKET, dim_or_fact="fact").expand(table_name=silver_tables_fact)

    # 3) Apply deletions for each stream
    raw_results = apply_deletions.expand(plan=raw_plans)
    facts_results = apply_deletions.expand(plan=facts_plans)
    dims_results = apply_deletions.expand(plan=dims_plans)

    # 4) Summary log
    summary_uri = finalize_run_log([raw_results, facts_results, dims_results])
