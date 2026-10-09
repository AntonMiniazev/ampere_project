from __future__ import annotations

import json
import logging
import time
from datetime import datetime
from urllib import error, request
from zoneinfo import ZoneInfo

from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import BranchPythonOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import Variable

from utils.ampere_dag_config import standard_default_args

DAG_ID = "ampere__curie__cache_refresh__post_iceberg_gold"
DEFAULT_CURIE_API_BASE_URL = "http://100.65.42.72"
REFRESH_PATH = "/api/cache/refresh_flightsql"
STATUS_PATH = "/api/cache/status_flightsql"
ADMIN_KEY_HEADER = "X-Curie-Admin-Key"
EXPECTED_TABLES = {
    "curie_marketing_sales_budget_monthly_store",
    "curie_marketing_product_sales_monthly_store",
    "curie_marketing_category_sales_monthly_store",
    "curie_marketing_client_metrics_monthly_store",
    "curie_marketing_active_client_month",
    "curie_financial_performance_monthly_store",
    "curie_financial_product_margin_monthly_store",
    "curie_delivery_courier_performance_monthly_store",
}


def _curie_api_base_url() -> str:
    """Return the configured Curie API base URL without a trailing slash."""
    return (
        Variable.get(
            "curie_api_base_url",
            default=DEFAULT_CURIE_API_BASE_URL,
        )
        .strip()
        .rstrip("/")
    )


def _curie_admin_key() -> str:
    """Return the Curie admin key, failing fast when the Airflow Variable is missing."""
    admin_key = Variable.get("curie_api_admin_key", default=None)
    if not admin_key or not str(admin_key).strip():
        raise ValueError("Airflow Variable curie_api_admin_key is required")
    return str(admin_key).strip()


def _request_json(
    *,
    method: str,
    url: str,
    headers: dict[str, str] | None = None,
    timeout_seconds: int = 30,
) -> tuple[int, dict]:
    """Call a Curie endpoint and decode its JSON response."""
    data = b"" if method.upper() == "POST" else None
    req = request.Request(
        url=url,
        data=data,
        headers=headers or {},
        method=method.upper(),
    )
    try:
        with request.urlopen(req, timeout=timeout_seconds) as response:
            body = response.read().decode("utf-8")
            return response.status, json.loads(body or "{}")
    except error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Curie API returned HTTP {exc.code}: {body}") from exc
    except error.URLError as exc:
        raise RuntimeError(f"Curie API request failed: {exc}") from exc


def read_curie_iceberg_cache_status() -> dict:
    """Read the status for the isolated Iceberg cache."""
    timeout_seconds = int(
        Variable.get("curie_api_status_timeout_seconds", default="30")
    )
    status_code, payload = _request_json(
        method="GET",
        url=f"{_curie_api_base_url()}{STATUS_PATH}",
        timeout_seconds=timeout_seconds,
    )
    if status_code != 200:
        raise RuntimeError(
            f"Expected Curie Iceberg cache status HTTP 200, got {status_code}"
        )
    return payload


def _validate_iceberg_cache_status(payload: dict) -> dict:
    """Require a published release containing the complete expected Gold table set."""
    if not payload.get("configured") or not payload.get("active_release_id"):
        raise RuntimeError("Curie Iceberg cache has no active release")

    tables = payload.get("tables", [])
    actual_tables = {table.get("name") for table in tables}
    if actual_tables != EXPECTED_TABLES:
        missing = sorted(EXPECTED_TABLES - actual_tables)
        unexpected = sorted(actual_tables - EXPECTED_TABLES)
        raise RuntimeError(
            "Curie Iceberg cache table set does not match the 8 expected Gold "
            f"tables; missing={missing}, unexpected={unexpected}"
        )
    return payload


def _trigger_iceberg_cache_refresh() -> dict:
    """Request Curie to build a new Iceberg cache release."""
    timeout_seconds = int(
        Variable.get("curie_api_refresh_timeout_seconds", default="30")
    )
    status_code, payload = _request_json(
        method="POST",
        url=f"{_curie_api_base_url()}{REFRESH_PATH}",
        headers={ADMIN_KEY_HEADER: _curie_admin_key()},
        timeout_seconds=timeout_seconds,
    )
    if status_code != 202:
        raise RuntimeError(
            f"Expected Curie Iceberg refresh HTTP 202, got {status_code}"
        )
    logging.getLogger(DAG_ID).info(
        "Curie Iceberg cache refresh accepted: status=%s, job_id=%s",
        payload.get("status"),
        payload.get("job_id"),
    )
    return payload


def trigger_and_wait_for_iceberg_cache_refresh() -> dict:
    """Trigger the Iceberg refresh and wait until a new cache release is active."""
    logger = logging.getLogger(DAG_ID)
    old_status = read_curie_iceberg_cache_status()
    old_release_id = old_status.get("active_release_id")
    _trigger_iceberg_cache_refresh()

    max_wait_seconds = int(
        Variable.get("curie_api_release_update_max_wait_seconds", default="2700")
    )
    poll_interval_seconds = int(
        Variable.get("curie_api_release_update_poll_interval_seconds", default="60")
    )
    deadline = time.monotonic() + max_wait_seconds
    logger.info(
        "Waiting for Curie Iceberg cache release: old_release_id=%s, "
        "max_wait_seconds=%s, poll_interval_seconds=%s",
        old_release_id,
        max_wait_seconds,
        poll_interval_seconds,
    )

    while True:
        remaining_seconds = deadline - time.monotonic()
        if remaining_seconds <= 0:
            raise RuntimeError(
                "Curie Iceberg cache release did not update within "
                f"{max_wait_seconds} seconds; old_release_id={old_release_id}"
            )

        time.sleep(min(poll_interval_seconds, remaining_seconds))
        status = read_curie_iceberg_cache_status()
        active_release_id = status.get("active_release_id")
        logger.info(
            "Curie Iceberg cache poll: active_release_id=%s, table_count=%s",
            active_release_id,
            len(status.get("tables", [])),
        )
        if active_release_id and active_release_id != old_release_id:
            return _validate_iceberg_cache_status(status)


def check_curie_iceberg_cache_status() -> dict:
    """Log the published release and table row counts for cache comparison."""
    logger = logging.getLogger(DAG_ID)
    payload = _validate_iceberg_cache_status(read_curie_iceberg_cache_status())
    logger.info(
        "Curie Iceberg cache active: release_id=%s, table_count=%s",
        payload["active_release_id"],
        len(payload["tables"]),
    )
    for table in sorted(payload["tables"], key=lambda item: item["name"]):
        logger.info(
            "Curie Iceberg cache table: name=%s, row_count=%s, checksum=%s",
            table["name"],
            table["row_count"],
            table["checksum"],
        )
    return payload


def _sunday_housekeeping_task(**context) -> str:
    """Select maintenance only after a successful Sunday pipeline."""
    logical_date = context["dag_run"].logical_date or context["dag_run"].run_after
    if logical_date.astimezone(ZoneInfo("Europe/Budapest")).weekday() == 6:
        return "trigger__iceberg__housekeeping__weekly"
    return "skip__iceberg__housekeeping__weekly"


with DAG(
    dag_id=DAG_ID,
    default_args=standard_default_args(retries=2),
    schedule=None,
    start_date=datetime(2025, 8, 24),
    tags=[
        "layer:gold",
        "format:iceberg",
        "system:curie",
        "system:api",
        "mode:post_gold",
    ],
    catchup=False,
    max_active_runs=1,
) as dag:
    # Mark the start of the isolated Curie Iceberg cache refresh.
    start_task = PythonOperator(
        task_id="run__curie_iceberg_cache_refresh__start",
        python_callable=print,
        op_args=["##### startCurieIcebergCacheRefresh #####"],
    )

    # Request a new cache build and wait for its release to become active.
    refresh_cache = PythonOperator(
        task_id="run__curie_iceberg_cache_refresh__trigger_and_wait",
        python_callable=trigger_and_wait_for_iceberg_cache_refresh,
    )

    # Verify all eight expected tables and log row counts/checksums for comparison.
    check_status = PythonOperator(
        task_id="run__curie_iceberg_cache_refresh__status",
        python_callable=check_curie_iceberg_cache_status,
    )

    # Mark completion after the release has been validated.
    done_task = PythonOperator(
        task_id="run__curie_iceberg_cache_refresh__done",
        python_callable=print,
        op_args=["##### doneCurieIcebergCacheRefresh #####"],
    )

    start_task >> refresh_cache >> check_status >> done_task

    choose_housekeeping = BranchPythonOperator(
        task_id="branch__iceberg__housekeeping__sunday",
        python_callable=_sunday_housekeeping_task,
    )
    trigger_housekeeping = TriggerDagRunOperator(
        task_id="trigger__iceberg__housekeeping__weekly",
        trigger_dag_id="ampere__housekeeping__iceberg_metadata__weekly",
        logical_date="{{ (dag_run.logical_date or dag_run.run_after).isoformat() }}",
        reset_dag_run=True,
        wait_for_completion=False,
    )
    skip_housekeeping = EmptyOperator(task_id="skip__iceberg__housekeeping__weekly")

    done_task >> choose_housekeeping >> [trigger_housekeeping, skip_housekeeping]
