from __future__ import annotations

import json
import logging
from datetime import datetime
from urllib import error, request

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Variable

from utils.ampere_dag_config import standard_default_args

DAG_ID = "ampere__curie__cache_refresh__post_gold"
DEFAULT_CURIE_API_BASE_URL = "http://100.65.42.72:8000"
REFRESH_PATH = "/api/cache/refresh"
STATUS_PATH = "/api/cache/status"
ADMIN_KEY_HEADER = "X-Curie-Admin-Key"


def _curie_api_base_url() -> str:
    """Return the configured Curie API base URL without a trailing slash."""
    return Variable.get(
        "curie_api_base_url",
        default=DEFAULT_CURIE_API_BASE_URL,
    ).strip().rstrip("/")


def _curie_admin_key() -> str:
    """Return the configured Curie admin key, failing fast when it is missing."""
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
    """Call Curie and return a JSON body with its HTTP status code."""
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


def trigger_curie_cache_refresh() -> dict:
    """Trigger Curie cache refresh after Ampere Gold has been published."""
    logger = logging.getLogger(DAG_ID)
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
        raise RuntimeError(f"Expected Curie refresh HTTP 202, got {status_code}")
    logger.info(
        "Curie cache refresh accepted: status=%s, job_id=%s",
        payload.get("status"),
        payload.get("job_id"),
    )
    return payload


def check_curie_cache_status() -> dict:
    """Read Curie's cache status after the refresh trigger is accepted."""
    logger = logging.getLogger(DAG_ID)
    timeout_seconds = int(
        Variable.get("curie_api_status_timeout_seconds", default="30")
    )
    status_code, payload = _request_json(
        method="GET",
        url=f"{_curie_api_base_url()}{STATUS_PATH}",
        timeout_seconds=timeout_seconds,
    )
    if status_code != 200:
        raise RuntimeError(f"Expected Curie cache status HTTP 200, got {status_code}")
    logger.info(
        "Curie cache status: configured=%s, active_release_id=%s, table_count=%s",
        payload.get("configured"),
        payload.get("active_release_id"),
        len(payload.get("tables", [])),
    )
    return payload


with DAG(
    dag_id=DAG_ID,
    default_args=standard_default_args(retries=2),
    schedule=None,
    start_date=datetime(2025, 8, 24),
    tags=[
        "layer:gold",
        "system:curie",
        "system:api",
        "mode:post_gold",
    ],
    catchup=False,
    max_active_runs=1,
) as dag:
    # Boundary marker before Curie cache refresh is requested.
    start_task = PythonOperator(
        task_id="run__curie_cache_refresh__start",
        python_callable=print,
        op_args=["##### startCurieCacheRefresh #####"],
    )

    # Curie starts the remote Polars cache update job and returns HTTP 202.
    refresh_cache = PythonOperator(
        task_id="run__curie_cache_refresh__trigger",
        python_callable=trigger_curie_cache_refresh,
    )

    # Status check confirms that the Curie API remains reachable after trigger.
    check_status = PythonOperator(
        task_id="run__curie_cache_refresh__status",
        python_callable=check_curie_cache_status,
    )

    # Boundary marker after Curie accepted the cache refresh request.
    done_task = PythonOperator(
        task_id="run__curie_cache_refresh__done",
        python_callable=print,
        op_args=["##### doneCurieCacheRefresh #####"],
    )

    start_task >> refresh_cache >> check_status >> done_task
