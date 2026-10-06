"""Rebuild Delta Silver, then Gold, with bounded dbt work per window.

Each window runs in a fresh dbt process and publishes before a MinIO checkpoint
is written. Retrying the Airflow task resumes after the last complete window.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
from datetime import date, timedelta
from pathlib import Path
from urllib.parse import quote

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError


ENTRYPOINT = "/usr/local/bin/ampere_dbt_entrypoint.sh"


def windows(start: date, end: date, days: int) -> list[tuple[str, str]]:
    """Cover the half-open date range with nonoverlapping bounded windows."""
    if start >= end or days < 1 or days > 31:
        raise ValueError("Rebuild needs start < end and window days between 1 and 31")
    result = []
    current = start
    while current < end:
        next_date = min(current + timedelta(days=days), end)
        result.append((current.isoformat(), next_date.isoformat()))
        current = next_date
    return result


def checkpoint_client():
    """Use the pod's MinIO credentials for durable progress markers."""
    endpoint = os.environ["MINIO_S3_ENDPOINT"]
    if not endpoint.startswith(("http://", "https://")):
        scheme = "https" if os.getenv("MINIO_S3_USE_SSL", "false").lower() == "true" else "http"
        endpoint = f"{scheme}://{endpoint}"
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
        aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
        region_name=os.getenv("MINIO_S3_REGION", "us-east-1"),
        config=Config(s3={"addressing_style": "path"}),
    )


def read_checkpoint(client, bucket: str, key: str, expected: dict) -> dict:
    """Load progress only when its window plan matches this DAG run."""
    try:
        body = client.get_object(Bucket=bucket, Key=key)["Body"].read()
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in {"NoSuchKey", "404"}:
            return {**expected, "silver_done": [], "gold_done": []}
        raise
    state = json.loads(body)
    for name, value in expected.items():
        if state.get(name) != value:
            raise RuntimeError(f"Checkpoint {key} has a different {name}")
    return state


def save_checkpoint(client, bucket: str, key: str, state: dict) -> None:
    """Persist one completed window after all its Delta publishes succeed."""
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(state, sort_keys=True).encode(),
        ContentType="application/json",
    )


def run_window(layer: str, start: str, end: str, first: bool) -> None:
    """Launch one bounded dbt build and publish in a fresh DuckDB file."""
    environment = os.environ.copy()
    environment.update(
        {
            "THREADS": "1",
            "DUCKDB_MEMORY_LIMIT": os.getenv("REBUILD_DUCKDB_MEMORY_LIMIT", "5GB"),
            "RUN_DBT_ARTIFACT_UPLOAD": "false",
            "GOLD_DBT_COMMAND": "",
            "SILVER_RUN_MODE": "daily_refresh",
            "GOLD_RUN_MODE": "daily_refresh",
            "RUN_SILVER_PUBLISH": "true" if layer == "silver" else "false",
            "RUN_GOLD_PUBLISH": "true" if layer == "gold" else "false",
            "RUN_BRONZE_SOURCE_PREPARE": "true" if layer == "silver" else "false",
            f"{layer.upper()}_WINDOW_START": start,
            f"{layer.upper()}_WINDOW_END": end,
            f"{layer.upper()}_PUBLISH_RUN_MODE": "full_rebuild" if first else "daily_refresh",
        }
    )
    # Never let a variable from the outer pod narrow the other layer's build.
    environment.pop("GOLD_WINDOW_START" if layer == "silver" else "SILVER_WINDOW_START", None)
    environment.pop("GOLD_WINDOW_END" if layer == "silver" else "SILVER_WINDOW_END", None)
    database = Path(environment.get("DUCKDB_PATH", "/app/artifacts/ampere.duckdb"))
    database.unlink(missing_ok=True)
    print(f"Rebuild {layer} window [{start}, {end}) bootstrap={first}", flush=True)
    command = "dbt run --select tag:silver --full-refresh" if layer == "silver" else "dbt run --select tag:gold --full-refresh"
    try:
        subprocess.run([ENTRYPOINT, command], env=environment, check=True)
    finally:
        database.unlink(missing_ok=True)


def main() -> None:
    """Publish all Silver windows before rebuilding Gold from complete Silver."""
    start = date.fromisoformat(os.getenv("REBUILD_START_DATE", "2025-12-01"))
    end = date.fromisoformat(os.environ["LOGICAL_DATE"]) + timedelta(days=1)
    days = int(os.getenv("REBUILD_WINDOW_DAYS", "14"))
    plan = windows(start, end, days)
    run_id = os.environ["REBUILD_RUN_ID"]
    if not re.fullmatch(r"[A-Za-z0-9_.:+-]+", run_id):
        raise ValueError("Unsafe rebuild run ID")
    bucket = os.getenv("REBUILD_CHECKPOINT_BUCKET", "ampere-silver-ops")
    key = f"dbt/rebuild_checkpoints/{quote(run_id, safe='')}.json"
    client = checkpoint_client()
    expected = {"start": start.isoformat(), "end": end.isoformat(), "days": days}
    state = read_checkpoint(client, bucket, key, expected)
    for layer in ("silver", "gold"):
        done = state[f"{layer}_done"]
        for index, (window_start, window_end) in enumerate(plan):
            if window_start in done:
                continue
            run_window(layer, window_start, window_end, first=index == 0)
            done.append(window_start)
            save_checkpoint(client, bucket, key, state)
            print(f"Checkpointed {layer} window {window_start}", flush=True)
    print(f"Completed {len(plan)} Silver and {len(plan)} Gold windows", flush=True)


if __name__ == "__main__":
    main()
