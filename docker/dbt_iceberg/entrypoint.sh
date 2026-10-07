#!/usr/bin/env bash
set -euo pipefail

started="$SECONDS"
if [[ "${ICEBERG_PUBLISH_MODE:-direct}" == "staged" ]]; then
  scratch_root="${DUCKDB_SCRATCH_ROOT:-/app/artifacts}"
  mkdir -p "$scratch_root"
  run_scratch="$(mktemp -d "$scratch_root/run.XXXXXXXX")"
  export DUCKDB_PATH="$run_scratch/ampere_work.duckdb"
  export DUCKDB_TEMP_DIRECTORY="$run_scratch/duckdb_tmp"
  trap 'rm -rf -- "$run_scratch"' EXIT
fi
python /app/prepare_catalog.py
echo "phase=catalog_prepare elapsed_seconds=$((SECONDS - started))"

report_resources() {
  local phase="$1"
  if [[ -r /sys/fs/cgroup/memory.peak ]]; then
    echo "phase=${phase} cgroup_memory_peak_bytes=$(cat /sys/fs/cgroup/memory.peak)"
  fi
  echo "phase=${phase} scratch_free_bytes=$(df -B1 --output=avail /app/artifacts | tail -n 1 | tr -d ' ')"
}

if [ "$#" -eq 0 ]; then
  set -- build
fi

run_dbt() {
  local phase="$1"
  shift
  local started="$SECONDS"
  dbt "$@" --project-dir "${DBT_PROJECT_DIR}" --profiles-dir "${DBT_PROFILES_DIR}" --threads "${DBT_THREADS:-2}"
  echo "phase=${phase} elapsed_seconds=$((SECONDS - started))"
  report_resources "$phase"
}

if [[ "${ICEBERG_PUBLISH_MODE:-direct}" == "staged" \
      && "${SILVER_RUN_MODE:-}" == "full_history" \
      && "${GOLD_RUN_MODE:-}" == "full_history" \
      && "$#" -eq 1 && "$1" == "build" ]]; then
  # A staged full rebuild needs room for both complete databases and DuckDB spill.
  python - <<'PY'
import os
from pathlib import Path

workspace = Path(os.getenv("DUCKDB_PATH", "/app/artifacts/ampere_work.duckdb"))
free_bytes = os.statvfs(workspace.parent).f_bavail * os.statvfs(workspace.parent).f_frsize
minimum_bytes = int(os.getenv("ICEBERG_FULL_STAGE_MIN_FREE_GB", "16")) * 1024**3
print(f"Full rebuild scratch: {free_bytes / 1024**3:.1f} GiB free", flush=True)
if free_bytes < minimum_bytes:
    raise RuntimeError(
        f"Staged full rebuild needs at least {minimum_bytes / 1024**3:.0f} GiB "
        "of free scratch before dbt starts"
    )
PY
  run_dbt silver_build build --select tag:silver
  run_dbt gold_build build --select tag:gold
else
  run_dbt dbt_build "$@"
fi

if [[ "${ICEBERG_PUBLISH_MODE:-direct}" == "staged" ]]; then
  started="$SECONDS"
  python /app/publish_catalog.py
  echo "phase=iceberg_publish elapsed_seconds=$((SECONDS - started))"
  report_resources iceberg_publish
fi
