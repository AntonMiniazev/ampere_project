#!/usr/bin/env bash
set -euo pipefail

started="$SECONDS"
if [[ "${ICEBERG_PUBLISH_MODE:-staged}" == "staged" ]]; then
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

layer="${ICEBERG_LAYER:?ICEBERG_LAYER must be silver or gold}"
if [[ "$layer" != "silver" && "$layer" != "gold" ]]; then
  echo "ICEBERG_LAYER must be silver or gold" >&2
  exit 2
fi
run_mode="${ICEBERG_RUN_MODE:-daily_refresh}"
if [[ "$run_mode" != "daily_refresh" && "$run_mode" != "full_history" ]]; then
  echo "ICEBERG_RUN_MODE must be daily_refresh or full_history" >&2
  exit 2
fi
if [[ "$run_mode" == "full_history" ]]; then
  python - <<'PY'
import os
from pathlib import Path

workspace = Path(os.environ["DUCKDB_PATH"])
stat = os.statvfs(workspace.parent)
free_bytes = stat.f_bavail * stat.f_frsize
minimum_bytes = int(os.getenv("ICEBERG_FULL_STAGE_MIN_FREE_GB", "16")) * 1024**3
print(f"Full {os.environ['ICEBERG_LAYER']} rebuild scratch: {free_bytes / 1024**3:.1f} GiB free", flush=True)
if free_bytes < minimum_bytes:
    raise RuntimeError(f"Full rebuild requires at least {minimum_bytes / 1024**3:.0f} GiB free scratch before dbt starts")
PY
fi
run_dbt "${layer}_build" build --select "tag:${layer}"

started="$SECONDS"
python /app/publish_catalog.py --layer "$layer"
echo "phase=iceberg_publish elapsed_seconds=$((SECONDS - started))"
report_resources iceberg_publish
