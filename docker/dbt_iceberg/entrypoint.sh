#!/usr/bin/env bash
set -euo pipefail

started="$SECONDS"
export ICEBERG_RUNTIME_STATE_FILE="${ICEBERG_RUNTIME_STATE_FILE:-/tmp/iceberg-runtime-state.sh}"
python /app/incremental_state.py prepare
source "$ICEBERG_RUNTIME_STATE_FILE"
if [[ "${ICEBERG_VALIDATION_ONLY:-false}" == "true" && "${ICEBERG_RUN_MODE:-daily_refresh}" != "full_history" ]]; then
  echo 'validation_only requires a full-rebuild DAG' >&2
  exit 2
fi
if [[ "${ICEBERG_LAYER}" == "gold" && "${ICEBERG_RUN_MODE:-daily_refresh}" == "daily_refresh" && -z "${GOLD_AFFECTED_MONTHS}" ]]; then
  python -c 'from incremental_state import record_gold_publication; record_gold_publication()'
  echo 'phase=gold_noop affected_months=0'
  exit 0
fi
if [[ "${ICEBERG_PUBLISH_MODE:-staged}" == "staged" ]]; then
  scratch_root="${DUCKDB_SCRATCH_ROOT:-/app/artifacts}"
  mkdir -p "$scratch_root"
  run_scratch="$(mktemp -d "$scratch_root/run.XXXXXXXX")"
  export DUCKDB_PATH="$run_scratch/ampere_work.duckdb"
  export DUCKDB_TEMP_DIRECTORY="$run_scratch/duckdb_tmp"
  mkdir -p "$DUCKDB_TEMP_DIRECTORY"
fi
python /app/prepare_catalog.py
echo "phase=catalog_prepare elapsed_seconds=$((SECONDS - started))"

active_phase=""
report_resources() {
  local phase="$1"
  local peak_file="/sys/fs/cgroup/memory.peak"
  if [[ ! -r "$peak_file" && -r /sys/fs/cgroup/memory/memory.max_usage_in_bytes ]]; then
    peak_file="/sys/fs/cgroup/memory/memory.max_usage_in_bytes"
  elif [[ ! -r "$peak_file" && -r /sys/fs/cgroup/memory.max_usage_in_bytes ]]; then
    peak_file="/sys/fs/cgroup/memory.max_usage_in_bytes"
  fi
  if [[ -r "$peak_file" ]]; then
    echo "phase=${phase} cgroup_memory_peak_bytes=$(cat "$peak_file")"
  fi
  echo "phase=${phase} scratch_free_bytes=$(df -B1 --output=avail /app/artifacts | tail -n 1 | tr -d ' ')"
  if [[ -d "${DUCKDB_TEMP_DIRECTORY:-}" ]]; then
    local spill_bytes
    if spill_bytes="$(du -sb "$DUCKDB_TEMP_DIRECTORY" 2>/dev/null | cut -f1)"; then
      echo "phase=${phase} duckdb_spill_bytes=${spill_bytes}"
    fi
  fi
}

reset_memory_peak() {
  local peak_file="/sys/fs/cgroup/memory.peak"
  if [[ ! -w "$peak_file" && -w /sys/fs/cgroup/memory/memory.max_usage_in_bytes ]]; then
    peak_file="/sys/fs/cgroup/memory/memory.max_usage_in_bytes"
  elif [[ ! -w "$peak_file" && -w /sys/fs/cgroup/memory.max_usage_in_bytes ]]; then
    peak_file="/sys/fs/cgroup/memory.max_usage_in_bytes"
  fi
  if [[ -w "$peak_file" ]]; then
    printf '0\n' > "$peak_file" || true
  fi
}

cleanup() {
  local exit_status=$?
  if [[ -n "$active_phase" ]]; then
    report_resources "${active_phase}_exit" || true
  fi
  if [[ -n "${run_scratch:-}" ]]; then
    rm -rf -- "$run_scratch" || true
  fi
  return "$exit_status"
}
trap cleanup EXIT

if [ "$#" -eq 0 ]; then
  set -- build
fi

run_dbt() {
  local phase="$1"
  shift
  local started="$SECONDS"
  active_phase="$phase"
  reset_memory_peak
  python /app/phase_metrics.py "$phase" dbt "$@" --project-dir "${DBT_PROJECT_DIR}" --profiles-dir "${DBT_PROFILES_DIR}" --threads "${DBT_THREADS:-2}"
  echo "phase=${phase} elapsed_seconds=$((SECONDS - started))"
  report_resources "$phase"
  active_phase=""
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

active_phase="iceberg_publish"
runner="/app/publish_catalog.py"
if [[ "${ICEBERG_VALIDATION_ONLY:-false}" == "true" ]]; then
  active_phase="iceberg_validate"
  runner="/app/validate_staged.py"
fi
reset_memory_peak
started="$SECONDS"
python /app/phase_metrics.py "$active_phase" python "$runner" --layer "$layer"
echo "phase=${active_phase} elapsed_seconds=$((SECONDS - started))"
report_resources "$active_phase"
active_phase=""
