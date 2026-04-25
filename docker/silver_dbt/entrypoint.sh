#!/usr/bin/env bash
set -euo pipefail

log() { printf '[silver-dbt] %s\n' "$*"; }

: "${DBT_PROJECT_DIR:=/app/dbt}"
: "${DBT_PROFILES_DIR:=/app/profiles}"
: "${DUCKDB_PATH:=/app/artifacts/ampere.duckdb}"
: "${DBT_LOG_PATH:=/app/logs}"
: "${THREADS:=4}"
: "${DBT_TARGET:=prod}"
: "${BRONZE_SOURCE_NAME:=bronze}"
: "${BRONZE_SOURCE_SCHEMA:=bronze}"
: "${BRONZE_SOURCE_MAPPING_PATH:=/app/artifacts/bronze_source_mapping.json}"
: "${BRONZE_SOURCE_MAPPING_MAX_AGE_HOURS:=24}"
: "${RUN_UC_MAPPING_GENERATION:=true}"
: "${RUN_BRONZE_PREFLIGHT:=true}"
: "${RUN_BRONZE_PREFLIGHT_DELTA_SCAN:=true}"

export PATH="/app/.venv/bin:${PATH}"
export BRONZE_SOURCE_NAME BRONZE_SOURCE_SCHEMA
export BRONZE_SOURCE_MAPPING_PATH BRONZE_SOURCE_MAPPING_MAX_AGE_HOURS

mkdir -p "$(dirname "${DUCKDB_PATH}")"
mkdir -p "${DBT_LOG_PATH}"
mkdir -p "$(dirname "${BRONZE_SOURCE_MAPPING_PATH}")"

if [ ! -f "${DBT_PROJECT_DIR}/dbt_project.yml" ]; then
  log "dbt project file not found at ${DBT_PROJECT_DIR}/dbt_project.yml"
  log "Build context must include the repo-root dbt project."
  exit 1
fi

log "render profiles"
/usr/local/bin/render_profiles.sh

if [ ! -f "${DBT_PROFILES_DIR}/profiles.yml" ]; then
  log "profiles.yml not found at ${DBT_PROFILES_DIR}"
  exit 1
fi

if [[ "${RUN_UC_MAPPING_GENERATION}" == "true" ]]; then
  log "generate bronze source mapping from UC"
  python /app/scripts/generate_bronze_source_mapping.py \
    --project-dir "${DBT_PROJECT_DIR}" \
    --source-name "${BRONZE_SOURCE_NAME}" \
    --output "${BRONZE_SOURCE_MAPPING_PATH}"
fi

if [[ "${RUN_BRONZE_PREFLIGHT}" == "true" ]]; then
  log "validate bronze source mapping"
  VALIDATE_ARGS=(
    --project-dir "${DBT_PROJECT_DIR}"
    --source-name "${BRONZE_SOURCE_NAME}"
    --mapping-path "${BRONZE_SOURCE_MAPPING_PATH}"
    --max-age-hours "${BRONZE_SOURCE_MAPPING_MAX_AGE_HOURS}"
  )
  if [[ "${RUN_BRONZE_PREFLIGHT_DELTA_SCAN}" != "true" ]]; then
    VALIDATE_ARGS+=(--skip-delta-scan)
  fi
  python /app/scripts/validate_bronze_sources.py "${VALIDATE_ARGS[@]}"
fi

DBT_BIN="/app/.venv/bin/dbt"
if [ ! -x "${DBT_BIN}" ]; then
  log "dbt binary not found at ${DBT_BIN}"
  exit 127
fi

RAW_CMD="$*"
if [ -z "${RAW_CMD}" ]; then
  RAW_CMD="${DBT_CMD-}"
fi
if [ -z "${RAW_CMD}" ]; then
  RAW_CMD="dbt build"
fi

if [[ "${RAW_CMD}" == dbt\ * ]]; then
  RAW_ARGS="${RAW_CMD#dbt }"
else
  RAW_ARGS="${RAW_CMD}"
fi

append_if_missing() {
  local flag="$1"
  local value="$2"
  grep -Eq "(^|[[:space:]])${flag}([[:space:]]|$)" <<<"${RAW_ARGS}" || RAW_ARGS+=" ${flag} ${value}"
}

append_if_missing "--project-dir" "${DBT_PROJECT_DIR}"
append_if_missing "--profiles-dir" "${DBT_PROFILES_DIR}"
append_if_missing "--threads" "${THREADS}"
append_if_missing "--target" "${DBT_TARGET}"
grep -Eq "(^|[[:space:]])--fail-fast([[:space:]]|$)" <<<"${RAW_ARGS}" || RAW_ARGS+=" --fail-fast"

log "dbt version: $("${DBT_BIN}" --version | head -n1)"
log "exec: ${DBT_BIN} ${RAW_ARGS}"
exec bash -lc "${DBT_BIN} ${RAW_ARGS}"
