#!/usr/bin/env bash
set -euo pipefail

log() { printf '[ampere-dbt] %s\n' "$*"; }

: "${DBT_PROJECT_DIR:=/app/dbt}"
: "${DBT_PROFILES_DIR:=/app/profiles}"
: "${DUCKDB_PATH:=/app/artifacts/ampere.duckdb}"
: "${DBT_LOG_PATH:=/app/logs}"
: "${THREADS:=4}"
: "${DBT_TARGET:=prod}"
: "${BRONZE_SOURCE_NAME:=bronze}"
: "${BRONZE_SOURCE_SCHEMA:=bronze}"
: "${BRONZE_SOURCE_MAPPING_PATH:=/app/artifacts/bronze_source_mapping.json}"
: "${SILVER_SOURCE_NAME:=silver}"
: "${SILVER_SOURCE_SCHEMA:=silver}"
: "${SILVER_SOURCE_MAPPING_PATH:=/app/artifacts/silver_source_mapping.json}"
: "${RUN_SILVER_PUBLISH:=true}"
: "${RUN_SILVER_UC_REGISTRATION:=true}"
: "${RUN_GOLD_PUBLISH:=false}"
: "${RUN_GOLD_UC_REGISTRATION:=true}"
: "${RUN_DBT_ARTIFACT_UPLOAD:=true}"
: "${SILVER_RUN_MODE:=daily_refresh}"
: "${SILVER_LOOKBACK_DAYS:=7}"
: "${SILVER_PUBLISH_MANIFEST_PATH:=/app/artifacts/silver_publish_manifest.json}"
: "${SILVER_DBT_ARTIFACT_ROOT:=s3://ampere-silver-ops/dbt}"
: "${GOLD_RUN_MODE:=daily_refresh}"
: "${GOLD_LOOKBACK_DAYS:=${SILVER_LOOKBACK_DAYS}}"
: "${GOLD_PUBLISH_MANIFEST_PATH:=/app/artifacts/gold_publish_manifest.json}"
: "${GOLD_DBT_ARTIFACT_ROOT:=s3://ampere-gold-ops/dbt}"

export PATH="/app/.venv/bin:${PATH}"
export BRONZE_SOURCE_NAME BRONZE_SOURCE_SCHEMA BRONZE_SOURCE_MAPPING_PATH
export SILVER_SOURCE_NAME SILVER_SOURCE_SCHEMA SILVER_SOURCE_MAPPING_PATH

normalize_bool() {
  local raw="${1:-false}"
  case "${raw,,}" in
    1|true|yes|y|on) printf 'true' ;;
    *) printf 'false' ;;
  esac
}

configure_aws_env_for_minio() {
  local endpoint="${MINIO_S3_ENDPOINT:-}"
  local use_ssl
  use_ssl="$(normalize_bool "${MINIO_S3_USE_SSL:-false}")"
  if [[ "${endpoint}" == https://* ]]; then
    endpoint="${endpoint#https://}"
    use_ssl=true
  elif [[ "${endpoint}" == http://* ]]; then
    endpoint="${endpoint#http://}"
    use_ssl=false
  fi
  local endpoint_with_scheme="http://${endpoint}"
  if [[ "${use_ssl}" == "true" ]]; then
    endpoint_with_scheme="https://${endpoint}"
  fi

  export AWS_EC2_METADATA_DISABLED=true
  export AWS_METADATA_SERVICE_NUM_ATTEMPTS=1
  export AWS_METADATA_SERVICE_TIMEOUT=1
  export AWS_REGION="${MINIO_S3_REGION:-us-east-1}"
  export AWS_DEFAULT_REGION="${AWS_REGION}"
  export AWS_ENDPOINT_URL="${endpoint_with_scheme}"
  export AWS_ENDPOINT_URL_S3="${endpoint_with_scheme}"
  export AWS_S3_FORCE_PATH_STYLE=true
  if [[ "${use_ssl}" != "true" ]]; then
    export AWS_ALLOW_HTTP=true
  fi
  if [[ -n "${MINIO_ACCESS_KEY:-}" ]]; then
    export AWS_ACCESS_KEY_ID="${MINIO_ACCESS_KEY}"
  fi
  if [[ -n "${MINIO_SECRET_KEY:-}" ]]; then
    export AWS_SECRET_ACCESS_KEY="${MINIO_SECRET_KEY}"
  fi
}

configure_aws_env_for_minio

mkdir -p "$(dirname "${DUCKDB_PATH}")"
mkdir -p "${DBT_LOG_PATH}"

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

DBT_BIN="/app/.venv/bin/dbt"
if [ ! -x "${DBT_BIN}" ]; then
  log "dbt binary not found at ${DBT_BIN}"
  exit 127
fi

if [[ "${RUN_BRONZE_SOURCE_PREPARE:-true}" == "true" ]]; then
  log "create bronze UC source mapping"
  python /app/scripts/create_uc_source_mapping.py \
    --project-dir "${DBT_PROJECT_DIR}" \
    --source-name "${BRONZE_SOURCE_NAME}" \
    --catalog "${BRONZE_UC_CATALOG:-ampere}" \
    --schema "${BRONZE_UC_SCHEMA:-bronze}" \
    --output "${BRONZE_SOURCE_MAPPING_PATH}"
fi

log "create silver UC source mapping"
python /app/scripts/create_uc_source_mapping.py \
  --project-dir "${DBT_PROJECT_DIR}" \
  --source-name "${SILVER_SOURCE_NAME}" \
  --catalog "${SILVER_UC_CATALOG:-${BRONZE_UC_CATALOG:-ampere}}" \
  --schema "${SILVER_UC_SCHEMA:-silver}" \
  --output "${SILVER_SOURCE_MAPPING_PATH}"

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
bash -lc "${DBT_BIN} ${RAW_ARGS}"

if [[ "${RUN_SILVER_PUBLISH}" == "true" ]]; then
  log "publish silver tables"
  python /app/scripts/publish_silver_tables.py \
    --layer silver \
    --duckdb-path "${DUCKDB_PATH}" \
    --manifest-path "${DBT_PROJECT_DIR}/target/manifest.json" \
    --run-mode "${SILVER_RUN_MODE}" \
    --local-manifest-output "${SILVER_PUBLISH_MANIFEST_PATH}"
fi

if [[ "${RUN_SILVER_PUBLISH}" == "true" && "${RUN_SILVER_UC_REGISTRATION}" == "true" ]]; then
  log "check published silver Delta locations"
  python /app/scripts/register_silver_uc_tables.py \
    --publish-manifest-path "${SILVER_PUBLISH_MANIFEST_PATH}"
fi

if [[ "${RUN_GOLD_PUBLISH}" == "true" ]]; then
  log "publish gold tables"
  python /app/scripts/publish_silver_tables.py \
    --layer gold \
    --duckdb-path "${DUCKDB_PATH}" \
    --manifest-path "${DBT_PROJECT_DIR}/target/manifest.json" \
    --run-mode "${GOLD_RUN_MODE}" \
    --local-manifest-output "${GOLD_PUBLISH_MANIFEST_PATH}"
fi

if [[ "${RUN_GOLD_PUBLISH}" == "true" && "${RUN_GOLD_UC_REGISTRATION}" == "true" ]]; then
  log "check published gold Delta locations"
  python /app/scripts/register_silver_uc_tables.py \
    --publish-manifest-path "${GOLD_PUBLISH_MANIFEST_PATH}" \
    --catalog "${GOLD_UC_CATALOG:-${BRONZE_UC_CATALOG:-ampere}}" \
    --schema "${GOLD_UC_SCHEMA:-gold}"
fi

if [[ "${RUN_DBT_ARTIFACT_UPLOAD}" == "true" && "${RUN_SILVER_PUBLISH}" == "true" ]]; then
  log "upload silver dbt artifacts"
  python /app/scripts/upload_dbt_artifacts.py \
    --artifacts-dir "${DBT_PROJECT_DIR}/target" \
    --log-file "${DBT_LOG_PATH}/dbt.log" \
    --upload-root "${SILVER_DBT_ARTIFACT_ROOT}" \
    --extra-file "${SILVER_PUBLISH_MANIFEST_PATH}"
fi

if [[ "${RUN_DBT_ARTIFACT_UPLOAD}" == "true" && "${RUN_GOLD_PUBLISH}" == "true" ]]; then
  log "upload gold dbt artifacts"
  python /app/scripts/upload_dbt_artifacts.py \
    --artifacts-dir "${DBT_PROJECT_DIR}/target" \
    --log-file "${DBT_LOG_PATH}/dbt.log" \
    --upload-root "${GOLD_DBT_ARTIFACT_ROOT}" \
    --extra-file "${GOLD_PUBLISH_MANIFEST_PATH}"
fi

if [[ "${RUN_DBT_ARTIFACT_UPLOAD}" == "true" && "${RUN_SILVER_PUBLISH}" != "true" && "${RUN_GOLD_PUBLISH}" != "true" ]]; then
  log "upload dbt artifacts"
  python /app/scripts/upload_dbt_artifacts.py \
    --artifacts-dir "${DBT_PROJECT_DIR}/target" \
    --log-file "${DBT_LOG_PATH}/dbt.log" \
    --upload-root "${DBT_ARTIFACT_ROOT:-${SILVER_DBT_ARTIFACT_ROOT}}"
fi
