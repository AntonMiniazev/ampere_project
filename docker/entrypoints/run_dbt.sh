#!/usr/bin/env bash
set -euo pipefail

log() { printf '[runner] %s\n' "$*"; }

# Defaults
: "${DBT_PROJECT_DIR:=/app/project}"
: "${DBT_PROFILES_DIR:=/app/profiles}"
: "${DUCKDB_PATH:=/app/artifacts/ampere.duckdb}"
: "${THREADS:=4}"
: "${DBT_TARGET:=prod}"

# 0) Ensure PATH contains venv bin (extra safety)
export PATH="/app/.venv/bin:${PATH}"

# 1) Ensure DuckDB dir exists
mkdir -p "$(dirname "${DUCKDB_PATH}")"

# 2) Render profiles and verify
log "render profiles"
/usr/local/bin/render_profiles.sh
test -f "${DBT_PROFILES_DIR}/profiles.yml" || { log "profiles.yml not found at ${DBT_PROFILES_DIR}"; exit 1; }

# 3) Resolve dbt binary (absolute)
DBT_BIN="/app/.venv/bin/dbt"
if [ ! -x "${DBT_BIN}" ]; then
  log "dbt binary not found at ${DBT_BIN}"
  command -v dbt || true
  exit 127
fi
log "dbt version: $("${DBT_BIN}" --version | head -n1)"

# 4) Pick command: arg > env > default
RAW_CMD="${1-}"
if [ -z "${RAW_CMD}" ]; then
  RAW_CMD="${DBT_CMD-}"
fi
if [ -z "${RAW_CMD}" ]; then
  RAW_CMD="dbt build"
fi

# 5) Strip leading 'dbt ' if present and build args
#    So we always execute the absolute DBT_BIN
if [[ "${RAW_CMD}" == dbt\ * ]]; then
  RAW_ARGS="${RAW_CMD#dbt }"
else
  RAW_ARGS="${RAW_CMD}"
fi

# 6) Ensure essential flags present (idempotent)
append_if_missing() { local flag="$1" value="$2"; grep -Eq "(^|[[:space:]])${flag}([[:space:]]|$)" <<<"${RAW_ARGS}" || RAW_ARGS+=" ${flag} ${value}"; }
append_if_missing "--project-dir"  "${DBT_PROJECT_DIR}"
append_if_missing "--profiles-dir" "${DBT_PROFILES_DIR}"
append_if_missing "--threads"      "${THREADS}"
append_if_missing "--target"       "${DBT_TARGET}"
grep -Eq "(^|[[:space:]])--fail-fast([[:space:]]|$)" <<<"${RAW_ARGS}" || RAW_ARGS+=" --fail-fast"

log "exec: ${DBT_BIN} ${RAW_ARGS}"
# 7) Exec with absolute dbt binary
exec bash -lc "${DBT_BIN} ${RAW_ARGS}"
