#!/usr/bin/env bash
set -euo pipefail

log() { printf '[runner] %s\n' "$*"; }

: "${DBT_PROJECT_DIR:=/app/project}"
: "${DBT_PROFILES_DIR:=/app/profiles}"
: "${DUCKDB_PATH:=/app/artifacts/ampere.duckdb}"
: "${THREADS:=4}"
: "${DBT_TARGET:=prod}"
# Optional selector (e.g. orders_flow)
: "${DBT_SELECTOR:=}"

# Make sure DuckDB dir exists
mkdir -p "$(dirname "${DUCKDB_PATH}")"

# Render profiles and show the result
log "render profiles"
if ! /usr/local/bin/render_profiles.sh; then
  log "render_profiles.sh failed"; exit 1
fi
log "profiles at: ${DBT_PROFILES_DIR}"
test -f "${DBT_PROFILES_DIR}/profiles.yml" || { log "profiles.yml not found"; exit 1; }

# Ensure dbt binary exists
DBT_BIN="/app/.venv/bin/dbt"
if [ ! -x "${DBT_BIN}" ]; then
  log "dbt binary not found at ${DBT_BIN}"
  command -v dbt && dbt --version || true
  exit 127
fi
log "dbt version: $("${DBT_BIN}" --version | head -n1)"

# Build the command deterministically
CMD=( "${DBT_BIN}" "build" "--project-dir" "${DBT_PROJECT_DIR}" "--profiles-dir" "${DBT_PROFILES_DIR}" "--fail-fast" "--threads" "${THREADS}" "--target" "${DBT_TARGET}" )
if [ -n "${DBT_SELECTOR}" ]; then
  CMD+=( "--selector" "${DBT_SELECTOR}" )
fi

log "exec: ${CMD[*]}"
exec "${CMD[@]}"
