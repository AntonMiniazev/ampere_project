#!/usr/bin/env bash
set -euo pipefail
# comments in English only

: "${DBT_CMD:=dbt build}"
: "${DBT_STATE_DIR:=}"
: "${DUCKDB_PATH:=/app/artifacts/ampere.duckdb}"

# ensure parent dir for duckdb file exists
mkdir -p "$(dirname "${DUCKDB_PATH}")"

# render profiles with normalized S3 endpoint
/usr/local/bin/render_profiles.sh

# activate venv and run
. /app/.venv/bin/activate

if [ -n "${DBT_STATE_DIR}" ]; then
  exec ${DBT_CMD} --project-dir /app/project --profiles-dir /app/profiles --state "${DBT_STATE_DIR}"
else
  exec ${DBT_CMD} --project-dir /app/project --profiles-dir /app/profiles
fi
