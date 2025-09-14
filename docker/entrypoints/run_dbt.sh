#!/usr/bin/env bash
set -euo pipefail
# Default command can be overridden via env (e.g. DBT_CMD="dbt build --selector processing")

: "${DBT_CMD:=dbt build}"
: "${DBT_STATE_DIR:=}"

# Render profiles from env
/usr/local/bin/render_profiles.sh

. /app/.venv/bin/activate

if [ -n "${DBT_STATE_DIR}" ]; then
  exec ${DBT_CMD} --project-dir /app/project --profiles-dir /app/profiles --state "${DBT_STATE_DIR}"
else
  exec ${DBT_CMD} --project-dir /app/project --profiles-dir /app/profiles
fi
