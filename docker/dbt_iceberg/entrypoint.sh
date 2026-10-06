#!/usr/bin/env bash
set -euo pipefail

python /app/prepare_catalog.py

if [ "$#" -eq 0 ]; then
  set -- build
fi
dbt "$@" --project-dir "${DBT_PROJECT_DIR}" --profiles-dir "${DBT_PROFILES_DIR}" --threads "${DBT_THREADS:-2}"

if [[ "${ICEBERG_PUBLISH_MODE:-direct}" == "staged" ]]; then
  python /app/publish_catalog.py
fi
