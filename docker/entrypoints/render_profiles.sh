#!/usr/bin/env bash
set -euo pipefail
# Render dbt profiles.yml from environment variables

: "${DBT_TARGET:=prod}"
: "${THREADS:=4}"
: "${DUCKDB_PATH:=/app/artifacts/ampere.duckdb}"

mkdir -p /app/profiles
cat > /app/profiles/profiles.yml <<YAML
ampere_duckdb_project:
  target: ${DBT_TARGET}
  outputs:
    prod:
      type: duckdb
      threads: ${THREADS}
      path: ${DUCKDB_PATH}
YAML
