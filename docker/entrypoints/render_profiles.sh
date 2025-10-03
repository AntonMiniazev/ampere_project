#!/usr/bin/env bash
set -euo pipefail

# Prefer in-cluster Service DNS; allow override via env
RAW_ENDPOINT="${MINIO_S3_ENDPOINT:-minio.ampere.svc.cluster.local:9000}"
# Strip scheme if someone passes http(s):// by mistake
RAW_ENDPOINT="${RAW_ENDPOINT#http://}"
RAW_ENDPOINT="${RAW_ENDPOINT#https://}"

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
      extensions: ["httpfs", "parquet", "aws"]
      settings:
        s3_region: "us-east-1"
        s3_url_style: "path"
        s3_endpoint: "${RAW_ENDPOINT}"   # host:port, no scheme
        s3_use_ssl: false
        s3_access_key_id: "${MINIO_ACCESS_KEY:-}"
        s3_secret_access_key: "${MINIO_SECRET_KEY:-}"
      external_root: "s3://ampere-prod-silver"
YAML
