#!/usr/bin/env bash
set -euo pipefail

: "${DBT_PROFILES_DIR:=/app/profiles}"
: "${DBT_TARGET:=prod}"
: "${THREADS:=4}"
: "${DUCKDB_PATH:=/app/artifacts/ampere.duckdb}"
: "${DUCKDB_MEMORY_LIMIT:=7GB}"
: "${DUCKDB_TEMP_DIRECTORY:=/app/artifacts/duckdb_tmp}"
: "${MINIO_S3_ENDPOINT:=minio.ampere.svc.cluster.local:9000}"
: "${MINIO_S3_USE_SSL:=false}"
: "${SILVER_EXTERNAL_ROOT:=s3://ampere-silver/silver}"

RAW_ENDPOINT="${MINIO_S3_ENDPOINT}"
if [[ "${RAW_ENDPOINT}" == https://* ]]; then
  MINIO_S3_USE_SSL=true
fi
RAW_ENDPOINT="${RAW_ENDPOINT#http://}"
RAW_ENDPOINT="${RAW_ENDPOINT#https://}"

mkdir -p "${DBT_PROFILES_DIR}"
mkdir -p "${DUCKDB_TEMP_DIRECTORY}"
cat > "${DBT_PROFILES_DIR}/profiles.yml" <<YAML
ampere_duckdb_project:
  target: ${DBT_TARGET}
  outputs:
    ${DBT_TARGET}:
      type: duckdb
      threads: ${THREADS}
      path: ${DUCKDB_PATH}
      extensions:
        - "httpfs"
        - "parquet"
        - "json"
        - "aws"
        - "delta"
      config_options:
        memory_limit: "${DUCKDB_MEMORY_LIMIT}"
        temp_directory: "${DUCKDB_TEMP_DIRECTORY}"
        preserve_insertion_order: false
      settings:
        s3_region: "us-east-1"
        s3_url_style: "path"
        s3_endpoint: "${RAW_ENDPOINT}"
        s3_use_ssl: ${MINIO_S3_USE_SSL}
        s3_access_key_id: "${MINIO_ACCESS_KEY:-}"
        s3_secret_access_key: "${MINIO_SECRET_KEY:-}"
      external_root: "${SILVER_EXTERNAL_ROOT}"
YAML
