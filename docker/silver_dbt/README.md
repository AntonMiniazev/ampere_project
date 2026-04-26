# silver_dbt runtime

This directory contains the dedicated runtime package for the silver dbt + DuckDB layer.

Responsibilities:
- install dbt runtime dependencies;
- package the repo-root `dbt/` project into the image;
- render `profiles.yml` from runtime environment variables;
- generate and validate bronze source mapping from UC metadata before dbt runs;
- persist dbt artifacts to the silver ops bucket;
- provide the entrypoint used later by Airflow and GitHub Actions image builds.

Build from repo root:

```bash
docker build -f docker/silver_dbt/Dockerfile -t ampere-silver-dbt .
```

Run example:

```bash
docker run --rm \
  -e MINIO_ACCESS_KEY=<your-minio-access-key> \
  -e MINIO_SECRET_KEY=<your-minio-secret-key> \
  -e MINIO_S3_ENDPOINT=minio.ampere.svc.cluster.local:9000 \
  -e UC_API_URI=http://unity-catalog-unitycatalog-server.unity-catalog.svc.cluster.local:8080 \
  -e UC_TOKEN=<your-uc-token-or-not-used> \
  -e BRONZE_UC_CATALOG=ampere \
  -e BRONZE_UC_SCHEMA=bronze \
  -e BRONZE_SOURCE_NAME=bronze \
  -e BRONZE_SOURCE_SCHEMA=bronze \
  -e BRONZE_SOURCE_MAPPING_PATH=/app/artifacts/bronze_source_mapping.json \
  -e BRONZE_SOURCE_MAPPING_MAX_AGE_HOURS=24 \
  -e DUCKDB_MEMORY_LIMIT=7GB \
  -e DUCKDB_TEMP_DIRECTORY=/app/artifacts/duckdb_tmp \
  -e SILVER_EXTERNAL_ROOT=s3://ampere-silver/silver \
  ampere-silver-dbt \
  "dbt build --select stg_clients"
```

Default runtime contract:
- dbt project path: `/app/dbt`
- generated profiles path: `/app/profiles`
- local DuckDB path: `/app/artifacts/ampere.duckdb`
- DuckDB memory cap: `DUCKDB_MEMORY_LIMIT`, default `7GB`
- DuckDB spill directory: `DUCKDB_TEMP_DIRECTORY`, default `/app/artifacts/duckdb_tmp`
- bundled helper scripts path: `/app/scripts`
- bronze source access is path-agnostic in SQL (`source()` only) and resolved at runtime via generated mapping views.

This image scaffold assumes the silver authoring project lives in the repo-root `dbt/` folder.

Runtime sequence in entrypoint:
1. render profiles (`render_profiles.sh`);
2. generate UC bronze metadata mapping (`generate_bronze_source_mapping.py`);
3. validate mapping coverage/freshness + optional `delta_scan` smoke (`validate_bronze_sources.py`);
4. run dbt command.

Mapping and preflight helpers:

```bash
python /app/scripts/generate_bronze_source_mapping.py \
  --project-dir /app/dbt \
  --output /app/artifacts/bronze_source_mapping.json

python /app/scripts/validate_bronze_sources.py \
  --project-dir /app/dbt \
  --mapping-path /app/artifacts/bronze_source_mapping.json \
  --max-age-hours 24

# Faster smoke when object storage probe is not required:
python /app/scripts/validate_bronze_sources.py \
  --project-dir /app/dbt \
  --mapping-path /app/artifacts/bronze_source_mapping.json \
  --skip-delta-scan

# Unit smoke for mapping helper logic:
python /app/scripts/test_bronze_source_mapping.py
python /app/scripts/upload_dbt_artifacts.py --artifacts-dir /app/dbt/target
```

Fallback behavior:
- if UC metadata cannot be fetched, entrypoint fails before dbt starts;
- if mapping is missing required tables, duplicated, or stale, entrypoint fails before dbt starts;
- if `RUN_UC_MAPPING_GENERATION=false`, an existing mapping at `BRONZE_SOURCE_MAPPING_PATH` can be reused, but freshness checks still apply;
- if `RUN_BRONZE_PREFLIGHT_DELTA_SCAN=false`, only mapping structure/freshness is validated (no storage read probe).
