# silver_dbt runtime

This directory contains the dedicated runtime package for the silver dbt + DuckDB layer.

Responsibilities:
- install dbt runtime dependencies;
- package the repo-root `dbt/` project into the image;
- render `profiles.yml` from runtime environment variables;
- generate and validate bronze source mapping from UC metadata before dbt runs;
- validate `publish`-tagged silver model schemas against Unity Catalog;
- publish validated silver tables as Delta tables to the durable silver bucket;
- validate published silver Delta tables against Unity Catalog;
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
  -e DUCKDB_MEMORY_LIMIT=5GB \
  -e DUCKDB_TEMP_DIRECTORY=/app/artifacts/duckdb_tmp \
  -e SILVER_EXTERNAL_ROOT=s3://ampere-silver/silver \
  -e SILVER_DBT_ARTIFACT_ROOT=s3://ampere-silver-ops/dbt \
  ampere-silver-dbt \
  "dbt build --select stg_clients"
```

Default runtime contract:
- dbt project path: `/app/dbt`
- generated profiles path: `/app/profiles`
- local DuckDB path: `/app/artifacts/ampere.duckdb`
- DuckDB memory cap: `DUCKDB_MEMORY_LIMIT`, default `7GB` for daily runs and `5GB` for the Airflow full rebuild DAG
- DuckDB spill directory: `DUCKDB_TEMP_DIRECTORY`, default `/app/artifacts/duckdb_tmp`
- durable silver table root: `SILVER_EXTERNAL_ROOT`, default `s3://ampere-silver/silver`
- dbt artifact root: `SILVER_DBT_ARTIFACT_ROOT`, default `s3://ampere-silver-ops/dbt`
- silver run mode: `SILVER_RUN_MODE`, default `daily_refresh`; `full_rebuild` disables lookback filtering
- daily lookback window: `SILVER_LOOKBACK_DAYS`, default `7`
- silver UC registration: `RUN_SILVER_UC_REGISTRATION`, default `true`
- bundled helper scripts path: `/app/scripts`
- bronze source access is path-agnostic in SQL (`source()` only) and resolved at runtime via generated mapping views.

DuckDB memory and spill-directory values are rendered as connection-time `config_options`. They must be applied before extensions or queries touch temporary storage.

Airflow full rebuild defaults are intentionally more conservative than daily refresh: `silver_full_rebuild_dbt_threads=1`, `silver_full_rebuild_duckdb_memory_limit=5GB`, `silver_full_rebuild_dbt_memory_request=8Gi`, and `silver_full_rebuild_dbt_memory_limit=10Gi`. Kubernetes schedules the pod from the memory request, while the memory limit remains the maximum runtime allowance.

This image scaffold assumes the silver authoring project lives in the repo-root `dbt/` folder.

Runtime sequence in entrypoint:
1. render profiles (`render_profiles.sh`);
2. generate UC bronze metadata mapping (`generate_bronze_source_mapping.py`);
3. validate mapping coverage/freshness + optional `delta_scan` smoke (`validate_bronze_sources.py`);
4. run dbt command.
5. validate each planned published table against existing UC metadata when `RUN_SILVER_UC_REGISTRATION=true`.
6. publish `publish`-tagged model tables to `SILVER_EXTERNAL_ROOT` as Delta tables; dimensions are replaced, fact/event tables are written one date partition at a time to keep Arrow and Delta writer memory bounded.
7. validate published silver Delta tables against existing UC metadata when `RUN_SILVER_UC_REGISTRATION=true`.
8. upload dbt artifacts to `SILVER_DBT_ARTIFACT_ROOT`.

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
- if `RUN_SILVER_PUBLISH=false`, dbt tables remain local to the runtime DuckDB file and are not copied to MinIO.
- if `RUN_SILVER_UC_REGISTRATION=false`, Delta tables are published but UC metadata validation is skipped.
- if `RUN_DBT_ARTIFACT_UPLOAD=false`, dbt artifacts remain local to the runtime container.
