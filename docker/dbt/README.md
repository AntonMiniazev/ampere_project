# shared dbt runtime

This directory contains the shared dbt + DuckDB runtime package for Ampere
modeling layers.

Current implemented layers:
- silver
- gold (orchestration baseline; model/publish rollout is incremental)

Responsibilities:
- install dbt runtime dependencies;
- package the repo-root `dbt/` project into the image;
- render `profiles.yml` from runtime environment variables;
- provide a single image build for silver and gold dbt jobs;
- run the requested dbt command with consistent DuckDB settings;
- run silver source preflight, publish, and artifact upload, while allowing
  gold dbt orchestration with layer-specific commands/selectors;
- provide the entrypoint used by Airflow and GitHub Actions image builds.

Default runtime contract:
- dbt project path: `/app/dbt`
- generated profiles path: `/app/profiles`
- local DuckDB path: `/app/artifacts/ampere.duckdb`
- DuckDB memory cap: `DUCKDB_MEMORY_LIMIT`, default `7GB` for daily runs and `5GB` for the Airflow full rebuild DAG
- DuckDB spill directory: `DUCKDB_TEMP_DIRECTORY`, default `/app/artifacts/duckdb_tmp`
- durable silver table root: `SILVER_EXTERNAL_ROOT`, default `s3://ampere-silver/silver`
- silver dbt artifact root: `SILVER_DBT_ARTIFACT_ROOT`, default `s3://ampere-silver-ops/dbt`
- gold dbt artifact root: `GOLD_DBT_ARTIFACT_ROOT`, default `s3://ampere-gold-ops/dbt`
- silver run mode: `SILVER_RUN_MODE`, default `daily_refresh`; `full_rebuild` disables lookback filtering
- daily lookback window: `SILVER_LOOKBACK_DAYS`, default `7`
- silver UC registration: `RUN_SILVER_UC_REGISTRATION`, default `true`
- bundled helper scripts path: `/app/scripts`
- bronze source access is path-agnostic in SQL (`source()` only) and resolved at runtime via generated mapping views.

DuckDB memory and spill-directory values are rendered as connection-time `config_options`. They must be applied before extensions or queries touch temporary storage.

Airflow full rebuild defaults use moderate parallelism while keeping node4 scheduling practical: `silver_full_rebuild_dbt_threads=2`, `silver_full_rebuild_duckdb_memory_limit=6GB`, `silver_full_rebuild_dbt_memory_request=5Gi`, and `silver_full_rebuild_dbt_memory_limit=10Gi`. Kubernetes schedules the pod from the memory request, while the memory limit remains the maximum runtime allowance.

This image assumes the shared dbt authoring project lives in the repo-root
`dbt/` folder. Layer-specific behavior is selected by Airflow DAG command,
selectors, and environment variables.

Runtime sequence in entrypoint:
1. render profiles (`render_profiles.sh`);
2. generate UC bronze metadata mapping (`generate_bronze_source_mapping.py`);
3. validate mapping coverage/freshness + optional `delta_scan` smoke (`validate_bronze_sources.py`);
4. run dbt command.
5. validate each planned published table against existing UC metadata when `RUN_SILVER_UC_REGISTRATION=true`.
6. publish `publish`-tagged model tables to the layer external root as Delta tables; replacement models are overwritten and partitioned models are written one date partition at a time to keep Arrow and Delta writer memory bounded. If daily mode finds a missing partitioned Delta table, the publish step automatically bootstraps that layer with full-rebuild publish mode for the run.
7. check published silver Delta locations are readable and match existing UC locations/formats when `RUN_SILVER_UC_REGISTRATION=true`.
8. upload dbt artifacts and `silver_publish_manifest.json` to `SILVER_DBT_ARTIFACT_ROOT` when silver publish is enabled.
9. upload dbt artifacts and `gold_publish_manifest.json` to `GOLD_DBT_ARTIFACT_ROOT` when gold publish is enabled.

Gold runs use this same image and call dbt with gold selectors. Gold publish,
UC validation, and artifact upload use the same shared runtime scripts with
layer-specific roots.

Mapping and preflight helpers are bundled under `/app/scripts`. They generate the Bronze source mapping, validate mapping coverage and freshness, optionally probe Delta storage readability, and upload dbt artifacts after a run.

Fallback behavior:
- if UC metadata cannot be fetched, entrypoint fails before dbt starts;
- if mapping is missing required tables, duplicated, or stale, entrypoint fails before dbt starts;
- if `RUN_UC_MAPPING_GENERATION=false`, an existing mapping at `BRONZE_SOURCE_MAPPING_PATH` can be reused, but freshness checks still apply;
- if `RUN_BRONZE_PREFLIGHT_DELTA_SCAN=false`, only mapping structure/freshness is validated (no storage read probe).
- if `RUN_SILVER_PUBLISH=false`, dbt tables remain local to the runtime DuckDB file and are not copied to MinIO.
- if `RUN_SILVER_UC_REGISTRATION=false`, Delta tables are published but UC pre-publish validation and post-publish location checks are skipped.
- if `RUN_DBT_ARTIFACT_UPLOAD=false`, dbt artifacts remain local to the runtime container.
