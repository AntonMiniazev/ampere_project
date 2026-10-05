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
- bronze and published-silver source access is path-agnostic in SQL (`source()` only) and resolved at runtime via `delta_scan(...)` views created from live Unity Catalog metadata.
- source mapping artifacts: `BRONZE_SOURCE_MAPPING_PATH` and `SILVER_SOURCE_MAPPING_PATH`, defaulting to `/app/artifacts/*.json`.
- DuckDB reads MinIO through the configured S3 secret; the direct attached UC scan path is not used until the published UC extension can propagate MinIO endpoint settings.

DuckDB memory and spill-directory values are rendered as connection-time `config_options`. They must be applied before extensions or queries touch temporary storage.

Airflow full rebuild defaults use moderate parallelism while keeping node4 scheduling practical: `silver_full_rebuild_dbt_threads=2`, `silver_full_rebuild_duckdb_memory_limit=6GB`, `silver_full_rebuild_dbt_memory_request=5Gi`, and `silver_full_rebuild_dbt_memory_limit=10Gi`. Kubernetes schedules the pod from the memory request, while the memory limit remains the maximum runtime allowance.

This image assumes the shared dbt authoring project lives in the repo-root
`dbt/` folder. Layer-specific behavior is selected by Airflow DAG command,
selectors, and environment variables.

Runtime sequence in entrypoint:
1. render profiles (`render_profiles.sh`);
2. create fresh Bronze/Silver source mappings from Unity Catalog metadata.
3. run the primary dbt command; dbt `on-run-start` creates `delta_scan(...)` source views from those mappings.
4. publish `publish`-tagged Silver models to the Silver external root as Delta tables, then validate/register them when `RUN_SILVER_UC_REGISTRATION=true`.
5. when `GOLD_DBT_COMMAND` is set, run it only after the Silver publish/registration step. The entrypoint requires the primary command to select `tag:silver`, the Gold command to select `tag:gold`, and both publish switches to be enabled. This ordering ensures Gold's `source('silver', ...)` relations see the latest published Silver snapshot.
6. publish `publish`-tagged Gold tables and validate/register them when enabled.
7. upload layer artifacts and publish manifests to `SILVER_DBT_ARTIFACT_ROOT` and `GOLD_DBT_ARTIFACT_ROOT`.

Partitioned models are written one date partition at a time to keep Arrow and
Delta writer memory bounded. Daily publishing replaces only the partitions
present in the current dbt result, retaining older partitions. If daily mode
finds a missing partitioned Delta table, publishing bootstraps it with a full
rebuild for that run.

The Delta daily combined DAG uses two commands: `silver_daily_dbt_command`
(default `dbt build --select tag:silver`) and `gold_daily_dbt_command` (default
`dbt build --select tag:gold`). The full-rebuild DAG uses
`silver_full_rebuild_silver_dbt_command` and `gold_full_rebuild_dbt_command`,
each defaulting to its layer selector plus `--full-refresh`.
If custom values were set under `silver_daily_with_gold_dbt_command`,
`silver_dbt_command`, or `silver_full_rebuild_dbt_command`, migrate them to the
new Silver-specific variables; these old names are no longer read by the daily
or full-rebuild Silver/Gold DAGs.

Gold runs use this same image and call dbt with gold selectors. Gold publish,
UC validation, and artifact upload use the same shared runtime scripts with
layer-specific roots.

Fallback behavior:
- if UC metadata mapping generation fails, the entrypoint stops before dbt starts;
- if a mapped Delta location cannot be read through DuckDB S3 settings, dbt fails during source view preparation or first model read;
- if `RUN_SILVER_PUBLISH=false`, dbt tables remain local to the runtime DuckDB file and are not copied to MinIO.
- if `RUN_SILVER_UC_REGISTRATION=false`, Delta tables are published but UC pre-publish validation and post-publish location checks are skipped.
- if `RUN_DBT_ARTIFACT_UPLOAD=false`, dbt artifacts remain local to the runtime container.
