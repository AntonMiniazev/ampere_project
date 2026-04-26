# Silver dbt Project

This folder is the single dbt authoring project for silver layer transformations.

Two execution flows are supported.

## 1. Cluster Flow (target runtime)

Used by `docker/silver_dbt` image and cluster orchestration.

- Profile source: generated in container by `docker/silver_dbt/render_profiles.sh`
- Project dir: `/app/dbt`
- Profiles dir: `/app/profiles`
- Target: `prod`

Typical command inside container:

```bash
dbt build --project-dir /app/dbt --profiles-dir /app/profiles --target prod
```

## 2. Local Dev Flow (Windows repo)

Used on this machine from repository checkout.

- Profile source: `./dbt_profiles/profiles.yml`
- Project dir: `./dbt`
- Profiles dir: `./dbt_profiles`
- Target: `dev`

Use this sequence:

```bash
# PowerShell (Windows): point DuckDB to local workspace file.
$env:DUCKDB_PATH="dbt/.dbt_local/ampere.duckdb"

python ./docker/silver_dbt/scripts/generate_bronze_source_mapping.py \
  --project-dir ./dbt \
  --output ./dbt/.dbt_local/bronze_source_mapping.json \
  --uc-api-uri http://ucatalog.local \
  --catalog ampere \
  --schema bronze

python ./docker/silver_dbt/scripts/validate_bronze_sources.py \
  --project-dir ./dbt \
  --mapping-path ./dbt/.dbt_local/bronze_source_mapping.json \
  --max-age-hours 24

# Optional lightweight smoke:
python ./docker/silver_dbt/scripts/test_bronze_source_mapping.py

dbt parse --project-dir ./dbt --profiles-dir ./dbt_profiles --no-partial-parse
dbt show --select stg_stores --indirect-selection empty --project-dir ./dbt --profiles-dir ./dbt_profiles --no-partial-parse
dbt build --selector silver_staging --project-dir ./dbt --profiles-dir ./dbt_profiles --no-partial-parse
```

## Runtime Contract

- Bronze sources in `models/staging/_sources.yml` are resolved to runtime-created `bronze.<table>` views.
- View definitions are created in `on-run-start` by `ampere_prepare_bronze_sources()` from generated UC metadata mapping.
- Silver model SQL remains path-agnostic and uses `source()` only.
- Large transactional staging models are materialized as DuckDB tables so tests, intermediate rollups, and fact models reuse one prepared relation instead of repeatedly scanning bronze Delta files.
- `stg_order_product` uses a latest-row window deduplication and avoids the memory-heavy hash aggregate path.
- `int_order_value_rollup` and `int_orders_latest_status` are materialized as tables because they are shared by facts and tests.
- Required runtime variables:
  - `BRONZE_SOURCE_NAME` (default `bronze`)
  - `BRONZE_SOURCE_SCHEMA` (default `bronze`)
  - `BRONZE_UC_CATALOG` (default `ampere`)
  - `BRONZE_UC_SCHEMA` (default `bronze`)
  - `UC_API_URI`
  - `UC_TOKEN`
  - `BRONZE_SOURCE_MAPPING_PATH` (default `./dbt/.dbt_local/bronze_source_mapping.json` in local flow)
  - `BRONZE_SOURCE_MAPPING_MAX_AGE_HOURS` (default `24`)
