# Ampere dbt Project

This folder is the shared dbt authoring project for Ampere Silver and Gold modeling layers.

Two execution flows are supported.

## 1. Cluster Flow (target runtime)

Used by the shared `docker/dbt` image and cluster orchestration.

- Profile source: generated in container by `docker/dbt/render_profiles.sh`
- Project dir: `/app/dbt`
- Profiles dir: `/app/profiles`
- Target: `prod`

## 2. Local Dev Flow (Windows repo)

Used on this machine from repository checkout.

- Profile source: `./dbt_profiles/profiles.yml`
- Project dir: `./dbt`
- Profiles dir: `./dbt_profiles`
- Target: `dev`

Local development uses a workspace DuckDB file under `dbt/.dbt_local/`, the checked-in dbt project under `./dbt`, and local profiles under `./dbt_profiles`.

## Runtime Contract

- Bronze sources in `models/staging/_sources.yml` are resolved to runtime-created `bronze.<table>` views over `delta_scan(...)` locations fetched live from Unity Catalog metadata.
- Silver sources used by Gold are resolved to runtime-created `silver.<table>` views over `delta_scan(...)` locations fetched live from Unity Catalog metadata.
- The container entrypoint creates short-lived source mapping JSON from UC before dbt starts; local runs should do the same with `docker/dbt/scripts/create_uc_source_mapping.py`.
- View definitions are created in `on-run-start` by `ampere_prepare_bronze_sources()` and `ampere_prepare_silver_sources()`.
- Silver model SQL remains path-agnostic and uses `source()` only.
- Gold model SQL reads published Silver through `source('silver', ...)`; it does not use same-run `ref()` sources.
- Large transactional staging models are materialized as DuckDB tables so tests, intermediate rollups, and fact models reuse one prepared relation instead of repeatedly scanning bronze Delta files.
- `stg_order_product` uses a latest-row window deduplication and avoids the memory-heavy hash aggregate path.
- `int_order_value_rollup` and `int_orders_latest_status` are materialized as tables because they are shared by facts and tests.
- Required runtime variables:
  - `BRONZE_SOURCE_NAME` (default `bronze`)
  - `BRONZE_SOURCE_SCHEMA` (default `bronze`)
  - `BRONZE_UC_CATALOG` (default `ampere`)
  - `BRONZE_UC_SCHEMA` (default `bronze`)
  - `SILVER_UC_SCHEMA` (default `silver`)
  - `UC_API_URI`
  - `UC_TOKEN`
  - `BRONZE_SOURCE_MAPPING_PATH`
  - `SILVER_SOURCE_MAPPING_PATH`
