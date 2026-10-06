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
- Daily staging selects every event for orders touched in the lookback window. The Delta publisher merges partitioned Silver and Gold facts by business key, retaining unrelated rows on the same business date. A full rebuild republishes all partitions and repairs history lost by earlier daily partition replacement.
- The manual Delta full rebuild processes 14-day windows, publishing all Silver windows before Gold. Each window starts a fresh dbt process and DuckDB file. Progress is checkpointed under `ampere-silver-ops/dbt/rebuild_checkpoints/`, so clearing a failed Airflow task resumes at the failed window. Its first publish replaces old output; later windows merge facts by business key. `silver_full_rebuild_start_date`, `silver_full_rebuild_window_days`, and `silver_full_rebuild_chunk_duckdb_memory_limit` control the rebuild (defaults `2025-12-01`, `14`, and `5GB`). The pod remains capped at 10 GiB.
- During the bounded Gold phase, `dim_delivery_cost` selects orders for the current window and merges by `order_id`. The ordinary daily Gold path still rebuilds that complete helper table. Pause Delta Bronze ingestion, Delta Silver/Gold daily jobs, and downstream Delta cache refresh until the full rebuild succeeds. This keeps the Bronze input stable; published tables are partial while windows are being processed.
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
