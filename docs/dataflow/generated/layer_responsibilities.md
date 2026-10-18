# Layer responsibilities

| Layer | Status | Engine | Storage | Catalog | Responsibility |
|---|---|---|---|---|---|
| Python generator + PostgreSQL | implemented | Python, PostgreSQL | PostgreSQL | - | Generate current operational data in PostgreSQL and capture exact mutation-time event images as bounded immutable Parquet batches in MinIO. |
| Raw Iceberg change history | implemented | Spark | MinIO, Iceberg | Lakekeeper / raw warehouse / raw namespace | Validate committed PostgreSQL batch controls and MinIO Parquet manifests, then merge typed event rows into Raw Iceberg with durable per-table acknowledgments. |
| Bronze | implemented | Spark, Iceberg | MinIO, Iceberg | Lakekeeper / bronze warehouse / bronze and ops namespaces | Maintain current state from accepted Raw changes; daily apply resumes one frozen transfer across two sequential Spark groups, using versioned merges, while full rebuild publishes each reconstructed table in one atomic overwrite commit. |
| Silver | implemented | DuckDB, dbt, Iceberg | MinIO, Iceberg | Lakekeeper / silver warehouse / silver namespace | Scope Raw deltas to the active dataset and checkpoint, rebuild complete affected orders, and persist retry-safe month invalidations before Silver publication. |
| Gold | implemented | DuckDB, dbt, Iceberg | MinIO, Iceberg | Lakekeeper / gold warehouse / gold namespace | Recompute complete affected months for eight Curie aggregates, resolve delivery cost once per order using identical tariffs across its store's zones for the courier type, and retain missing inputs as NULL. |
| Serving / BI | implemented | Curie | Cache | - | Refresh Curie caches from Gold marts for application and dashboard reads. |
