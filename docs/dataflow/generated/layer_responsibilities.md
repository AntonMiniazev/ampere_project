# Layer responsibilities

| Layer | Status | Engine | Storage | Catalog | Responsibility |
|---|---|---|---|---|---|
| Python generator + PostgreSQL | implemented | Python, PostgreSQL | PostgreSQL | - | Generate synthetic operational data in PostgreSQL. |
| Raw Iceberg change history | implemented | Spark | MinIO, Iceberg | Lakekeeper / raw warehouse / raw namespace | Join compact PostgreSQL change keys to source after-images and merge typed change rows into Raw Iceberg with durable per-table acknowledgments. |
| Bronze | implemented | Spark, Iceberg | MinIO, Iceberg | Lakekeeper / bronze warehouse / bronze and ops namespaces | Reconstruct and incrementally maintain current source state from accepted Raw change rows, with dataset-scoped row identity and checkpoints. |
| Silver | implemented | DuckDB, dbt, Iceberg | MinIO, Iceberg | Lakekeeper / silver warehouse / silver namespace | Build tested analytical entities from Bronze with dbt and DuckDB. |
| Gold | implemented | DuckDB, dbt, Iceberg | MinIO, Iceberg | Lakekeeper / gold warehouse / gold namespace | Publish eight monthly Curie report aggregates for Marketing, Financial, and Delivery. |
| Serving / BI | implemented | Curie | Cache | - | Refresh Curie caches from Gold marts for application and dashboard reads. |
