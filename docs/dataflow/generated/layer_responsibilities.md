# Layer responsibilities

| Layer | Status | Engine | Storage | Catalog | Responsibility |
|---|---|---|---|---|---|
| Python generator + PostgreSQL | implemented | Python, PostgreSQL | PostgreSQL | - | Generate synthetic operational data in PostgreSQL. |
| Raw landing | implemented | Spark | MinIO, Parquet | - | Extract immutable Parquet batches with manifests, success markers, and extraction state. |
| Bronze | implemented | Spark, Iceberg | MinIO, Iceberg | Lakekeeper / bronze warehouse / bronze and ops namespaces | Apply Raw batches to Iceberg tables and track processed batches in an Iceberg registry. |
| Silver | implemented | DuckDB, dbt, Iceberg | MinIO, Iceberg | Lakekeeper / silver warehouse / silver namespace | Build tested analytical entities from Bronze with dbt and DuckDB. |
| Gold | implemented | DuckDB, dbt, Iceberg | MinIO, Iceberg | Lakekeeper / gold warehouse / gold namespace | Publish serving marts for sales, delivery, product cost, and margin. |
| Serving / BI | implemented | Curie | Cache | - | Refresh Curie caches from Gold marts for application and dashboard reads. |
