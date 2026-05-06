# Layer responsibilities

| Layer | Status | Engine | Storage | Catalog | Responsibility |
|---|---|---|---|---|---|
| Python generator + PostgreSQL | implemented | Python, PostgreSQL | PostgreSQL | - | Synthetic operational source data generation and storage. |
| Raw landing | implemented | Spark | MinIO, Parquet | - | Immutable parquet landing batches with manifest, success markers, and extraction state. |
| Bronze | implemented | Spark, Delta Lake | MinIO, Delta Lake | Unity Catalog OSS / ampere.bronze | Apply raw landing batches into Unity Catalog registered Delta tables. |
| Silver | implemented | DuckDB, dbt, Delta Lake | MinIO, Delta Lake | Unity Catalog OSS / ampere.silver | Clean analytical dbt models built from Bronze through the Unity Catalog metadata bridge. |
| Gold | implemented | DuckDB, dbt, Delta Lake | MinIO, Delta Lake | Unity Catalog OSS / ampere.gold | Serving-oriented marts for BI and downstream consumers, built from Silver in the shared dbt runtime. |
| Serving / BI | planned | BI / serving | - | - | Dashboard and downstream serving consumption from Gold marts. |
