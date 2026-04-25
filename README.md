# Ampere Project Overview

Ampere is a homelab data platform that generates synthetic operational data, lands it in Postgres, moves it into a Delta Lake (MinIO + Spark + Delta), and publishes curated marts for BI. The end state is a data engineering stack running on a small K8s cluster.

Combination of services based on **_practice-first_** focus rather than actual project need.

Current focus: stabilizing raw landing, keeping bronze resource tuning healthy, and finishing silver runtime publication/export steps.

## Project Structure
- dags/ — Airflow DAGs and SparkApplication templates.
- docker/ — container images (generators, Spark ETL, silver dbt runtime).
- dbt/ — active silver dbt models and tests.
- project_images/ — diagrams [outdated].
- .github/workflows/ — CI image build and release-tag workflows.

## Architecture Summary
1) Pre-raw generators -> PostgreSQL (source schema)
2) Source -> Raw landing (Spark -> Parquet in MinIO)
3) Raw -> Bronze Delta (Spark)
4) Bronze -> Silver (DuckDB + dbt via UC metadata bridge)
5) Silver -> Gold marts (DuckDB + dbt -> PostgreSQL)

Deployment reference: follow the completed infra runbook from https://github.com/AntonMiniazev/bohr_project.

## Execution Plan and Status

### 0) Platform and cluster
- [x] K8s cluster (control plane + workers)
- [x] Airflow, MinIO, Postgres, Spark Operator, monitoring stack
- [ ] Delta Lake config
- [x] Networking polish (ingress, TLS, hostname routing)

### 1) Pre-raw generation (Postgres)
- [x] Generator code + Docker images
- [x] Init DAG: dags/my_dags/ampere__pre_raw__generators__init.py
- [x] Daily DAG: dags/my_dags/ampere__pre_raw__generators__daily.py
- [x] Postgres schema + indexes

### 2) Source -> Raw landing (MinIO)
- [x] Spark ETL image: docker/spark/raw_etl
- [x] SparkApplication template: dags/sparkapplications/source_to_raw_template.yaml
- [x] DAG: dags/my_dags/ampere__raw_landing__postgres_to_landing__daily.py
- [x] _manifest.json + _SUCCESS
- [x] State-based watermarks for mutable dims (B2)
- [ ] Operational tuning (resources, retries, SLA)

### 3) Raw -> Bronze Delta
- [x] Delta Lake tables in MinIO
- [x] Airflow DAG for bronze load
- [x] Bronze triggers silver daily DAG on success

### 4) Bronze -> Silver
- [x] DuckDB/dbt silver runtime: docker/silver_dbt
- [x] UC metadata bridge for bronze source resolution
- [x] Airflow DAG: dags/my_dags/ampere__silver__dbt_duckdb__daily.py
- [x] Bronze -> silver trigger wiring
- [ ] Cluster-ready silver runtime image publication
- [ ] Publish silver outputs to ampere.silver
- [ ] Persist silver audit/artifact outputs

### 5) Silver -> Gold marts (PostgreSQL)
- [ ] Export silver outputs into PostgreSQL-serving gold layer
- [ ] Gold orchestration DAGs
- [ ] Backups and retention

### 6) Observability and Data Quality
- [ ] Persist dbt artifacts (manifest/run_results)
- [x] dbt tests and silver model contracts in project
- [ ] Pipeline status dashboard

## Image Versioning Workflow

Runtime image selection is tied to one Airflow variable:
- `ampere_release_version`

Default image resolution:
- `ghcr.io/antonminiazev/init-source-preparation:<ampere_release_version>`
- `ghcr.io/antonminiazev/order-data-generator:<ampere_release_version>`
- `ghcr.io/antonminiazev/ampere-spark:<ampere_release_version>`
- `ghcr.io/antonminiazev/ampere-silver-dbt:<ampere_release_version>`

---

<p align="center">
  <img src="https://github.com/AntonMiniazev/ampere_project/blob/master/project_images/high_level_structure.svg" />
</p>
