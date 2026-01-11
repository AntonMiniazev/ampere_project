# Ampere Project Overview

Ampere is a homelab data platform that generates synthetic operational data, lands it in Postgres, moves it into a lakehouse (MinIO + Spark + Delta), and publishes curated marts for BI. The end state is a data engineering stack running on a small K8s cluster.

Combination of services based on **_practice-first_** focus rather than actual project need.

Current focus: finishing raw landing refactoring, then re-building Bronze/Silver/Gold pipelines and operational controls.

## Project Structure
- dags/ — Airflow DAGs and SparkApplication templates.
- docker/ — container images (generators, Spark ETL).
- dbt/ — dbt models and tests [to be refactored].
- project_images/ — diagrams [outdated].

## Architecture Summary
1) Pre-raw generators -> PostgreSQL (source schema)
2) Source -> Raw landing (Spark -> Parquet in MinIO)
3) Raw -> Bronze Delta (Spark)
4) Bronze -> Silver Delta (Spark)
5) Silver -> Gold marts (DuckDB + dbt -> PostgreSQL)

Deployment reference: follow the completed infra runbook from https://github.com/AntonMiniazev/bohr_project.

## Execution Plan and Status

### 0) Platform and cluster
- [x] K8s cluster (control plane + workers)
- [x] Airflow, MinIO, Postgres, Spark Operator, monitoring stack
- [ ] Delta Lake OSS runtime config
- [ ] Networking polish (ingress, TLS, hostname routing)

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
- [ ] Spark jobs for deduplication, type normalization, and schema evolution
- [ ] Delta Lake tables in MinIO
- [ ] Airflow DAG for bronze load and backfill

### 4) Bronze -> Silver Delta
- [ ] Conformed dimensions + SCD handling
- [ ] Airflow DAG for silver load and backfill

### 5) Silver -> Gold marts (PostgreSQL)
- [ ] DuckDB/dbt models for business logic
- [ ] Airflow DAGs for dbt runs + exports
- [ ] Backups and retention

### 6) Observability and Data Quality
- [ ] dbt artifacts (manifest/run_results)
- [ ] DQ checks / contracts
- [ ] Pipeline status dashboard

---

<p align="center">
  <img src="https://github.com/AntonMiniazev/ampere_project/blob/master/project_images/high_level_structure.svg" />
</p>
