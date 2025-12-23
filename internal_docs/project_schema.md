# OSS “Databricks-like” Ampere project architecture

## Target dataflow
1) Pre-Raw data generated in docker image: [to_be_refactored]
   - Python scripts generate Pre-Raw data 
   - Pre-Raw data bulk insert into PostgreSQL (Source database)
2) Pre-Raw data transfered to MinIO storage (Spark to Delta Lake bronze layer) [to_be_refactored]
3) Bronze -> Silver transfer via Spark in Delta Lake (MinIO): deduplication, filtering invalid records, type normalization, conforming dimensions, schema evolution [to_be_implemented]
4) Silver -> Gold transfer via DuckDB + dbt, preparation of data marts in PostgreSQL for BI [to_be_refactored]

## Service List

### Backend architecture

1. Development environment:
Windows machine with devcontainers in VS Code

2. Home-lab KVM host (Ubuntu)
Ubuntu KVM/libvirt host

3. Home-lab Kubernetes VMs
KVM/libvirt VMs on Home-lab KVM host (control plane + workers)
 - PostgreSQL (ampere-k8s-node1)
 - MinIO (ampere-k8s-node2)
 - Airflow (ampere-k8s-node3)
 - KEDA
 - Delta Lake OSS [to_be_configured]
 - Spark [to_be_configured]


### Functional services

1) Storage Layer (Lake)
- MinIO [present_on_cluster]
  Role: S3-compatible object storage
  Purpose: emulate Amazon S3 as the lake backend
  Mandatory configuration:
  - bucket versioning enabled
  - lifecycle policies (retention, cleanup)
  - logical separation: bronze / silver / gold

2) Table Format (Lakehouse Core)
- Delta Lake OSS [to_be_configured]
  Role: transactional table format
  Purpose:
  - system of record
  - ACID transactions
  - time travel
  - MERGE INTO
  - schema enforcement and evolution

3) Compute — Canonical Writer
- Apache Spark [to_be_configured]
  Role: primary Delta Lake writer
  Used for:
  - raw → bronze ingestion
  - deduplication
  - SCD Type 2
  - MERGE INTO
  - backfills
  - file compaction
  - VACUUM

- Spark Operator (Kubernetes) [to_be_configured]
  Role: Spark job lifecycle management
  Purpose: production-like execution on Kubernetes

4) Orchestration
- Apache Airflow [present_on_cluster]
  Role: orchestration and dependency management
  Typical DAGs:
  - ingest → bronze
  - bronze → silver
  - silver → gold
  - maintenance (OPTIMIZE / VACUUM)
  - data quality gates

5) SQL / Validation / Lightweight Compute
- DuckDB [to_be_configured]
  Role: read-only analytics engine
  Used for:
  - reading Delta tables
  - validation
  - fast checks
  - lightweight aggregations

- dbt [to_be_configured]
  Role: data contracts, tests, documentation
  Used for:
  - schema contracts
  - data quality tests
  - documentation
  - lineage (via artifacts)
  dbt does not manage Delta transactions

6) Query / Serving Layer
- PostgreSQL [present_on_cluster]
  Role: serving database
  Purpose:
  - gold-layer marts
  - Power BI / Superset / Metabase

7) Data Quality & Lineage
- dbt artifacts [to_be_configured]
  - manifest
  - run_results

- OpenLineage + Marquez (optional) [to_be_configured]
  Role: end-to-end lineage
  Purpose: enterprise-grade pipeline traceability


8) Secrets & Security [present_on_cluster]
- SOPS with key at Azure Key Vault
  Role: secrets at rest

Responsibility Model:
- Delta Lake + Spark = system of record
- DuckDB + dbt = validation and lightweight analytics
- MinIO = storage only, no data logic
- Airflow = orchestration
