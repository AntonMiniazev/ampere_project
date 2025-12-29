# Project Overview:

<b> [WIP:] </b> Establishing Generation + ETL + Reporting on my homelab (Ubuntu server).

<b> [Current stage:] </b> ETL development and dbt model preparation.

- [x] Complete [master diagram](https://raw.githubusercontent.com/AntonMiniazev/ampere_project/refs/heads/master/project_images/ampere_project_structure.svg)
  - [x] Initialized
  - [x] Update tables
- [x] Prepare Python generators
  - [x] Prepare python scripts to generate raw data (`dags/generators`)
  - [x] Complete diagram with [generator's logic](https://github.com/AntonMiniazev/ampere_project/blob/master/project_images/python_generator_script.svg)
- [x] Deployment
  - [x] Prepare Helm charts
    - [x] Airflow (`kube-cluster/airflow-chart`)
    - [x] MinIO (`kube-cluster/minio-chart`)
    - [x] SQL Server (`kube-cluster/ms-chart`)
    - [x] DuckDB + dbt (`kube-cluster/dbt-chart`)
  - [x] Prepare one script for deployment
    - [x] Prepare sub-script (`kube-cluster/deploy.sh`) for VM deployment
    - [x] Organize `Vagrantfile` for separate deployment (`kube-cluster/other_scripts/post-deployment.sh`)
    - [x] Prepare sub-script for Helm charts on master
- [ ] ETL
  - [x] Establish DS → Stage layer
  - [x] Launch generator DAG (deployed through `dags/my_dags/ampere__pre_raw__generators__daily.py`)
    - [x] Launch transfer ingestion DAG (deployed through `dags/my_dags/source_to_minio.py`)
    - [x] Launch MinIO cleaning DAG to remove unnecessary artefacts in RAW and Processing buckets (deployed through `dags/my_dags/cleanup_minio_shapshots.py`)
    - [x] Prepare multi-stage docker image for dbt development (local dev container + dev image for pod to process dbt models) 
    - [x] Create dbt model for processing layer
      - [x] Processing dbt model with tests (deployed through `/workspaces/ampere_project/dbt/models/processing/`)
      - [x] DAG to manage processing dbt model (deployed through `dags/my_dags/dbt_processing_model.py`)
    - [x] Create dbt model for stage pre-business logic layer
      - [x] Pre-BL dbt model (deployed through `dbt/models/business_logic/export_processing_to_mssql.py`)
      - [x] DAG to manage BL dbt model
  - [x] Stage to Business logic process
    - [x] Initial schemas
    - [x] DDL / DML scripts with associated DAGs
    - [ ] Backups
- [ ] Enchance all layers with incremental updates/SCD adoption
- [ ] BI layer
---

<p align="center">
  <img src="https://github.com/AntonMiniazev/ampere_project/blob/master/project_images/high_level_structure.svg" />
</p>
