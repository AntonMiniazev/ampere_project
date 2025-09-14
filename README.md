# Project Overview:

<b> [WIP:] </b> Establishing Generation + ETL + Reporting on homelab (Ubuntu server).

<b> [Current stage:] </b> ETL development and dbt model preparation.

- [ ] Complete [master diagram](https://raw.githubusercontent.com/AntonMiniazev/ampere_project/refs/heads/master/project_images/ampere_project_structure.svg)
  - [x] Initialized
  - [x] Update tables
  - [ ] Complete Web part
- [x] Prepare Python generators
  - [x] Prepare python scripts to generate raw data (`dags/generators`)
  - [ ] Complete diagram with [generator's logic](https://github.com/AntonMiniazev/ampere_project/blob/master/project_images/python_generator_script.svg)
- [x] Deployment
  - [x] Prepare Helm charts
    - [x] Airflow (`kube-cluster/airflow-chart`)
    - [x] MinIO (`kube-cluster/minio-chart`)
    - [x] MSSQL (`kube-cluster/ms-chart`)
    - [x] DuckDB + dbt (`kube-cluster/dbt-chart`)
  - [x] Prepare one script for deployment
    - [x] Prepare sub-script (`kube-cluster/deploy.sh`) for VM deployment
    - [x] Organize `Vagrantfile` for separate deployment (`kube-cluster/other_scripts/post-deployment.sh`)
    - [x] Prepare sub-script for Helm charts on master
- [ ] ETL
  - [ ] Establish DS → Stage layer
	  - [x] Launch generator DAG (deployed through `dags/my_dags/orders_clients_gen_dag.py`)
    - [x] Launch transfer ingestion DAG (deployed through `dags/my_dags/source_to_minio.py`)
    - [x] Launch MinIO cleaning DAG to remove unnecessary artefacts (deployed through `dags/my_dags/cleanup_raw_shapshots.py`)
    - [ ] Create dbt model for processing layer
    - [ ] Create dbt model for business logic layer
	  - [ ] Create sensor to push dbt model
	  - [ ] Downstream DAGs to push data to golden layer
  - [ ] Upload to MSSQL (business logic layer)
    - [ ] DDL / DML
    - [ ] Backups
  - [ ] Testing
- [ ] Data Marts preparation
  - [x] Initial schemas
- [ ] BI layer
---

<p align="center">
  <img src="https://github.com/AntonMiniazev/ampere_project/blob/master/project_images/high_level_structure.svg" />
</p>
