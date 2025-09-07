# Project Overview:

<b> [WIP:] </b> Establishing Generation + ETL + Reporting on homelab (Ubuntu server).

<b> [Current stage:] </b> Deployment.

- [ ] Complete [master diagram](https://github.com/AntonMiniazev/ampere_project/blob/master/project_images/ampere_project_structure.svg)
  - [x] Initialized
  - [x] Update tables
  - [ ] Complete Web part
- [x] Prepare Python generators (`dags/generators`)
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
	  - [x] Launch generators in Airflow (deployed through `dags/my_dags/orders_clients_gen_dag.py` dag)
	  - [ ] Create sensor to push to dbt model
	  - [ ] Downstream DAGs to push data to golden layer
  - [ ] Upload to MSSQL
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
