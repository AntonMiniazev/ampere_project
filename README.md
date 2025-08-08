# Project Overview:

<b> [WIP:] </b> Establishing Generation + ETL + Reporting on homelab (Ubuntu server).

<b> [Current stage:] </b> Deployment.

- [ ] Create master diagram
  - [x] Initialized
  - [ ] Update tables
  - [ ] Complete Web part
- [x] Prepare Python generators
- [x] Deployment
  - [x] Prepare Helm charts
    - [x] Airflow
    - [x] MinIO
    - [x] MSSQL
    - [ ] DuckDB + dbt
  - [x] Prepare one script
    - [x] Prepare sub-script (`bootstrap.sh`) for VM deployment
    - [x] Organize `Vagrantfile` for separate deployment
    - [x] Prepare sub-script for Helm charts on master
- [ ] ETL
  - [ ] Establish DS → Stage layer
	  - [x] Launch generators in Airflow (deployed through orders_clients_generation dag)
	  - [ ] Create sensor to push to dbt model
	  - [ ] Downstream DAGs to push data to golden layer
  - [ ] Upload to MSSQL
    - [ ] DDL / DML
    - [ ] Backups
  - [ ] Testing
- [ ] Data Marts preparation
- [ ] BI layer
---

<p align="center">
  <img src="https://github.com/AntonMiniazev/ampere_project/blob/master/project_images/high_level_structure.svg" />
</p>
