# Project Overview:

<b> [WIP:] </b> Establishing Generation + ETL + Reporting on homelab (Ubuntu server).

<b> [Current stage:] </b> Deployment.

- [ ] Create master diagram
	- [x] Initialized 
	- [ ] Update tables
	- [ ] Complete Web part
- [x] Prepare python generators 
- [x] Deployment
	- [x] Prepare helm charts 
		- [x] Airflow 
		- [x] Minio 
		- [x] MSSQL 
	- [x] Prepare One script
		- [x] Prepare sub-script (bootstrap.sh) for VM deployment 
		- [x] Organize Vagrantfile for separate deployment
		- [x] Prepare sub-script for helm charts on Master
- [ ] ETL
	* [ ] Establish DS - Stage layer
	* [ ] Uploading to MSSQL
		* [ ] DDL / DML
		* [ ] Backups
	* [ ] Testing
- [ ] Data Marts preparation
- [ ] BI
---

<p align="center">
  <img src="https://github.com/AntonMiniazev/ampere_project/blob/master/project_images/high_level_structure.svg" />
</p>
