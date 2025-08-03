# Deployment

## Overview

This repository provides automated deployment of a local Kubernetes cluster with 4 VMs using Vagrant and VirtualBox.

### Specifications
- Ubuntu 24.04.2 LTS
- vagrant 2.4.7
- VirtualBox 7.0.16

### Cluster Structure

The following nodes are provisioned:

1. **ampere-k8s-master** (192.168.10.100)
   - Kubernetes Control Plane
   - Helm (service deployment)
   - Calico (CNI plugin)
   - Local-path provisioner

2. **ampere-k8s-node1** (192.168.10.101)
   - SQL Server

3. **ampere-k8s-node2** (192.168.10.102)
   - MinIO (Bronze/ingestion)
   - DuckDB and dbt (Silver/processing)

4. **ampere-k8s-node3** (192.168.10.103)
   - Airflow (Orchestration)

---

## Deployment Flow

1. `deploy.sh` launches the entire provisioning process.
2. Shared configuration parameters are stored in `deploy.env`.
3. The `provision/` folder contains scripts for:
   - installing required software,
   - initializing Kubernetes master and nodes,
   - applying Calico and storage class,
   - copying Helm values via `envsubst`.
4. `post-deployment.sh` handles:
   - Helm and sops installation,
   - GPG import,
   - Helm chart deployment (SQL Server, MinIO, Airflow),
   - `git-credentials.yaml` decryption and injection.

---

## Parameters
    - Vagrantfile contains chart locations, assigned addresses and VM names. Fleet configured in cluster_config.rb
    - deploy.sh, provision scripts and post-deployment include deploy.env parameters. 
    - Chart configured to utilize values.generated.yaml, which is completed by parameters from deploy.env inserted in values.template.yaml

# Security

git-credentials.yaml in airflow-chart and credentials in ms-chart are secured by sops.
sops.yaml should be adjusted for a new created fingerprints.
gpg key is put on the host and checked by deploy.sh: /home/gpg_key/private-key.asc - default location on host.
Airflow configured with gitsync keys.

# NGINX Reverse Proxy Configuration

## MS SQL /etc/nginx/nginx.conf

```
stream {
  upstream mssql_upstream {
    server 192.168.10.101:31433;
  }

  server {
    listen 14330;
    proxy_pass mssql_upstream;
  }
}
```

## Airflow /etc/nginx/sites-available/airflow

```
server {
    listen 80;
    server_name airflow.local;

    location / {
        proxy_pass http://192.168.10.103:30080;
        proxy_set_header Host $host;
        proxy_set_header X-Forwarded-Host $host;
        proxy_set_header X-Forwarded-Proto http;
        proxy_set_header X-Forwarded-For $remote_addr;

        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_redirect off;
    }
}
```

## Minio /etc/nginx/sites-available/minio
```
server {
    listen 80;
    server_name minio.local;

    location / {
        proxy_pass http://192.168.10.102:30090;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";

        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;

        proxy_buffering off;
        proxy_request_buffering off;
    }
}
```