#!/bin/bash
set -e

apt-get update && apt-get install -y gnupg curl
curl -LO https://github.com/getsops/sops/releases/download/v3.10.2/sops-v3.10.2.linux.amd64
mv sops-v3.10.2.linux.amd64 /usr/local/bin/sops
chmod +x /usr/local/bin/sops
helm plugin install https://github.com/jkroepke/helm-secrets
helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo update