#!/bin/bash
set -e

# Update package lists and install required tools
sudo apt-get update && sudo apt-get install -y gnupg curl

# Download and install sops binary
curl -LO https://github.com/getsops/sops/releases/download/v3.10.2/sops-v3.10.2.linux.amd64
sudo mv sops-v3.10.2.linux.amd64 /usr/local/bin/sops
sudo chmod +x /usr/local/bin/sops

# Install helm-secrets plugin (as the current user, NOT with sudo)
helm plugin install https://github.com/jkroepke/helm-secrets || echo "Helm plugin already installed"

# Add Bitnami Helm repo and update
helm repo add bitnami https://charts.bitnami.com/bitnami || true
helm repo updates