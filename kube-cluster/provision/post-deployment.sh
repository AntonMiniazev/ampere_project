#!/bin/bash
set -e

# Wait for all nodes to be Ready
echo ">> Started post-deployment"
EXPECTED_NODES=4
for i in {1..60}; do
  READY_NODES=$(kubectl get nodes --no-headers | grep -c " Ready")
  echo "[$i] Ready nodes: $READY_NODES/$EXPECTED_NODES"
  if [ "$READY_NODES" -eq "$EXPECTED_NODES" ]; then
    echo ">> All nodes are Ready"
    break
  fi
  sleep 5
done

if [ "$READY_NODES" -ne "$EXPECTED_NODES" ]; then
  echo "[ERROR] Timeout waiting for nodes"
  exit 1
fi

# Configuring nodes through master
if [ "$(hostname)" = "ampere-k8s-master" ]; then
  # Install gnupg and helm
  sudo apt-get install -y gnupg curl
  if ! command -v helm &>/dev/null; then
    echo "[INFO] Installing helm..."
    curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
  fi
  
  # Download and install sops binary
  curl -LO https://github.com/getsops/sops/releases/download/v3.10.2/sops-v3.10.2.linux.amd64
  sudo mv sops-v3.10.2.linux.amd64 /usr/local/bin/sops
  sudo chmod +x /usr/local/bin/sops
  
  # Install helm-secrets plugin (as the current user, NOT with sudo)
  helm plugin install https://github.com/jkroepke/helm-secrets || echo "Helm plugin already installed"
  
  # Add Bitnami Helm repo and update
  helm repo add bitnami https://charts.bitnami.com/bitnami || true
  helm repo updates
  
  echo ">> Importing GPG private key"
  gpg --import /home/vagrant/gpg_key/private-key.asc
  #rm -f /home/vagrant/gpg_key/private-key.asc

  # SQL Server on node1
  echo ">> Deploying SQL Server via Helm"
  cd /home/vagrant/ms-chart
  kubectl get ns ampere-project >/dev/null 2>&1 || kubectl create ns ampere-project
  
  helm secrets upgrade --install mssql . \
  -f values.yaml \
  -f credentials.yaml \
  -n ampere-project

fi