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
  kubectl get nodes
  sleep 5
done

if [ "$READY_NODES" -ne "$EXPECTED_NODES" ]; then
  echo "[ERROR] Timeout waiting for all $EXPECTED_NODES nodes to become Ready."
  echo ">>> Final node status:"
  kubectl get nodes
  NOT_READY=$(kubectl get nodes --no-headers | grep -v " Ready" | awk '{print $1, $2}')
  if [ -n "$NOT_READY" ]; then
    echo ">>> Nodes not Ready:"
    echo "$NOT_READY"
  fi
  exit 1
else
  echo ">> Proceeding with deployment: all $READY_NODES nodes are Ready."
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

  # MinIO on node2
  echo ">> Deploying MinIO via Helm"
  cd /home/vagrant/minio-chart
  helm upgrade --install minio . --namespace ampere-project --create-namespace

  # Airflow on node3
  echo ">> Deploying Airflow via Helm"
  cd /home/vagrant/airflow-chart
  
  helm secrets upgrade airflow apache-airflow/airflow \
    -n ampere-project \
    -f values.yaml \
    -f git-credentials.yaml \
    --create-namespace \
    --timeout 10m0s

fi