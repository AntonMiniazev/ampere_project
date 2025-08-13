#!/bin/bash
set -e
set -a
source /vagrant/deploy.env
set +a

# Wait for all nodes to be Ready
echo ">> Started post-deployment"

for i in {1..60}; do
  READY_NODES=$(kubectl get nodes --no-headers | grep -c " Ready")
  echo "[$i] Ready nodes: $READY_NODES/$NODE_NUMBER"
  if [ "$READY_NODES" -eq "$NODE_NUMBER" ]; then
    echo ">> All nodes are Ready"
    break
  fi
  kubectl get nodes
  sleep 5
done

if [ "$READY_NODES" -ne "$NODE_NUMBER" ]; then
  echo "[ERROR] Timeout waiting for all $NODE_NUMBER nodes to become Ready."
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
if [ "$(hostname)" = "$MASTER_NAME" ]; then
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

  # Add KEDA for Airflow workers scalling
  helm repo add kedacore https://kedacore.github.io/charts
  helm repo update

  helm upgrade --install keda kedacore/keda \
    -n keda --create-namespace \
    --set watchNamespace="ampere-project"

  echo ">> Importing GPG private key"
  gpg --import /home/vagrant/gpg_key/private-key.asc
  #rm -f /home/vagrant/gpg_key/private-key.asc

  # SQL Server on node1
  echo ">> Deploying SQL Server via Helm"
  cd /home/vagrant/ms-chart
  kubectl get ns $PROJECT_NAME >/dev/null 2>&1 || kubectl create ns $PROJECT_NAME
  
  envsubst < values.template.yaml > values.generated.yaml
  helm secrets upgrade --install mssql . \
    -f values.generated.yaml \
    -f credentials.yaml \
    -n $PROJECT_NAME

  # MinIO on node2
  echo ">> Deploying MinIO via Helm"
  cd /home/vagrant/minio-chart

  envsubst < values.template.yaml > values.generated.yaml
  helm secrets upgrade --install minio . \
    -f values.generated.yaml \
    -f credentials.yaml \
    -n $PROJECT_NAME

  # Airflow on node3

  MSSQL_SECRET=$(kubectl get secret mssql-sa-secret \
    -n $PROJECT_NAME \
    -o jsonpath="{.data.SA_PASSWORD}" | base64 --decode)

  export MSSQL_SECRET

  echo ">> Deploying Airflow via Helm"
  cd /home/vagrant/airflow-chart
  
  helm repo add apache-airflow https://airflow.apache.org
  helm repo update
  
  sops -d git-credentials.yaml | kubectl apply -f -
  
  envsubst < values.template.yaml > values.generated.yaml
  helm secrets install airflow apache-airflow/airflow \
    -n $PROJECT_NAME \
    -f values.generated.yaml \
    -f credentials.yaml \
    --timeout 10m0s \
    --debug

fi