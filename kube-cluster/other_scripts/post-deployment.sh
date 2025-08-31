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

  kubectl create ns $PROJECT_NAME

  #Creating secret for webserver
  WEB_KEY=$(python3 -c 'import secrets; print(secrets.token_hex(16))')

  echo "$WEB_KEY" > /home/vagrant/my-airflow-secret.txt

  kubectl -n $PROJECT_NAME create secret generic my-airflow-secret \
  --from-literal=webserver-secret-key="$WEB_KEY" \
  --dry-run=client -o yaml | kubectl apply -f -


  # Add KEDA for Airflow workers scalling
  helm repo add kedacore https://kedacore.github.io/charts
  helm repo update

  helm upgrade --install keda kedacore/keda \
    -n keda --create-namespace \
    --set watchNamespace=$PROJECT_NAME

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

  # dbt+DuckDB on node2
  cd /home/vagrant/dbt-chart

  kubectl apply -f dbt-code-pvc.yaml
  kubectl apply -f dbt-data-pvc.yaml  

  kubectl apply -f dbt-code-tools.yaml
  kubectl apply -f dbt-data-tools.yaml

  kubectl -n $PROJECT_NAME get pod dbt-code-tools dbt-data-tools

  wait_pod_ready () {
    local pod="$1"
    echo "Waiting Pod ${pod} to be Ready..."
    if ! kubectl -n "$PROJECT_NAME" wait --for=condition=Ready "pod/${pod}" --timeout=180s; then
      echo "Pod ${pod} is not Ready. Diagnostics:"
      kubectl -n "$PROJECT_NAME" get pod "${pod}" -o wide || true
      kubectl -n "$PROJECT_NAME" describe pod "${pod}" || true
      # Частые причины: nodeSelector не совпал, нет imagePull/нет сети, или Claim не смонтировался.
      exit 1
    fi
  }
  wait_pod_ready dbt-code-tools
  wait_pod_ready dbt-data-tools

  kubectl -n $PROJECT_NAME exec -it dbt-code-tools -- sh -lc '
  set -euo pipefail
  ssh -o StrictHostKeyChecking=yes -T git@github.com || true

  rm -rf /workspace/dbt_project/* /workspace/dbt_project/.git || true
  GIT_SSH_COMMAND="ssh -i /root/.ssh/id_ed25519 -o IdentitiesOnly=yes" \
    git clone --filter=blob:none --sparse '"$REPO_SSH"' /workspace/dbt_project

  cd /workspace/dbt_project
  git sparse-checkout set dbt/
  git checkout master

  # Local excludes to avoid dbt artifacts interfering with git
  mkdir -p .git/info
  cat > .git/info/exclude << "EOF"
  /dbt/target/
  /dbt/logs/
  /dbt/dbt_packages/
  EOF

  echo "[HEAD] $(git rev-parse --short HEAD)"
  ls -la
  ls -la dbt || true
  '

  kubectl -n "$PROJECT_NAME" exec -i dbt-code-tools -- sh -lc '
  set -euo pipefail
  cd /workspace/dbt_project
  mkdir -p .ops
  cat > .ops/git_update.sh << '"'"'SH'"'"'
  #!/bin/sh
  # Fast-forward local branch to origin/<ref> or checkout a detached commit
  set -euo pipefail
  cd /workspace/dbt_project
  REF="${1:-master}"

  # Ensure sparse-checkout covers the dbt subdir (cone mode includes entire subtree)
  git sparse-checkout set dbt/

  # Fetch latest refs
  git fetch --all --prune

  # If remote branch exists -> fast-forward to origin/REF; else treat REF as commit SHA
  if git show-ref --verify --quiet "refs/remotes/origin/$REF"; then
    # Ensure local branch exists and is checked out
    if git show-ref --verify --quiet "refs/heads/$REF"; then
      git checkout "$REF"
    else
      git checkout -b "$REF" "origin/$REF"
    fi
    # Hard reset to the remote tip (fast-forward without merge commits)
    git reset --hard "origin/$REF"
  else
    # Detached checkout for specific commit/tag
    git checkout --detach "$REF"
  fi

  echo "Updated to: $(git rev-parse --short HEAD)"
  SH

  # make it executable and strip CRLF just in case
  chmod 0755 .ops/git_update.sh
  sed -i "s/\r$//" .ops/git_update.sh || true
  head -n 3 .ops/git_update.sh; tail -n 3 .ops/git_update.sh
  '


  kubectl -n $PROJECT_NAME exec -it dbt-data-tools -- sh -lc '
  set -eu
  mkdir -p /workspace/dbt_work/profiles
  cat > /workspace/dbt_work/profiles/profiles.yml << "YAML"
  ampere_project:
    target: k8s
    outputs:
      k8s:
        type: duckdb
        path: /workspace/dbt_data/warehouse.duckdb
        threads: 4
        extensions: ["httpfs","parquet","json"]
  YAML
  echo "profiles.yml written:"; cat /workspace/dbt_work/profiles/profiles.yml
  '

fi