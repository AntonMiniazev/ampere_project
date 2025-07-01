#!/bin/bash
set -e

# Update package lists and install required tools
sudo apt-get update && sudo apt-get install -y gnupg curl

# Install helm
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

# Install k3s
if ! command -v k3s &>/dev/null; then
  echo "[INFO] Installing k3s without Traefik..."
  curl -sfL https://get.k3s.io | INSTALL_K3S_EXEC="--disable traefik" sh -
else
  echo "[INFO] k3s already installed"
fi

# Install kubectl
if ! command -v kubectl &>/dev/null; then
  echo "[INFO] Installing kubectl..."
  curl -LO https://dl.k8s.io/release/v1.30.1/bin/linux/amd64/kubectl
  chmod +x kubectl
  sudo mv kubectl /usr/local/bin/
fi

# Check kubeconfig
if ! kubectl get namespace default &>/dev/null; then
  echo "Error: kubectl is not configured. Check if KUBECONFIG is mounted or ~/.kube/config exists and is valid."
  exit 1
fi

# Create namespace
echo "Creating namespace if it does not exist: ampere-project"
kubectl get namespace "ampere-project" >/dev/null 2>&1 || kubectl create namespace "ampere-project"

# Ingress
echo "Adding ingress-nginx Helm repository..."
helm repo add ingress-nginx https://kubernetes.github.io/ingress-nginx || true
helm repo update

echo "Uninstalling Traefik if it exists..."
kubectl delete svc traefik -n kube-system --ignore-not-found
kubectl delete deployment traefik -n kube-system --ignore-not-found
kubectl delete daemonset -n kube-system -l app=svclb-traefik --ignore-not-found

echo "Installing ingress-nginx with hostNetwork (port 80/443)..."
helm upgrade --install ingress-nginx ingress-nginx/ingress-nginx \
  --namespace ingress-nginx --create-namespace \
  --set controller.hostNetwork=true \
  --set controller.dnsPolicy=ClusterFirstWithHostNet \
  --set controller.kind=DaemonSet \
  --set controller.service.type="" \
  --wait

echo "Waiting for ingress-nginx-controller pod to be ready..."
kubectl rollout status daemonset ingress-nginx-controller -n ingress-nginx --timeout=120s

echo "[INFO] Configuring systemd-resolved DNS"

# Ensure systemd-resolved is enabled and started
sudo systemctl enable systemd-resolved --now

# Restart resolver
sudo systemctl restart systemd-resolved

# Update /etc/resolv.conf
sudo ln -sf /run/systemd/resolve/resolv.conf /etc/resolv.conf