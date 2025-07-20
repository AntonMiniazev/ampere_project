#!/bin/bash
set -e

echo ">> Updating system packages"
apt-get update -y
apt-get upgrade -y

echo ">> Installing base utilities"
apt-get install -y curl vim net-tools htop lsb-release apt-transport-https ca-certificates gnupg sudo openssh-server

echo ">> Disabling swap"
swapoff -a
sed -i '/ swap / s/^/#/' /etc/fstab

#echo ">> Adding cluster node hostnames to /etc/hosts"
#
## Remove any previous cluster entries between markers
#sudo sed -i '/# --- K8S CLUSTER BEGIN ---/,/# --- K8S CLUSTER END ---/d' /etc/hosts
#
## Add new cluster host entries
#sudo tee -a /etc/hosts > /dev/null <<EOF
## --- K8S CLUSTER BEGIN ---
#192.168.56.100 ampere-k8s-master
#192.168.56.101 ampere-k8s-node1
#192.168.56.102 ampere-k8s-node2
#192.168.56.103 ampere-k8s-node3
## --- K8S CLUSTER END ---
#EOF

# SSH config
echo ">> Disabling password authentication in SSH"
sed -i 's/^#\?PasswordAuthentication.*/PasswordAuthentication no/' /etc/ssh/sshd_config
sed -i 's/^#\?PubkeyAuthentication.*/PubkeyAuthentication yes/' /etc/ssh/sshd_config

echo ">> Restarting SSH service"
systemctl restart ssh

# Kubernetes preparation
echo ">> Kubernetes prep"
sudo modprobe overlay
sudo modprobe br_netfilter

sudo tee /etc/modules-load.d/k8s.conf <<EOF
overlay
br_netfilter
EOF

sudo tee /etc/sysctl.d/k8s.conf <<EOF
net.bridge.bridge-nf-call-iptables  = 1
net.bridge.bridge-nf-call-ip6tables = 1
net.ipv4.ip_forward   = 1
EOF

echo ">> Docker installation"
sudo apt install docker.io -y
sudo systemctl enable docker

echo ">> Containerd installation"
sudo mkdir /etc/containerd
sudo sh -c "containerd config default > /etc/containerd/config.toml"
sudo sed -i 's/ SystemdCgroup = false/ SystemdCgroup = true/' /etc/containerd/config.toml
sudo systemctl restart containerd.service
sudo systemctl status containerd.service

echo ">> Kubernetes components installation"
sudo apt-get install curl ca-certificates apt-transport-https  -y
curl -fsSL https://pkgs.k8s.io/core:/stable:/v1.31/deb/Release.key | sudo gpg --dearmor -o /etc/apt/keyrings/kubernetes-apt-keyring.gpg
echo "deb [signed-by=/etc/apt/keyrings/kubernetes-apt-keyring.gpg] https://pkgs.k8s.io/core:/stable:/v1.31/deb/ /" | sudo tee /etc/apt/sources.list.d/kubernetes.list
sudo apt update
sudo apt install kubelet kubeadm kubectl -y

if [ "$(hostname)" = "ampere-k8s-master" ]; then
  echo ">> Running master-only setup"

  IP_ADDR=$(ip -4 -o addr show scope global | grep -v '10.0.2' | grep -v '127.0.0.1' | awk '{print $4}' | cut -d/ -f1 | head -n1)
  echo "Detected host IP: $IP_ADDR"

  echo ">> Initializing kubeadm..."
  sudo kubeadm init \
  --apiserver-advertise-address=192.168.56.100 \
  --apiserver-cert-extra-sans=192.168.56.100 \
  --pod-network-cidr=10.10.0.0/16

  echo ">> Setting up kubeconfig for kubectl"
  
  # Waiting for admin.conf
  for i in {1..8}; do
    if [ -f /etc/kubernetes/admin.conf ]; then break; fi
    sleep 15
  done

  # Copying config
  if [ -f /etc/kubernetes/admin.conf ]; then
    mkdir -p /home/vagrant/.kube
    sudo cp -i /etc/kubernetes/admin.conf /home/vagrant/.kube/config
    sudo chown vagrant:vagrant /home/vagrant/.kube/config
    sudo chmod 600 /home/vagrant/.kube/config    
  else
    echo "[ERROR] /etc/kubernetes/admin.conf not found after kubeadm init!"
    exit 1
  fi

  for i in {1..20}; do
    kubectl get nodes && break
    echo "[INFO] Waiting for API server ($i/20)..."
    sleep 3
  done

  echo ">> Installing Calico CNI"
  kubectl apply --validate=false -f https://raw.githubusercontent.com/projectcalico/calico/v3.30.2/manifests/tigera-operator.yaml

  for i in {1..6}; do
    kubectl get crd installations.operator.tigera.io &>/dev/null && break
    echo "[INFO] Waiting for Calico CRDs to be established ($i/30)..."
    sleep 10
  done

  echo ">> Applying Calico configuration"
  kubectl apply -f https://raw.githubusercontent.com/projectcalico/calico/v3.30.2/manifests/custom-resources.yaml
  sed -i 's/cidr: 192\.168\.0\.0\/16/cidr: 10.10.0.0\/16/g' custom-resources.yaml
  kubectl create -f custom-resources.yaml

  echo ">> Saving join command to shared folder"
  kubeadm token create --print-join-command > /vagrant/join.sh
  chmod +x /vagrant/join.sh
  
  echo ">> Master setup complete"
fi

if [ "$(hostname)" = "ampere-k8s-master" ]; then

  echo "=== SSH DEBUG INFO (master node) ==="

  echo -n "[1] .ssh directory: "
  if [ -d /home/vagrant/.ssh ]; then echo "OK"; else echo "NOT FOUND"; fi

  echo -n "[2] authorized_keys: "
  if [ -f /home/vagrant/.ssh/authorized_keys ]; then
    echo "OK"
    echo -n "[3] authorized_keys permissions: "
    stat -c "%A %U:%G" /home/vagrant/.ssh/authorized_keys
    echo "[4] First line of authorized_keys:"
    head -n 1 /home/vagrant/.ssh/authorized_keys
  else
    echo "NOT FOUND"
  fi

  echo -n "[5] .ssh directory permissions: "
  stat -c "%A %U:%G" /home/vagrant/.ssh

  echo "[6] SSH service status:"
  sudo systemctl status ssh | head -n 10

  echo "[7] Whoami: $(whoami)"
  echo "[8] IP addresses:"
  ip -4 addr show | grep inet

  echo "[9] Last SSH logins:"
  last -n 5 -a | grep "sshd" || true

  echo "=== END OF SSH DEBUG INFO ==="
fi


if [[ "$(hostname)" != "ampere-k8s-master" ]]; then
  echo ">> Waiting for join.sh to appear from master"
  while [ ! -f /vagrant/join.sh ]; do
    sleep 5
  done

  echo ">> join.sh contents:"
  cat /vagrant/join.sh

  echo ">> Joining cluster"
  bash /vagrant/join.sh
fi

chmod +x /vagrant/provision/post-deployment.sh

sudo sysctl --system

echo ">> Setting timezone"
timedatectl set-timezone Europe/Belgrade

echo ">> Done"
