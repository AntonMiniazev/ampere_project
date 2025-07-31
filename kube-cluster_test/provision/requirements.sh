#!/bin/bash
set -e

### --- SYSTEM UPDATE AND BASE UTILITIES --- ###
echo ">> [1/8] Updating system packages"
apt-get update -y
apt-get upgrade -y

echo ">> Installing base utilities (curl, vim, etc.)"
apt-get install -y curl vim net-tools htop lsb-release apt-transport-https ca-certificates gnupg sudo openssh-server


### --- SWAP DISABLEMENT --- ###
echo ">> [2/8] Disabling swap (required for Kubernetes)"
swapoff -a
sed -i '/ swap / s/^/#/' /etc/fstab

### --- LOCAL PATH PROVISIONER PREP (OPTIONAL FOR PVC TESTING) --- ###
echo ">> Creating local path provisioner directory (used for dynamic PVC provisioning in development)"
sudo mkdir -p /opt/local-path-provisioner
sudo chmod -R 777 /opt/local-path-provisioner

### --- /etc/hosts CONFIGURATION --- ###
echo ">> [3/8] Adding Kubernetes cluster hostnames to /etc/hosts"

# Clean up any previously defined cluster host mappings
sudo sed -i '/# --- K8S CLUSTER BEGIN ---/,/# --- K8S CLUSTER END ---/d' /etc/hosts

# Append fresh entries
sudo tee -a /etc/hosts > /dev/null <<EOF
# --- K8S CLUSTER BEGIN ---
192.168.10.100 ampere-t-k8s-master
192.168.10.101 ampere-t-k8s-node1
192.168.10.102 ampere-t-k8s-node2
192.168.10.103 ampere-t-k8s-node3
# --- K8S CLUSTER END ---
EOF

### --- SSH CONFIGURATION --- ###
echo ">> [4/8] Hardening SSH configuration (disable password auth)"
sed -i 's/^#\?PasswordAuthentication.*/PasswordAuthentication no/' /etc/ssh/sshd_config
sed -i 's/^#\?PubkeyAuthentication.*/PubkeyAuthentication yes/' /etc/ssh/sshd_config

echo ">> Restarting SSH service"
systemctl restart ssh

### --- KERNEL MODULES AND SYSCTL (K8S PREREQUISITES) --- ###
echo ">> [5/8] Enabling required kernel modules and sysctl settings for Kubernetes"
sudo modprobe overlay
sudo modprobe br_netfilter

# Persist loaded modules
sudo tee /etc/modules-load.d/k8s.conf > /dev/null <<EOF
overlay
br_netfilter
EOF

# Apply sysctl settings
sudo tee /etc/sysctl.d/k8s.conf > /dev/null <<EOF
net.bridge.bridge-nf-call-iptables  = 1
net.bridge.bridge-nf-call-ip6tables = 1
net.ipv4.ip_forward   = 1
EOF

sudo sysctl --system

### --- CONTAINER RUNTIME INSTALLATION --- ###
echo ">> [6/8] Installing Docker (used by containerd as backend)"
sudo apt install docker.io -y
sudo systemctl enable docker

echo ">> Installing and configuring containerd"
sudo mkdir -p /etc/containerd
containerd config default | sudo tee /etc/containerd/config.toml > /dev/null
sudo sed -i 's/SystemdCgroup = false/SystemdCgroup = true/' /etc/containerd/config.toml
sudo systemctl restart containerd.service

echo ">> Checking containerd status (for debug)"
sudo systemctl status containerd.service || true


### --- KUBERNETES COMPONENTS INSTALLATION --- ###
echo ">> [7/8] Installing Kubernetes components: kubelet, kubeadm, kubectl"

# Add Kubernetes repo
sudo apt-get install -y curl ca-certificates apt-transport-https
sudo mkdir -p /etc/apt/keyrings
curl -fsSL https://pkgs.k8s.io/core:/stable:/v1.31/deb/Release.key | sudo gpg --dearmor -o /etc/apt/keyrings/kubernetes-apt-keyring.gpg
echo "deb [signed-by=/etc/apt/keyrings/kubernetes-apt-keyring.gpg] https://pkgs.k8s.io/core:/stable:/v1.31/deb/ /" | sudo tee /etc/apt/sources.list.d/kubernetes.list > /dev/null

# Install components
sudo apt update
sudo apt install -y kubelet kubeadm kubectl
sudo apt-mark hold kubelet kubeadm kubectl


### --- TIMEZONE CONFIGURATION --- ###
echo ">> [8/8] Setting system timezone to Europe/Belgrade"
timedatectl set-timezone Europe/Belgrade


### --- FINALIZATION --- ###
echo ">>>>>>>> System setup complete. Ready for Kubernetes initialization or joining."