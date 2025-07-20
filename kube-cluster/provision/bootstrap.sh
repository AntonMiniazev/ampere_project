#!/bin/bash
set -e

VAGRANT_HOME="/home/vagrant"

echo ">> Updating system packages"
apt-get update -y
apt-get upgrade -y

echo ">> Installing base utilities"
apt-get install -y curl vim net-tools htop lsb-release apt-transport-https ca-certificates gnupg sudo openssh-server

echo ">> Disabling swap"
swapoff -a
sed -i '/ swap / s/^/#/' /etc/fstab

echo ">> Appending hostname to /etc/hosts"
echo "127.0.1.1 $(hostname)" >> /etc/hosts

echo ">> Creating .ssh directory and copying public keys"
mkdir -p /home/vagrant/.ssh
cat /home/vagrant/host_ssh/authorized_keys > /home/vagrant/.ssh/authorized_keys

chmod 700 /home/vagrant/.ssh
chmod 600 /home/vagrant/.ssh/authorized_keys
chown -R vagrant:vagrant /home/vagrant/.ssh

echo ">> Disabling password authentication in SSH"
sed -i 's/^#\?PasswordAuthentication.*/PasswordAuthentication no/' /etc/ssh/sshd_config
sed -i 's/^#\?PubkeyAuthentication.*/PubkeyAuthentication yes/' /etc/ssh/sshd_config

echo ">> Restarting SSH service"
systemctl restart ssh

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
    --apiserver-advertise-address=$IP_ADDR \
    --apiserver-cert-extra-sans=$IP_ADDR \
    --control-plane-endpoint=$IP_ADDR \
    --pod-network-cidr=192.168.0.0/16

  echo ">> Setting up kubeconfig for kubectl"
if [ "$(hostname)" = "ampere-k8s-master" ]; then
  
  # Waiting for admin.conf
  for i in {1..10}; do
    if [ -f /etc/kubernetes/admin.conf ]; then break; fi
    sleep 2
  done

  # Copying config
  if [ -f /etc/kubernetes/admin.conf ]; then
    mkdir -p $VAGRANT_HOME/.kube
    cp /etc/kubernetes/admin.conf $VAGRANT_HOME/.kube/config
    chown vagrant:vagrant $VAGRANT_HOME/.kube/config
  else
    echo "[ERROR] /etc/kubernetes/admin.conf not found after kubeadm init!"
    exit 1
  fi

fi

  echo ">> Installing Calico CNI"
  kubectl create -f https://raw.githubusercontent.com/projectcalico/calico/v3.28.0/manifests/tigera-operator.yaml

  echo ">> Waiting for Calico CRD to be ready"
  kubectl wait --for=condition=Established crd/installations.operator.tigera.io --timeout=60s

  echo ">> Applying Calico configuration"
  curl -O https://raw.githubusercontent.com/projectcalico/calico/v3.28.0/manifests/custom-resources.yaml
  kubectl apply -f custom-resources.yaml

  echo ">> Saving join command to shared folder"
  kubeadm token create --print-join-command > /vagrant/join.sh
  chmod +x /vagrant/join.sh
  

  echo ">> Master setup complete"
fi

if [[ "$(hostname)" != "ampere-k8s-master" ]]; then
  echo ">> Waiting for join.sh to appear from master"
  while [ ! -f /vagrant/join.sh ]; do
    sleep 5
  done

  echo ">> Joining cluster"
  bash /vagrant/join.sh
fi

chmod +x /vagrant/provision/post-deployment.sh

sudo sysctl --system

echo ">> Setting timezone"
timedatectl set-timezone Europe/Belgrade

echo ">> Done"
