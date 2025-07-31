#!/bin/bash
set -e

echo ">> [MASTER] Running master-only setup"

### --- KUBEADM INITIALIZATION --- ###
echo ">> Initializing kubeadm control plane"
sudo kubeadm init \
  --apiserver-advertise-address=192.168.10.100 \
  --apiserver-cert-extra-sans=192.168.10.100 \
  --pod-network-cidr=10.10.0.0/16

### --- KUBECONFIG SETUP FOR VAGRANT USER --- ###
echo ">> Configuring kubeconfig for user 'vagrant'"

# Wait for kubeadm to generate admin.conf (just in case it takes time)
for i in {1..8}; do
  if [ -f /etc/kubernetes/admin.conf ]; then break; fi
  echo "[INFO] Waiting for /etc/kubernetes/admin.conf ($i/8)..."
  sleep 15
done

# Copy kubeconfig for non-root usage via vagrant user
if [ -f /etc/kubernetes/admin.conf ]; then
  mkdir -p /home/vagrant/.kube
  cp /etc/kubernetes/admin.conf /home/vagrant/.kube/config
  chown vagrant:vagrant /home/vagrant/.kube/config
  chmod 600 /home/vagrant/.kube/config
else
  echo "[ERROR] /etc/kubernetes/admin.conf not found after kubeadm init!"
  exit 1
fi

### --- WAIT FOR KUBE-API TO BE ACCESSIBLE --- ###
echo ">> Waiting for Kubernetes API server to become available"

for i in {1..20}; do
  sudo -u vagrant kubectl get nodes && break
  echo "[INFO] Waiting for API server to respond ($i/20)..."
  sleep 3
done

### --- INSTALLING CALICO CNI --- ###
echo ">> Installing Calico (CNI plugin)"
sudo -u vagrant kubectl apply --validate=false -f https://raw.githubusercontent.com/projectcalico/calico/v3.30.2/manifests/tigera-operator.yaml

# Wait for Calico CRDs to be registered
for i in {1..6}; do
  sudo -u vagrant kubectl get crd installations.operator.tigera.io &>/dev/null && break
  echo "[INFO] Waiting for Calico CRDs to be established ($i/6)..."
  sleep 10
done

# Download and apply Calico config with custom CIDR
echo ">> Applying Calico configuration with custom CIDR (10.10.0.0/16)"
curl -LO https://raw.githubusercontent.com/projectcalico/calico/v3.30.2/manifests/custom-resources.yaml
sed -i 's/cidr: 192\.168\.0\.0\/16/cidr: 10.10.0.0\/16/g' custom-resources.yaml
sudo -u vagrant kubectl apply -f custom-resources.yaml

echo ">> Installing local-path-provisioner"

# Apply the official manifest as the vagrant user (kubectl is configured under their context)
sudo -u vagrant kubectl apply -f https://raw.githubusercontent.com/rancher/local-path-provisioner/master/deploy/local-path-storage.yaml

# Wait briefly to ensure the StorageClass is created
sleep 5

# Set 'local-path' as the default storage class
echo ">> Patching StorageClass to set 'local-path' as default"
sudo -u vagrant kubectl patch storageclass local-path \
  -p '{"metadata": {"annotations":{"storageclass.kubernetes.io/is-default-class":"true"}}}'


### --- SAVE JOIN COMMAND --- ###
echo ">> Generating join command for worker nodes"
kubeadm token create --print-join-command > /vagrant/join.sh
chmod +x /vagrant/join.sh


### --- DONE --- ###
echo ">>>>>>>> [MASTER] Kubernetes master setup complete"