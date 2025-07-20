#!/bin/bash
set -e

KEY_PATH="/home/oppie/gpg_key/private-key.asc"

ssh-keygen -f '/home/oppie/.ssh/known_hosts' -R 'ampere-k8s-master'
ssh-keygen -f '/home/oppie/.ssh/known_hosts' -R 'ampere-k8s-node1'
ssh-keygen -f '/home/oppie/.ssh/known_hosts' -R 'ampere-k8s-node2'
ssh-keygen -f '/home/oppie/.ssh/known_hosts' -R 'ampere-k8s-node3'

read -p "Is the private key present at $KEY_PATH? (yes/no): " confirm

if [[ "$confirm" != "yes" ]]; then
  echo "Aborting. Please make sure the key is present before continuing."
  exit 1
fi

echo "Starting Vagrant deployment..."
vagrant up

echo "Removing private key from host..."
#rm -f "$KEY_PATH"
echo "Private key removed."

# post-deployment
cd ~/projects/ampere_project/kube-cluster
vagrant ssh ampere-k8s-master
bash /vagrant/provision/post-deployment.sh