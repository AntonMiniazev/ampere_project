#!/bin/bash
set -e

KEY_PATH="/home/gpg_key/private-key.asc"

echo ">> Cleaning up old SSH known_hosts entries"
ssh-keygen -f '/home/oppie/.ssh/known_hosts' -R 'ampere-t-k8s-master' || true
ssh-keygen -f '/home/oppie/.ssh/known_hosts' -R 'ampere-t-k8s-node1' || true
ssh-keygen -f '/home/oppie/.ssh/known_hosts' -R 'ampere-t-k8s-node2' || true
ssh-keygen -f '/home/oppie/.ssh/known_hosts' -R 'ampere-t-k8s-node3' || true

read -p "Is the GPG private key present at $KEY_PATH? (yes/no): " confirm

if [[ "$confirm" != "yes" ]]; then
  echo ">>>>>>>> Aborting. Please make sure the key is present before continuing."
  exit 1
fi

echo ">> Starting Vagrant deployment"
vagrant up

echo ">> Removing GPG private key from host for security"
#rm -f "$KEY_PATH"
echo ">>>>>>>> Private key removed"

echo ">> Running post-deployment script inside ampere-t-k8s-master"
cd ~/projects/ampere_project/kube-cluster
vagrant ssh ampere-t-k8s-master -c "bash /vagrant/other_scripts/post-deployment.sh"

echo ">>>>>>>> Deployment complete"