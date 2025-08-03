#!/bin/bash
set -e

source ./deploy.env

read -p "Is the GPG private key present at $GPG_LOC? (yes/no): " confirm

if [[ "$confirm" != "yes" ]]; then
  echo ">>>>>>>> Aborting. Please make sure the key is present before continuing."
  exit 1
fi

echo ">> Cleaning up old SSH known_hosts entries"
KNOWN_HOSTS="${HOME}/.ssh/known_hosts"

ssh-keygen -f "$KNOWN_HOSTS" -R "$MASTER_NAME" || true
ssh-keygen -f "$KNOWN_HOSTS" -R "$NODE1_NAME" || true
ssh-keygen -f "$KNOWN_HOSTS" -R "$NODE2_NAME" || true
ssh-keygen -f "$KNOWN_HOSTS" -R "$NODE3_NAME" || true

echo ">> Starting Vagrant deployment"
vagrant up

echo ">> Removing GPG private key from host for security"
#rm -f "$GPG_LOC"
echo ">>>>>>>> Private key removed"

echo ">> Running post-deployment script inside $MASTER_NAME"
vagrant ssh $MASTER_NAME -c "bash /vagrant/other_scripts/post-deployment.sh"

echo ">>>>>>>> Deployment complete"