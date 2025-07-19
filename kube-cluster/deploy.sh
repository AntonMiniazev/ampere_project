#!/bin/bash
set -e

KEY_PATH="/home/oppie/gpg_key/private-key.asc"

read -p "Is the private key present at $KEY_PATH? (yes/no): " confirm

if [[ "$confirm" != "yes" ]]; then
  echo "Aborting. Please make sure the key is present before continuing."
  exit 1
fi

echo "Starting Vagrant deployment..."
vagrant up

echo "Removing private key from host..."
rm -f "$KEY_PATH"
echo "Private key removed."