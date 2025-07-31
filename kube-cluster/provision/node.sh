#!/bin/bash
set -e

echo ">> [NODE] Waiting for join.sh to be available from master node"

# Wait until the master generates /vagrant/join.sh
for i in {1..30}; do
  if [ -f /vagrant/join.sh ]; then
    echo "[INFO] join.sh found"
    break
  fi
  echo "[INFO] Waiting for join.sh... ($i/30)"
  sleep 5
done

# Fail after timeout
if [ ! -f /vagrant/join.sh ]; then
  echo "[ERROR] join.sh not found after 30 attempts (~150s)"
  exit 1
fi

### --- PRINT JOIN SCRIPT FOR DEBUG --- ###
echo ">> Contents of /vagrant/join.sh:"
cat /vagrant/join.sh

### --- EXECUTE JOIN --- ###
echo ">> Joining Kubernetes cluster using join.sh"
bash /vagrant/join.sh

echo ">>>>>>>> [NODE] Successfully joined the Kubernetes cluster"