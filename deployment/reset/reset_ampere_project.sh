#!/bin/bash

set -e

# Absolute path to the script directory
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CONFIG_FILE="$SCRIPT_DIR/deploy.env"

# Check if the env file exists
if [ ! -f "$CONFIG_FILE" ]; then
  echo "Error: $CONFIG_FILE not found."
  exit 1
fi

# Load environment variables
set -a
source "$CONFIG_FILE"
set +a

# Check required variables
REQUIRED_VARS=("NAMESPACE" "NAMESPACE_INGRESS")
for var in "${REQUIRED_VARS[@]}"; do
  if [ -z "${!var}" ]; then
    echo "Error: $var is not set in $CONFIG_FILE"
    exit 1
  fi
done

echo "Deleting namespace: $NAMESPACE..."
kubectl delete namespace $NAMESPACE --ignore-not-found

echo "Waiting for namespace deletion to complete..."
while kubectl get namespace $NAMESPACE >/dev/null 2>&1; do
  echo -n "."
  sleep 2
done
echo "Namespace deleted."

echo "Deleting namespace: $NAMESPACE_INGRESS..."
kubectl delete namespace $NAMESPACE_INGRESS --ignore-not-found

echo "Waiting for namespace deletion to complete..."
while kubectl get namespace $NAMESPACE_INGRESS >/dev/null 2>&1; do
  echo -n "."
  sleep 2
done
echo "Namespace deleted."

read -p "Do you want to remove all stopped Docker containers? (y/n): " rm_containers
if [[ "$rm_containers" == "y" ]]; then
  echo "Stopping and removing all Docker containers..."
  docker stop $(docker ps -aq) 2>/dev/null || true
  docker rm $(docker ps -aq) 2>/dev/null || true
fi

read -p "Do you want to remove all unused Docker images? (y/n): " rm_images
if [[ "$rm_images" == "y" ]]; then
  echo "Removing unused Docker images..."
  docker image prune -a -f
fi

echo "Cleanup complete."
