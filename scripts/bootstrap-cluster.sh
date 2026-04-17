#!/usr/bin/env bash
set -euo pipefail

# --- Prerequisite checks ---
for cmd in docker kind kubectl curl sed; do
  if ! command -v "$cmd" &> /dev/null; then
    echo "ERROR: '$cmd' is not installed. Please install it before running this script."
    exit 1
  fi
done

echo "All prerequisites found."

# --- Cluster creation ---
if kind get clusters 2>/dev/null | grep -q "^comhub$"; then
  echo "Kind cluster 'comhub' already exists. Reusing."
else
  echo "Creating kind cluster 'comhub'..."
  kind create cluster --config k8s/kind-config.yaml
fi

# --- Namespaces ---
kubectl create namespace kafka --dry-run=client -o yaml | kubectl apply -f -
echo "Namespace for Kafka created"

# --- Strimzi Kafka operator (pinned to 0.51.0, do not use 'latest') ---
echo "Installing Strimzi operator 0.51.0..."
curl -sL https://github.com/strimzi/strimzi-kafka-operator/releases/download/0.51.0/strimzi-cluster-operator-0.51.0.yaml \
  | sed 's/namespace: .*/namespace: kafka/' \
  | kubectl apply -f - -n kafka
kubectl wait deployment/strimzi-cluster-operator -n kafka --for=condition=Available --timeout=300s

# --- Apply Kafka cluster CR ---
echo "Applying Kafka cluster..."
kubectl apply -f k8s/strimzi/kafka-cluster.yaml -n kafka
kubectl wait kafka/comhub --namespace kafka --for=condition=Ready --timeout=300s
echo "Kafka cluster ready."

# --- Apply Kafka Topics CR ---
echo "Applying topic CRs..."
kubectl apply -f k8s/strimzi/topics/ -n kafka
kubectl wait kafkatopic --all --namespace kafka --for=condition=Ready --timeout=120s
echo "All topics ready."

echo "Bootstrap complete."
