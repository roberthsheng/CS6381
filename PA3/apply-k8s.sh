#!/bin/bash
set -e  # Exit immediately if a command exits with a non-zero status.

# Apply the YAML files one-by-one in the specified order.
echo "Applying k8s/namespace.yaml..."
kubectl apply -f k8s/namespace.yaml

echo "Applying k8s/configmap.yaml..."
kubectl apply -f k8s/configmap.yaml

echo "Applying k8s/discovery-service.yaml..."
kubectl apply -f k8s/discovery-service.yaml

echo "Applying k8s/discovery-deployment.yaml..."
kubectl apply -f k8s/discovery-deployment.yaml

# Finally, apply all the remaining YAML files in the k8s directory.
echo "Applying all remaining YAML files in the k8s directory..."
kubectl apply -f k8s/

echo "All YAML files have been applied in the specified order."
