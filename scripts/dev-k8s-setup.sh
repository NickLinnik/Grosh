#!/usr/bin/env bash
#
# Sets up a local Kubernetes environment for backfill jobs.
# Requires: Docker Desktop with Kubernetes enabled (using dockerd, not containerd).
#
# What it does:
#   1. Verifies K8s is running
#   2. Creates the grosh namespace and RBAC
#   3. Creates K8s secrets from infra/.env (with host.docker.internal substitution)
#   4. Generates a docker-friendly kubeconfig for the ingestion container
#   5. Builds the ingestion Docker image
#
# Usage: ./scripts/k8s-setup.sh
#        or: make k8s-setup

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
NAMESPACE="grosh"
IMAGE_NAME="grosh-ingestion:latest"

cd "$PROJECT_ROOT"

# ── 1. Verify K8s ───────────────────────────────────────────────────────────

if ! kubectl cluster-info >/dev/null 2>&1; then
    echo "Error: Kubernetes not running."
    echo "Enable it in Docker Desktop → Settings → Kubernetes."
    exit 1
fi

if [ ! -f infra/.env ]; then
    echo "Error: infra/.env not found. Run 'make setup' first."
    exit 1
fi

# ── 2. Namespace and RBAC ───────────────────────────────────────────────────

echo "Creating namespace and RBAC..."
kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f infra/k8s/rbac/

# ── 3. Secrets ──────────────────────────────────────────────────────────────
# Read infra/.env, substitute Docker Compose hostnames with host.docker.internal,
# and override INGESTION_IMAGE to use the local Docker image directly.

echo "Creating K8s secrets from infra/.env..."
kubectl delete secret grosh-secrets -n "$NAMESPACE" --ignore-not-found

sed \
    -e 's/timescaledb/host.docker.internal/g' \
    -e 's/redpanda:9092/host.docker.internal:29092/g' \
    -e "s|^INGESTION_IMAGE=.*|INGESTION_IMAGE=${IMAGE_NAME}|" \
    infra/.env \
    | grep -v '^#' | grep -v '^$' \
    > /tmp/grosh-k8s-env

kubectl create secret generic grosh-secrets -n "$NAMESPACE" --from-env-file=/tmp/grosh-k8s-env
rm -f /tmp/grosh-k8s-env

# ── 4. Kubeconfig for Docker ────────────────────────────────────────────────
# The ingestion container (Docker Compose) needs to reach the K8s API.
# Docker Desktop K8s listens on 127.0.0.1 which is unreachable from containers.
# Generate a kubeconfig with host.docker.internal and skip TLS verification
# (the K8s cert is issued for 127.0.0.1, not host.docker.internal).

echo "Generating infra/kubeconfig.docker..."
sed -e 's|127\.0\.0\.1|host.docker.internal|g' \
    -e '/certificate-authority-data/d' \
    ~/.kube/config \
    | sed '/server:/a\'$'\n''    insecure-skip-tls-verify: true' \
    > infra/kubeconfig.docker

# ── 5. Build and load ingestion image ───────────────────────────────────────
# Docker Desktop K8s always uses containerd for pods, even when dockerd is the
# CLI backend. Local images from `docker build` are not visible to K8s.
# We build with docker, then load into containerd via `ctr import`.

echo "Building ingestion image..."
docker compose -p grosh -f infra/docker-compose.yml build ingestion

if docker exec desktop-control-plane true 2>/dev/null; then
    echo "Loading image into Docker Desktop K8s node (containerd)..."
    docker save "${IMAGE_NAME}" \
        | docker exec -i desktop-control-plane ctr -n k8s.io images import -
else
    echo "Warning: Could not detect Docker Desktop K8s node."
    echo "You may need to push ${IMAGE_NAME} to a registry."
fi

# ── Done ────────────────────────────────────────────────────────────────────

echo ""
echo "K8s local setup complete."
echo "  - Namespace:  ${NAMESPACE}"
echo "  - Image:      ${IMAGE_NAME} (local, no registry)"
echo "  - Kubeconfig: infra/kubeconfig.docker"
echo ""
echo "Run 'make dev' to start the stack."
