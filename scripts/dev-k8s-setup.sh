#!/usr/bin/env bash
#
# Sets up a local Kubernetes environment for Grosh background Jobs (backfill +
# reprocess). Idempotent — safe to re-run after volume drops, image rebuilds,
# or .env changes.
#
# Requires: Docker Desktop with Kubernetes enabled.
#
# What it does:
#   1. Verifies K8s is running and infra/.env exists
#   2. Verifies all required env keys are present (fails loudly if missing)
#   3. Creates the grosh namespace and applies RBAC manifests
#   4. Builds the ingestion + normalization + enrichment Docker images
#   5. Imports all three into Docker Desktop's containerd (so K8s pods see them)
#   6. Creates two K8s Secrets — one per credential set:
#        - grosh-secrets-ingestion (DATABASE_URL = grosh_ingestion creds; for backfill jobs)
#        - grosh-secrets-consumer  (DATABASE_URL = grosh_consumer creds;  for reprocess jobs)
#      Each secret carries the shared keys (Kafka bootstrap, JWT secret, etc.)
#      plus its own DATABASE_URL. The two-secret split exists because backfill
#      and reprocess Jobs need different DB privileges (backfill writes to
#      accounts/bank_integrations/currency_rates as grosh_ingestion; reprocess
#      writes to transactions and holds advisory locks as grosh_consumer).
#   7. Generates a docker-friendly kubeconfig for the ingestion container
#
# Usage: ./scripts/dev-k8s-setup.sh
#        or: make dev-k8s-setup

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
NAMESPACE="grosh"
INGESTION_IMAGE="grosh-ingestion:latest"
NORMALIZATION_IMAGE="grosh-normalization:latest"
ENRICHMENT_IMAGE="grosh-enrichment:latest"
# Reprocess Jobs run `python -m grosh_normalization.reprocess_main`, so they use
# the normalization image — not enrichment.
REPROCESS_IMAGE_TAG="$NORMALIZATION_IMAGE"

cd "$PROJECT_ROOT"

# ── 1. Pre-flight: K8s + .env ───────────────────────────────────────────────

if ! kubectl cluster-info >/dev/null 2>&1; then
    echo "Error: Kubernetes not running."
    echo "Enable it in Docker Desktop → Settings → Kubernetes."
    exit 1
fi

if [ ! -f infra/.env ]; then
    echo "Error: infra/.env not found. Run 'make setup' first."
    exit 1
fi

# ── 2. Verify required env keys ─────────────────────────────────────────────

REQUIRED_KEYS=(
    DATABASE_URL_INGESTION
    DATABASE_URL_CONSUMER
    KAFKA_BOOTSTRAP_SERVERS
    JWT_SECRET
    ENCRYPTION_KEY
    ADMIN_EMAIL
    ADMIN_PASSWORD
    POSTGRES_USER
    POSTGRES_PASSWORD
    POSTGRES_DB
)

missing=()
for key in "${REQUIRED_KEYS[@]}"; do
    if ! grep -qE "^${key}=" infra/.env; then
        missing+=("$key")
    fi
done
if [ ${#missing[@]} -gt 0 ]; then
    echo "Error: missing required keys in infra/.env:"
    for k in "${missing[@]}"; do echo "  - $k"; done
    exit 1
fi

# ── 3. Namespace and RBAC ───────────────────────────────────────────────────

echo "Creating namespace and RBAC..."
kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f infra/k8s/rbac/

# ── 4 + 5. Build + import both images ───────────────────────────────────────
# Docker Desktop K8s uses containerd for pods. Local images from `docker build`
# or `docker compose build` are not visible to K8s by default — they live in
# dockerd's image store. We build with Compose, then load into containerd via
# `ctr import`. Compose tags images as `grosh-${service}:latest` (the project
# name is `grosh`), which already matches the K8s-expected names.

build_and_import() {
    local service="$1"
    local image="$2"

    echo "Building ${service} image..."
    docker compose -p grosh -f infra/docker-compose.yml build "$service"

    if ! docker exec desktop-control-plane true 2>/dev/null; then
        echo "Warning: Could not detect Docker Desktop K8s node (desktop-control-plane)."
        echo "Skipping containerd import for ${image} — pods may fail to pull."
        return
    fi

    echo "Importing ${image} into containerd..."
    docker save "${image}" \
        | docker exec -i desktop-control-plane ctr -n k8s.io images import -
}

build_and_import ingestion     "$INGESTION_IMAGE"
build_and_import normalization "$NORMALIZATION_IMAGE"
build_and_import enrichment    "$ENRICHMENT_IMAGE"

# ── 6. Two secrets, one per credential set ──────────────────────────────────
# We generate two K8s Secrets so backfill and reprocess Jobs each see a
# DATABASE_URL pointing to the right per-service Postgres role. Substitute
# Compose hostnames with host.docker.internal so K8s pods reach the host's
# Postgres + Redpanda containers.

generate_secret() {
    local secret_name="$1"
    local database_url_source="$2"  # e.g. DATABASE_URL_INGESTION

    local tmpfile
    tmpfile=$(mktemp -t grosh-k8s-env.XXXXXX)
    # shellcheck disable=SC2064  # expand $tmpfile now, not on EXIT
    trap "rm -f $tmpfile" EXIT

    sed \
        -e "s|postgres:5432|host.docker.internal:5432|g" \
        -e "s|redpanda:9092|host.docker.internal:29092|g" \
        -e "s|^INGESTION_IMAGE=.*|INGESTION_IMAGE=${INGESTION_IMAGE}|" \
        -e "s|^REPROCESS_IMAGE=.*|REPROCESS_IMAGE=${REPROCESS_IMAGE_TAG}|" \
        infra/.env \
        | grep -v '^#' | grep -v '^$' \
        > "$tmpfile"

    # Append/overwrite a plain DATABASE_URL pointing to the chosen per-service role.
    local url
    url=$(grep "^${database_url_source}=" infra/.env | head -1 | cut -d= -f2-)
    # Apply the same hostname substitution as the rest of the file.
    url=$(echo "$url" | sed -e "s|@postgres:|@host.docker.internal:|g")
    # Remove any stale DATABASE_URL line and append the canonical one.
    sed -i.bak '/^DATABASE_URL=/d' "$tmpfile" && rm -f "${tmpfile}.bak"
    echo "DATABASE_URL=${url}" >> "$tmpfile"

    echo "Creating secret ${secret_name} (DATABASE_URL from ${database_url_source})..."
    kubectl delete secret "$secret_name" -n "$NAMESPACE" --ignore-not-found >/dev/null
    kubectl create secret generic "$secret_name" \
        -n "$NAMESPACE" \
        --from-env-file="$tmpfile"

    rm -f "$tmpfile"
    trap - EXIT
}

generate_secret grosh-secrets-ingestion DATABASE_URL_INGESTION
generate_secret grosh-secrets-consumer  DATABASE_URL_CONSUMER

# ── 7. Kubeconfig for Docker ────────────────────────────────────────────────
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

# ── Done ────────────────────────────────────────────────────────────────────

echo ""
echo "K8s local setup complete."
echo "  - Namespace:           ${NAMESPACE}"
echo "  - Ingestion image:     ${INGESTION_IMAGE} (imported into containerd)"
echo "  - Normalization image: ${NORMALIZATION_IMAGE} (imported into containerd; also used by reprocess Jobs)"
echo "  - Enrichment image:    ${ENRICHMENT_IMAGE} (imported into containerd)"
echo "  - Secrets:             grosh-secrets-ingestion, grosh-secrets-consumer"
echo "  - Kubeconfig:          infra/kubeconfig.docker"
echo ""
echo "Run 'make dev' to start the stack."
