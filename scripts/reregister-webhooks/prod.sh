#!/usr/bin/env bash
#
# Re-register webhooks in production (k3s).
# Runs run.py inside the ingestion pod via kubectl exec.
#
# Usage: ./scripts/reregister-webhooks/prod.sh

set -euo pipefail

NAMESPACE="${K8S_NAMESPACE:-grosh}"

echo "Re-registering webhooks (prod)..."

POD=$(kubectl get pods -n "$NAMESPACE" -l app=grosh-ingestion -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)

if [ -z "$POD" ]; then
    echo "Error: No ingestion pod found in namespace ${NAMESPACE}"
    exit 1
fi

kubectl exec -n "$NAMESPACE" "$POD" -- \
    python -m grosh_ingestion.jobs.reregister_webhooks
