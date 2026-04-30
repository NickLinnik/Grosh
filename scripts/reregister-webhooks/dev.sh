#!/usr/bin/env bash
#
# Re-register webhooks in local dev (Docker Compose).
# Runs run.py inside the ingestion container via python -m.
#
# Usage: ./scripts/reregister-webhooks/dev.sh
#        or: make dev-reregister-webhooks

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(dirname "$(dirname "$SCRIPT_DIR")")"

cd "$PROJECT_ROOT"

echo "Re-registering webhooks (dev)..."

docker compose -p grosh \
    -f infra/docker-compose.yml \
    -f infra/docker-compose.dev.yml \
    exec -T -e PYTHONPATH=/app/src ingestion \
    python -m grosh_ingestion.jobs.reregister_webhooks
