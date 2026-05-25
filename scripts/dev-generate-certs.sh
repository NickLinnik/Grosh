#!/usr/bin/env bash
#
# Generate mkcert-issued localhost TLS certs for dev HTTPS.
# One-time per machine. The CA gets trusted by your browser/OS.
# Certs are written to infra/certs/ (gitignored).
#
# Usage: ./scripts/generate-certs.sh
#        or: make certs

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
CERT_DIR="${PROJECT_ROOT}/infra/certs"

if ! command -v mkcert >/dev/null 2>&1; then
    echo "Error: mkcert not found. Install it first:"
    echo "  macOS:  brew install mkcert"
    echo "  Linux:  https://github.com/FiloSottile/mkcert#installation"
    exit 1
fi

if [ -f "${CERT_DIR}/localhost.pem" ]; then
    echo "Local HTTPS certs already present at ${CERT_DIR}/"
    exit 0
fi

echo "Installing mkcert local CA (may prompt for sudo)..."
mkcert -install

mkdir -p "${CERT_DIR}"
cd "${CERT_DIR}"
mkcert -cert-file localhost.pem -key-file localhost-key.pem localhost 127.0.0.1 ::1

echo "Local HTTPS certs created at ${CERT_DIR}/"
