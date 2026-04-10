VENV := .venv
UV   := uv

.PHONY: setup dev migrate lint test fmt help certs hooks

help:
	@echo "Available targets:"
	@echo "  setup - Create venv (Python $(PYTHON_VERSION)) and install all dependencies"
	@echo "  certs - Generate a local mkcert CA and localhost TLS certs for dev HTTPS"
	@echo "  hooks - Install pre-commit and pre-push git hooks"
	@echo "  dev   - Start all services (Docker Compose)"
	@echo "  lint  - Lint Python services and frontend"
	@echo "  test  - Run tests for all services"
	@echo "  migrate - Run database migrations"
	@echo "  fmt   - Format Python services and frontend"

# ── Setup ────────────────────────────────────────────────────────────────────

setup: _copy-env _install-python-deps _install-node-deps certs hooks
	@echo ""
	@echo "Setup complete."
	@echo "Point PyCharm interpreter to: $(PWD)/$(VENV)/bin/python3.12"
	@echo "Edit infra/.env and set a strong POSTGRES_PASSWORD before running 'make dev'."
	@echo "Dev URL: https://localhost"

# Install pre-commit and pre-push hooks. Idempotent — safe to re-run.
hooks:
	@echo "Installing git hooks..."
	$(UV) run pre-commit install --install-hooks
	$(UV) run pre-commit install --hook-type pre-push

# Generate a local mkcert CA and a leaf cert for localhost. One-time per
# machine. The CA gets trusted by your browser/OS via 'mkcert -install'.
# Certs are written to infra/certs/ and gitignored — each dev has their own.
certs:
	@if ! command -v mkcert >/dev/null 2>&1; then \
		echo "mkcert not found. Install it first:"; \
		echo "  macOS:   brew install mkcert"; \
		echo "  Linux:   https://github.com/FiloSottile/mkcert#installation"; \
		exit 1; \
	fi
	@if [ ! -f infra/certs/localhost.pem ]; then \
		echo "Installing mkcert local CA (may prompt for sudo)..."; \
		mkcert -install; \
		mkdir -p infra/certs; \
		cd infra/certs && mkcert -cert-file localhost.pem -key-file localhost-key.pem localhost 127.0.0.1 ::1; \
		echo "Local HTTPS certs created at infra/certs/"; \
	else \
		echo "Local HTTPS certs already present at infra/certs/"; \
	fi

_copy-env:
	@if [ ! -f infra/.env ]; then \
		cp infra/.env.example infra/.env; \
		echo "Created infra/.env from .env.example — update POSTGRES_PASSWORD before use."; \
	fi

_install-python-deps:
	@echo "Installing Python packages..."
	$(UV) sync --all-packages --all-extras

_install-node-deps:
	@echo "Installing Node packages..."
	cd services/frontend && npm install

# ── Dev ──────────────────────────────────────────────────────────────────────

dev:
	docker compose -p grosh -f infra/docker-compose.yml -f infra/docker-compose.dev.yml up

# ── Migrate ──────────────────────────────────────────────────────────────────

migrate:
	docker compose -p grosh -f infra/docker-compose.yml -f infra/docker-compose.dev.yml up -d timescaledb
	docker compose -p grosh -f infra/docker-compose.yml -f infra/docker-compose.dev.yml run --rm --no-deps api alembic -c /app/alembic.ini upgrade head

# ── Lint ─────────────────────────────────────────────────────────────────────

lint:
	$(UV) run ruff check services/api/src
	$(UV) run ruff check services/consumer/src
	$(UV) run ruff check services/ml/src
	$(UV) run mypy -p grosh_api -p grosh_consumer -p grosh_ml
	cd services/frontend && npm run typecheck
	cd services/frontend && npm run lint

# ── Test ─────────────────────────────────────────────────────────────────────

test:
	@# pytest exits with code 5 when no tests are collected — treat that as success
	@# so services without tests yet don't break the chain
	$(UV) run pytest services/api   || [ $$? = 5 ]
	$(UV) run pytest services/consumer || [ $$? = 5 ]
	$(UV) run pytest services/ml    || [ $$? = 5 ]
	cd services/frontend && npm test -- --passWithNoTests

# ── Format ───────────────────────────────────────────────────────────────────

fmt:
	$(UV) run ruff format services/api/src
	$(UV) run ruff format services/consumer/src
	$(UV) run ruff format services/ml/src
	cd services/frontend && npx prettier --write src
