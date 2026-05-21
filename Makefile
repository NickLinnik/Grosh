VENV := .venv
UV   := uv

.PHONY: setup dev migrate lint test fmt help dev-certs hooks dev-k8s-setup dev-reregister-webhooks

help:
	@echo "Available targets:"
	@echo "  setup - Create venv (Python $(PYTHON_VERSION)) and install all dependencies"
	@echo "  dev-certs               - Generate a local mkcert CA and localhost TLS certs for dev HTTPS"
	@echo "  hooks - Install pre-commit and pre-push git hooks"
	@echo "  dev   - Start all services (Docker Compose)"
	@echo "  lint  - Lint Python services and frontend"
	@echo "  test  - Run tests for all services"
	@echo "  migrate   - Run database migrations"
	@echo "  dev-k8s-setup           - Set up local K8s namespace, secrets, and RBAC for backfill jobs"
	@echo "  dev-reregister-webhooks - Re-register all bank webhooks with current WEBHOOK_BASE_URL"
	@echo "  fmt       - Format Python services and frontend"

# ── Setup ────────────────────────────────────────────────────────────────────

setup: _copy-env _install-python-deps _install-node-deps dev-certs hooks
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

dev-certs:
	./scripts/dev-generate-certs.sh

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
	docker compose -p grosh -f infra/docker-compose.yml -f infra/docker-compose.dev.yml up --build

# ── K8s Local Setup ─────────────────────────────────────────────────────────

dev-k8s-setup:
	./scripts/dev-k8s-setup.sh

dev-reregister-webhooks:
	./scripts/reregister-webhooks/dev.sh

# ── Migrate ──────────────────────────────────────────────────────────────────

migrate:
	docker compose -p grosh -f infra/docker-compose.yml -f infra/docker-compose.dev.yml up -d postgres
	docker compose -p grosh -f infra/docker-compose.yml -f infra/docker-compose.dev.yml run --rm --no-deps api alembic -c /app/alembic.ini upgrade head

# ── Lint ─────────────────────────────────────────────────────────────────────

lint:
	$(UV) run ruff check services/api/src
	$(UV) run ruff check services/ingestion/src
	$(UV) run ruff check services/normalizer/src
	$(UV) run ruff check services/pipeline/src
	$(UV) run ruff check services/ml/src
	$(UV) run mypy -p grosh_api -p grosh_normalizer -p grosh_pipeline -p grosh_ml
	cd services/frontend && npm run typecheck
	cd services/frontend && npm run lint

# ── Test ─────────────────────────────────────────────────────────────────────

test:
	@# pytest exits with code 5 when no tests are collected — treat that as success
	@# so services without tests yet don't break the chain
	$(UV) run pytest services/api        || [ $$? = 5 ]
	$(UV) run pytest services/ingestion  || [ $$? = 5 ]
	$(UV) run pytest services/normalizer || [ $$? = 5 ]
	$(UV) run pytest services/pipeline   || [ $$? = 5 ]
	$(UV) run pytest services/runtime    || [ $$? = 5 ]
	$(UV) run pytest services/ml         || [ $$? = 5 ]
	cd services/frontend && npm test -- --passWithNoTests

# ── Format ───────────────────────────────────────────────────────────────────

fmt:
	$(UV) run ruff format services/api/src
	$(UV) run ruff format services/ingestion/src
	$(UV) run ruff format services/normalizer/src
	$(UV) run ruff format services/pipeline/src
	$(UV) run ruff format services/ml/src
	cd services/frontend && npx prettier --write src
