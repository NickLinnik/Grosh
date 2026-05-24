VENV := .venv
UV   := uv

.PHONY: setup dev migrate lint test test-e2e test-stack-up test-stack-down test-stack-nuke fmt help dev-certs hooks dev-k8s-setup dev-reregister-webhooks

help:
	@echo "Available targets:"
	@echo "  setup - Create venv (Python $(PYTHON_VERSION)) and install all dependencies"
	@echo "  dev-certs               - Generate a local mkcert CA and localhost TLS certs for dev HTTPS"
	@echo "  hooks - Install pre-commit and pre-push git hooks"
	@echo "  dev   - Start all services (Docker Compose)"
	@echo "  lint  - Lint Python services and frontend"
	@echo "  test  - Run unit + integration tests for all services (no e2e)"
	@echo "  test-stack-up           - Start the parallel e2e test stack (postgres + redpanda + apps on ports 5433/19093/8010/8011)"
	@echo "  test-stack-down         - Stop the test stack (preserves volumes / DB content)"
	@echo "  test-stack-nuke         - Stop and remove all test stack data (full reset)"
	@echo "  test-e2e                - Run e2e suite against the test stack (requires test-stack-up first)"
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
	$(UV) run ruff check services/normalization/src
	$(UV) run ruff check services/enrichment/src
	$(UV) run ruff check services/ml/src
	$(UV) run mypy -p grosh_api -p grosh_normalization -p grosh_enrichment -p grosh_ml
	cd services/frontend && npm run typecheck
	cd services/frontend && npm run lint

# ── Test ─────────────────────────────────────────────────────────────────────

test:
	@# Unit + integration only — e2e requires the dev stack and a separate
	@# target. pytest exits with code 5 when no tests collected; treat as success.
	$(UV) run pytest services/api        || [ $$? = 5 ]
	$(UV) run pytest services/ingestion  || [ $$? = 5 ]
	$(UV) run pytest services/normalization || [ $$? = 5 ]
	$(UV) run pytest services/enrichment   || [ $$? = 5 ]
	$(UV) run --project tests/e2e pytest tests/e2e/integration tests/e2e/unit || [ $$? = 5 ]
	$(UV) run pytest services/ml         || [ $$? = 5 ]
	cd services/frontend && npm test -- --passWithNoTests

# ── E2E test stack ───────────────────────────────────────────────────────────
#
# The e2e test stack is a separate docker-compose project (`grosh-test`) that
# runs in parallel with the dev stack. Different ports, different volumes,
# different DB — so dev work is never disrupted by tests.
#
# Bring up sequence:
#   1. Start postgres + redpanda (fresh volumes if test-stack-nuke was used)
#   2. Run alembic migrations against the test postgres (idempotent — alembic_version
#      is checked, only missing revisions are applied)
#   3. Start app services (api, ingestion, normalization, enrichment, ml)
#
# Subsequent test-stack-up calls are idempotent: containers already running
# stay up, only the migration step re-runs (fast no-op if at head).

# Shorthand for the compose command. Layers the test override over the base
# compose file under the `grosh-test` project name — same pattern as `make
# dev`, which layers docker-compose.dev.yml over docker-compose.yml under the
# `grosh` project name. The --env-file flag sources POSTGRES_*, DATABASE_URL_*,
# etc. from infra/.env.test instead of infra/.env.
TEST_COMPOSE := docker compose -p grosh-test -f infra/docker-compose.yml -f infra/docker-compose.test.yml --env-file infra/.env.test

test-stack-up:
	@# Step 1: bring up infra (postgres + redpanda) and wait for healthy.
	$(TEST_COMPOSE) up --build -d postgres redpanda
	@# Step 2: run migrations. Uses the api image's alembic binary. The
	@# --env-file flag passes the test stack's DATABASE_URL_ADMIN and
	@# GROSH_*_DB_PASSWORD into the migration container for role creation.
	$(TEST_COMPOSE) run --rm --no-deps api alembic -c /app/alembic.ini upgrade head
	@# Step 3: pre-create the Kafka topics consumers subscribe to. Without
	@# this, normalization + enrichment crash-loop on UNKNOWN_TOPIC_OR_PART
	@# until the conftest creates them on its first test. Idempotent — rpk
	@# returns 0 even if a topic already exists.
	$(TEST_COMPOSE) exec redpanda rpk topic create raw_transactions.monobank raw_transactions.manual normalized_transactions 2>/dev/null || true
	@# Step 4: start the app services. depends_on healthchecks ensure
	@# postgres + redpanda are ready when these come up.
	$(TEST_COMPOSE) up --build -d api ingestion normalization enrichment ml
	@echo ""
	@echo "Test stack up. Endpoints:"
	@echo "  api:        http://localhost:8010/health"
	@echo "  ingestion:  http://localhost:8011/health"
	@echo "  postgres:   localhost:5433  (db=grosh)"
	@echo "  redpanda:   localhost:19093"

test-stack-down:
	$(TEST_COMPOSE) stop
	$(TEST_COMPOSE) rm -f

test-stack-nuke:
	$(TEST_COMPOSE) down -v --remove-orphans

test-e2e:
	$(UV) run --project tests/e2e pytest tests/e2e/services -v

# ── Format ───────────────────────────────────────────────────────────────────

fmt:
	$(UV) run ruff format services/api/src
	$(UV) run ruff format services/ingestion/src
	$(UV) run ruff format services/normalization/src
	$(UV) run ruff format services/enrichment/src
	$(UV) run ruff format services/ml/src
	cd services/frontend && npx prettier --write src
