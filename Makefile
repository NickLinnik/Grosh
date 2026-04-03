VENV := .venv
UV   := uv

.PHONY: setup dev lint test fmt help

help:
	@echo "Available targets:"
	@echo "  setup - Create venv (Python $(PYTHON_VERSION)) and install all dependencies"
	@echo "  dev   - Start all services (Docker Compose)"
	@echo "  lint  - Lint Python services and frontend"
	@echo "  test  - Run tests for all services"
	@echo "  fmt   - Format Python services and frontend"

# ── Setup ────────────────────────────────────────────────────────────────────

setup: _copy-env _install-python-deps _install-node-deps
	@echo ""
	@echo "Setup complete."
	@echo "Point PyCharm interpreter to: $(PWD)/$(VENV)/bin/python3.12"
	@echo "Edit infra/.env and set a strong POSTGRES_PASSWORD before running 'make dev'."

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
	docker compose -f infra/docker-compose.yml -f infra/docker-compose.dev.yml up

# ── Lint ─────────────────────────────────────────────────────────────────────

lint:
	$(UV) run ruff check services/api/src
	$(UV) run ruff check services/consumer/src
	$(UV) run ruff check services/ml/src
	$(UV) run mypy services/api/src services/consumer/src services/ml/src
	cd services/frontend && npm run typecheck
	cd services/frontend && npm run lint

# ── Test ─────────────────────────────────────────────────────────────────────

test:
	$(UV) run pytest services/api
	$(UV) run pytest services/consumer
	$(UV) run pytest services/ml
	cd services/frontend && npm test -- --passWithNoTests

# ── Format ───────────────────────────────────────────────────────────────────

fmt:
	$(UV) run ruff format services/api/src
	$(UV) run ruff format services/consumer/src
	$(UV) run ruff format services/ml/src
	cd services/frontend && npx prettier --write src
