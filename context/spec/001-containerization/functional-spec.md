# Functional Specification: Containerization

- **Roadmap Item:** Infrastructure & Auth → Containerization
- **Status:** Completed
- **Author:** Nick

---

## 1. Overview and Rationale (The "Why")

Running the full Grosh stack locally requires coordinating 6 services (TimescaleDB, Redpanda, api, consumer, ml, frontend) with correct network connectivity, environment config, and startup order. Without containerization, each developer must manually install and configure each service, which is error-prone and not reproducible.

This item delivers a complete local development environment where the entire stack — including hot reload and the Cloudflare tunnel — starts with `make dev` and behaves identically across machines.

**Success:** A developer can clone the repo, copy `.env.example`, run `make dev`, and reach a running, fully editable stack within minutes — with no manual service installation.

---

## 2. Functional Requirements (The "What")

- **As a developer, I want to start the full stack with `make dev`** so that I don't need to install or configure any service manually.
  - **Acceptance Criteria:**
    - [x] `make dev` starts all 6 services: TimescaleDB, Redpanda, api, consumer, ml, and frontend.
    - [x] All containers report healthy in `docker compose ps` after startup.
    - [x] All services start without error when `infra/.env` exists (copied from `.env.example`).
    - [x] `GET http://localhost:8000/health` returns `{"status": "ok"}` (HTTP 200).
    - [x] `http://localhost:3000` is reachable and serves the Next.js app.
    - [x] TimescaleDB accepts a connection (`pg_isready` passes).
    - [x] Redpanda admin API (port 9644) returns HTTP 200.
    - [x] Consumer and ml containers log a startup confirmation message on boot without crashing.

- **As a developer, I want hot reload in dev mode** so that code changes are reflected without restarting containers.
  - **Acceptance Criteria:**
    - [x] Editing a `.py` file in `services/api/src/` causes uvicorn to reload within a few seconds.
    - [x] Editing a `.tsx` file in `services/frontend/src/` causes Next.js to hot-reload in the browser.

- **As a developer, I want each service to be independently buildable** so that I can rebuild one service without touching the others.
  - **Acceptance Criteria:**
    - [x] Each service (`api`, `consumer`, `ml`, `frontend`) has its own Dockerfile.
    - [x] `docker compose build <service>` for any single service completes without errors.

- **As a developer, I want a complete local dev setup command** so that a fresh clone is ready to run in one step.
  - **Acceptance Criteria:**
    - [x] `make setup` installs all Python and Node dependencies into the local environment.
    - [x] `make dev` is the single command to start the full stack after setup.
    - [x] `make lint` and `make test` run their respective checks across all services.

---

## 3. Scope and Boundaries

### In-Scope
- Dockerfile for each of the 4 services: `api`, `consumer`, `ml`, `frontend`
- `infra/docker-compose.yml` — base stack (all services + infra)
- `infra/docker-compose.dev.yml` — dev overlay: hot reload + cloudflared
- Makefile targets: `setup`, `dev`, `lint`, `test`, `fmt`, `help`
- Service networking, startup order (healthchecks / depends_on), and env var wiring

### Out-of-Scope
- Terraform and k3s manifests — part of Go Live milestone
- CI/CD build pipeline — separate roadmap item
- Production-hardened images (multi-stage, non-root users) — deferred to Go Live
- User auth, Monobank webhook, any application-level features
