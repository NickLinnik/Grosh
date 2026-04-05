# Technical Specification: Containerization

- **Functional Specification:** `context/spec/001-containerization/functional-spec.md`
- **Status:** Completed
- **Author(s):** Nick

---

## 1. High-Level Technical Approach

All four services (`api`, `consumer`, `ml`, `frontend`) already have Dockerfiles and are wired into `infra/docker-compose.yml`. The Makefile targets are also already implemented. The remaining work is:

1. Add Docker healthchecks and restart policies to the base Compose config so `docker compose ps` accurately reflects service health.
2. Add source volume mounts and hot-reload commands for `consumer` and `ml` in the dev overlay (matching what `api` and `frontend` already have).
3. Verify the complete `.env.example` covers all variables consumed by Compose.

---

## 2. Proposed Solution & Implementation Plan

### Docker Compose — Base (`infra/docker-compose.yml`)

Each service needs a `healthcheck` and `restart: unless-stopped`. Proposed checks:

| Service | Healthcheck mechanism | Key interval/timeout |
|---|---|---|
| `timescaledb` | `pg_isready -U $POSTGRES_USER -d $POSTGRES_DB` | start_period: 30s |
| `redpanda` | `curl -sf http://localhost:9644/v1/node_config` | start_period: 20s |
| `api` | `python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')"` | start_period: 10s |
| `consumer` | `test -f /tmp/healthy` (touch file written on startup) | start_period: 15s |
| `ml` | same sentinel file approach as consumer | start_period: 15s |
| `frontend` | `wget -qO- http://localhost:3000 > /dev/null` | start_period: 30s |

**Note:** `python:3.12-slim` does not include `curl` — use Python's `urllib` for the api healthcheck. `node:20-alpine` includes `wget` but not `curl` — use `wget` for the frontend healthcheck.

`depends_on` conditions should be upgraded from the current plain list to `condition: service_healthy` for timescaledb and redpanda, so dependent services wait for them to actually be ready.

### Docker Compose — Dev Overlay (`infra/docker-compose.dev.yml`)

Add overrides for `consumer` and `ml` to match the existing `api` pattern:

| Service | Dev command | Volume mount |
|---|---|---|
| `consumer` | `python -m watchfiles --filter python "python -m grosh_consumer.main" src/` | `../services/consumer/src:/app/src` |
| `ml` | `python -m watchfiles --filter python "python -m grosh_ml.main" src/` | `../services/ml/src:/app/src` |

`watchfiles` is already a transitive dependency of `uvicorn[standard]` — no new package needed for `api`. For `consumer` and `ml`, `watchfiles>=0.21` must be in **main** dependencies (not dev), because it runs inside the Docker image at dev time. The `watchfiles` binary is not on PATH in the container — invoke it as `python -m watchfiles`. The `--filter python` flag prevents restarts on `.pyc` cache writes.

### Makefile

All six targets (`setup`, `dev`, `lint`, `test`, `fmt`, `help`) are already implemented and correct. No changes needed.

### Environment Variables

`infra/.env.example` must declare all variables referenced in Compose files:

| Variable | Used by | Purpose |
|---|---|---|
| `POSTGRES_USER` | timescaledb, api, consumer, ml | DB username |
| `POSTGRES_PASSWORD` | timescaledb, api, consumer, ml | DB password |
| `POSTGRES_DB` | timescaledb, api, consumer, ml | DB name |
| `TUNNEL_TOKEN` | cloudflared (dev only) | Cloudflare tunnel auth |
| `INFISICAL_TOKEN` | api, consumer, ml (future) | Secrets manager auth |
| `INFISICAL_PROJECT_ID` | api, consumer, ml (future) | Secrets manager project |

### Consumer and ML Startup Signal

For the healthcheck sentinel: `consumer` and `ml` main modules write `/tmp/healthy` after successful startup (Kafka consumer group joined, DB connection verified). This is the only application-level change required in this spec.

---

## 3. Implementation Findings (Discovered During Build)

### Docker Build Context Must Be Repo Root for Python Services

`grosh-shared` is referenced as `grosh-shared = { workspace = true }` in `[tool.uv.sources]`. This workspace reference only works inside a uv workspace — it breaks when uv is run inside a Docker build context that only contains the service directory.

**Fix:** Set `context: ..` (repo root) and explicit `dockerfile:` path for `api`, `consumer`, and `ml`. The Dockerfile then `COPY shared/` before installing the service.

### uv `--no-sources` Required for Service Install

Even with shared/ available, uv still reads `[tool.uv.sources]` and tries to resolve `grosh-shared` as a workspace member — which fails inside Docker (no workspace root). The fix: install `shared/` first as a standalone package, then install the service with `--no-sources` to ignore the workspace reference:

```dockerfile
RUN uv pip install --system --no-cache shared/ && \
    uv pip install --system --no-cache --no-sources services/api/
```

### Frontend Requires a `dev` Stage in Its Dockerfile

The production stage uses Next.js standalone output — it deliberately excludes `node_modules`. Running `npm run dev` in the production image fails with `next: not found`. The fix: add a separate `dev` stage that runs `npm ci` and `CMD ["npm", "run", "dev"]`. The dev overlay targets this stage via `build: target: dev`.

### CPU-Only PyTorch for ML Service

`sentence-transformers` pulls in the default PyPI `torch`, which includes ~2GB of NVIDIA CUDA libraries. These are useless on an M-series Mac (dev) and a Hetzner CPU-only VPS (production). Pin CPU-only torch explicitly before installing `sentence-transformers`:

```dockerfile
RUN uv pip install --system --no-cache torch --index-url https://download.pytorch.org/whl/cpu && \
    uv pip install --system --no-cache --no-sources services/ml/
```

### `sentence-transformers` Belongs Only in `ml`, Not `consumer`

The consumer's role is pipeline orchestration: read from Kafka, apply rule/MCC lookups, call the `ml` service for embeddings, write to DB. It should not run inference directly. `sentence-transformers` (and therefore PyTorch) was removed from `consumer`'s dependencies.

### Cloudflare Tunnel Setup

`cloudflared` requires a `TUNNEL_TOKEN` in `infra/.env`. Without it the container exits immediately, causing a benign "network not found" error during `docker compose down` (race condition: network is removed before cloudflared finishes stopping). The token is obtained from the Cloudflare Zero Trust dashboard when creating a named tunnel. A named tunnel provides a stable public hostname — useful for Monobank webhook testing where you don't want to re-register the URL on every restart.

### Compose Project Name

Docker Compose defaults the project name to the directory containing the compose file (`infra`), which shows as "infra" in Docker Desktop. Override with `-p grosh` in all `docker compose` invocations in the Makefile to get `grosh-api-1`, `grosh-timescaledb-1`, etc.

---

## 4. Impact and Risk Analysis

- **System dependencies:** No changes to application logic. Healthchecks and restart policies are purely operational config.
- **`condition: service_healthy` risk:** If timescaledb or redpanda fail their healthcheck repeatedly, dependent services will not start. This is the correct behaviour — fail visibly rather than silently crashing on connection error. Mitigation: set generous `start_period` values to account for cold-start time.
- **`watchfiles` on consumer/ml:** Watches `src/` for changes and restarts the Python process. Safe for dev — does not affect production images. Risk: watchfiles may trigger on `.pyc` cache files; mitigate by watching only `*.py` (watchfiles supports filter patterns).

---

## 4. Testing Strategy

Verification is manual for this item (no unit tests apply to Compose config):

1. `make dev` — all 6 containers come up; `docker compose ps` shows all healthy within ~60s.
2. `GET http://localhost:8000/health` → `{"status": "ok"}`.
3. `http://localhost:3000` loads in browser.
4. `pg_isready -h localhost -U grosh -d grosh` exits 0.
5. `curl -sf http://localhost:9644/v1/node_config` exits 0.
6. Edit `services/api/src/grosh_api/main.py` → uvicorn reloads in logs within 3s.
7. Edit `services/consumer/src/grosh_consumer/main.py` → watchfiles restarts consumer in logs within 3s.
8. Edit `services/frontend/src/app/page.tsx` → Next.js hot-reloads in browser.
