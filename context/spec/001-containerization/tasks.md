# Tasks: Containerization

**Spec:** `context/spec/001-containerization/`
**Rule:** The stack must remain runnable after each slice is completed.

---

## Slice 1: All services start and are healthy

_`make dev` brings everything up and `docker compose ps` shows all containers healthy._

- [x] Add `restart: unless-stopped` and healthchecks for `timescaledb` and `redpanda` in `infra/docker-compose.yml`. Upgrade `depends_on` for `api`, `consumer`, `ml`, and `frontend` to `condition: service_healthy`. **[Agent: general-purpose]**
- [x] Add healthcheck for `api` (`curl -sf http://localhost:8000/health`) in `infra/docker-compose.yml`. **[Agent: general-purpose]**
- [x] Add `/tmp/healthy` sentinel write to `consumer` and `ml` main modules on successful startup (after consumer group join + DB connection verified). Add corresponding `test -f /tmp/healthy` healthchecks in `infra/docker-compose.yml`. **[Agent: general-purpose]**
- [x] Add healthcheck for `frontend` (`curl -sf http://localhost:3000`) in `infra/docker-compose.yml`. **[Agent: general-purpose]**
- [x] Verify: run `make dev`; confirm all 6 containers report healthy in `docker compose ps`; `GET http://localhost:8000/health` returns `{"status":"ok"}`; `http://localhost:3000` loads; `pg_isready -h localhost -U grosh -d grosh` exits 0; `curl -sf http://localhost:9644/v1/node_config` exits 0. **[Agent: general-purpose]**

---

## Slice 2: Hot reload for consumer and ml in dev mode

_Extends the existing dev overlay (api and frontend already have hot reload) to cover the two background workers._

- [x] Add `watchfiles>=0.21` to `[project.optional-dependencies] dev` in `services/consumer/pyproject.toml` and `services/ml/pyproject.toml`. Run `uv sync --all-packages --all-extras` to update `uv.lock`. **[Agent: general-purpose]**
- [x] Add `consumer` and `ml` overrides to `infra/docker-compose.dev.yml`: dev command (`watchfiles --filter python 'python -m grosh_X.main' src/`) and source volume mount (`../services/X/src:/app/src`). **[Agent: general-purpose]**
- [x] Verify: edit a `.py` file in `services/consumer/src/` and confirm the consumer restarts in Docker logs within ~3s; repeat for `ml`. **[Agent: general-purpose]**

---

## Slice 3: Environment variable completeness

_Ensures a fresh clone has everything needed to run with no undocumented variables._

- [x] Audit all `env_file` and `environment:` entries across `infra/docker-compose.yml` and `infra/docker-compose.dev.yml`; confirm every referenced variable is present in `infra/.env.example` with a comment explaining its purpose. **[Agent: general-purpose]**
