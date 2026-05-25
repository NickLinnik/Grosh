# System Architecture Overview: Grosh

---

## 1. Application & Technology Stack

- **Main API:** Python + FastAPI — async, Pydantic models, OpenAPI docs out of the box. Serves read endpoints for the frontend (transaction queries, aggregates, account listing). Owns the auth system (JWT issuance, refresh token rotation, user management). No Redpanda dependency.
- **Ingestion Service:** Python + FastAPI — thin gateway for all pipeline-feeding writes. Receives bank webhooks, handles account linking, manual transaction entry, and backfill triggers. Validates webhook authenticity, resolves user/account from DB, and publishes **raw bank payloads** (not normalized) to per-source Redpanda topics. Validates JWTs for authenticated endpoints but does not issue tokens.
- **Consumer Service:** Python worker — two-stage pipeline:
  - **Normalization consumer:** subscribes to all `raw_transactions.*` topics, dispatches to per-source `NormalizationStrategy`, publishes `NormalizedTransaction` to `normalized_transactions` topic.
  - **Pipeline consumer:** subscribes to `normalized_transactions`, runs layered processing: transfer detection → currency conversion → classification → persistence. Each layer uses Strategy pattern dispatch (per-source where needed, source-agnostic where not).
- **Frontend:** Next.js (App Router) — SSR, React ecosystem. Single unified UI for transaction feed, manual entry, feedback loop, forecast view, net worth dashboard, and family aggregate view.
- **UI Components:** shadcn/ui — accessible component library for fast, consistent UI.
- **Charts:** Recharts or Tremor — financial time-series charts, net worth, category breakdowns.
- **Data Fetching:** TanStack Query — async data fetching, caching, and background revalidation on the frontend.
- **Source Registry:** Provider registries in `registry.py` — `RATE_PROVIDERS` (dict of `RateProviderConfig` with optional `fetch_historical`) and `TRANSACTION_BACKFILL_PROVIDERS` (dict of `TransactionBackfillProvider`). Both `main.py` and standalone backfill K8s Jobs import from the same registry. Adding a new bank = adding entries here + source-specific strategies in the consumer; no generic code changes.
- **Message Broker:** Redpanda — Kafka-compatible broker (single binary, no ZooKeeper/JVM). Topics:
  - `raw_transactions.{source}` — per-source topics with raw bank payloads (key: `user_id`)
  - `normalized_transactions` — source-agnostic intermediate topic consumed by pipeline consumer
  - Future: `user_notifications` (SSE push)
- **GraphQL (future):** Strawberry — Python GraphQL library, added alongside REST when the dashboard constructor feature is built.

---

## 2. Data & Persistence

- **Primary Database:** PostgreSQL 18 with `pgvector`, `pg_cron`, and `pg_stat_statements` extensions. Plain tables with B-tree indexes. No TimescaleDB (see `adr-drop-timescaledb.md` for rationale — composite PK tax, watermark footgun, 12x image bloat with no performance benefit at our scale).
- **Vector Store:** pgvector extension — stores sentence-transformer embeddings for k-NN transaction classification.
- **TTL Cleanup:** `pg_cron` — in-database scheduled job purges expired revoked tokens every 5 minutes. Replaces TimescaleDB retention policies.
- **Query Observability:** `pg_stat_statements` — per-query execution stats (calls, total/mean/max time, rows, buffer hits/misses). Loaded at startup via `shared_preload_libraries`. Used for ad-hoc profiling today, planned Grafana wiring for ongoing observability.
- **Aggregations:** Computed on read. Single SQL query with `date_trunc` + `FILTER` clauses. Sub-ms at per-user scale with `INDEX (user_id, time DESC)`. Supports any bucket (day/week/month/quarter/year), arbitrary date ranges, per-currency selection, and `converted_pct` for data quality visibility.
- **Row-Level Security:** Enabled on all user-scoped tables. User-facing services (API, ingestion) set `app.current_user_id` session variable from JWT claim at request start; the database enforces isolation at query level. Consumer operates above RLS (full table access for background processing).
- **Currency Rates:** SCD Type 2 `currency_rates` table — per-source (Monobank, NBU), per-pair rates with `valid_from`/`valid_to` windows. Two independent sequences: polled rows (with `last_polled_at`, `update_cadence_seconds`) and historical rows (explicit time windows). `rate_source_config` table defines fallback chains (monobank → nbu) and base pivot currencies per source. Consumer resolves rates via two quality tiers: FRESH (actively monitored at transaction time) then CLOSEST (nearest within 7-day window). Historical rates backfillable from NBU via K8s Job.
- **Key Tables:** `users`, `refresh_tokens`, `revoked_tokens`, `networks`, `network_members`, `user_settings`, `bank_integrations`, `accounts`, `counterparties`, `transactions` (regular table, PK on `id`), `transfer_match_anomalies`, `currency_rates` (SCD2), `rate_source_config`, `reprocessing_locks`, `reprocessing_backups`, `transaction_embeddings` (pgvector), `merchant_rules`, `categories`, `scheduled_events`, `sharing_permissions`, `ml_labels`.
- **Caching:** No dedicated cache layer. Compute-on-read aggregations are sub-ms; TanStack Query manages client-side freshness. Sufficient for the ~3-user scale.
- **Docker Image:** Custom `pgvector/pgvector:pg16` + `postgresql-16-cron` (~450 MB vs 5.4 GB for timescaledb-ha).

---

## 3. ML & Forecasting

- **Transaction Classifier — three-tier pipeline:**
  1. **Rule lookup:** `merchant_rules` table maps normalized merchant / counterparty IBAN → category. ~80% coverage once populated. 100% accurate.
  2. **MCC fallback:** ISO 18245 MCC code → coarse category. Free signal, no model needed.
  3. **Embedding classifier:** multilingual MiniLM (sentence-transformers) embeddings stored in pgvector; k-NN similarity search against labeled transactions. Handles unknown merchants.
- **Active learning loop:** Unclassified / low-confidence transactions surfaced in UI. User confirms or corrects. High-confidence recurring merchants auto-promoted to rules. Labeled samples accumulate for periodic retraining.
- **Classifier evolution:** Start with sklearn k-NN (Level 1). Promote to fine-tuned transformer when labeled dataset reaches ~500 examples (Level 3).
- **Cash Flow Forecasting:**
  - **Deterministic component:** `scheduled_events` table (salary, rent, subscriptions) projected forward as exact future cash flows.
  - **Probabilistic component:** Prophet (NeuralProphet once history > 12 months) on monthly category aggregates. Outputs expected spend per category with confidence intervals.
  - **Combined view:** Scheduled events + probabilistic variable spend. Configurable horizon: 90-day default, up to 1 year as history accumulates.

---

## 4. Infrastructure & Deployment

- **Cloud Provider:** Hetzner (CX31 — 2 vCPU, 8 GB RAM, 80 GB SSD, ~€9/month). Single node.
- **All services in k3s:** Lightweight single-node Kubernetes. Stateless services (main API, ingestion, consumer, ML, Next.js) as Deployments. Stateful services (PostgreSQL, Redpanda) as StatefulSets with PVCs. Traefik ingress bundled with k3s. Docker Compose used for local dev only.
- **Reverse proxy / TLS:** Caddy or Traefik with automatic Let's Encrypt certificates.
- **Provisioning:** Terraform — Hetzner VPS, DNS records, firewall rules, SSH key injection. Full "deployable from scratch" in one command.
- **CI/CD:** GitHub Actions — build → push to ghcr.io → SSH deploy → rolling restart.
- **Secrets management:** Infisical (self-hosted). No `.env` files in repos. Secrets injected at runtime.
- **Local dev webhook tunnel:** Cloudflare Tunnel (`cloudflared`) — exposes webhook endpoint to Monobank without port forwarding. Dev Compose profile only; removed in production.

---

## 5. Security

- **Network:** Only ports 80/443 exposed publicly. All inter-service traffic on internal Docker/k3s network. Hetzner firewall + ufw block everything else.
- **TLS:** Automatic via Caddy / Traefik + Let's Encrypt.
- **Auth:** JWT with refresh token rotation. Access tokens: 15-min TTL. Refresh tokens: 30 days, `httpOnly` cookies. No public registration — admin creates users manually. The main API owns token issuance and refresh; the ingestion service validates tokens using the shared `JWT_SECRET` but does not issue them.
- **Token revocation:** `revoked_tokens` table (plain table, PK on `jti`). `pg_cron` purges expired entries every 5 minutes. API checks revocation on every authenticated request.
- **Monobank token storage:** Encrypted in DB with pgcrypto `pgp_sym_encrypt`. App encryption key stored in Infisical, never in code or environment variables.
- **Data isolation:** PostgreSQL Row-Level Security on all user-scoped tables. Enforced at the database layer, independent of application logic. Two database roles: `grosh_app` (RLS enforced, used by API and ingestion services) and `grosh_admin` (table owner, RLS bypassed, used by migrations and consumer).
- **Family aggregates:** `sharing_permissions` table controls visibility. Admin sees category-level aggregates by default; raw transactions require explicit per-user grant.

---

## 6. Observability & Monitoring

- **Logs:** Loki — structured JSON logs from all services, aggregated and queryable.
- **Metrics:** Prometheus — scrapes FastAPI (`/metrics`), k3s node exporter, PostgreSQL exporter, Redpanda metrics endpoint.
- **Dashboards:** Grafana — time-series dashboards for API latency, consumer lag, DB query times, forecast job duration.
- **Alerting:** Grafana alerting rules for consumer lag spikes, high error rates, and disk usage thresholds.

---

## 7. External Integrations

- **Monobank API:** Personal token auth (`X-Token` header). Two ingestion paths, both publishing raw bank payloads to `raw_transactions.monobank`:
  - **Webhook** — real-time push for new transactions. Monobank calls `POST /webhook/monobank` on the ingestion service; the service validates, resolves user/account, and publishes the raw payload + routing envelope to Redpanda. Normalization happens in the consumer.
  - **Historical backfill** — `POST /monobank/accounts/{id}/backfill` on the ingestion service triggers a K8s Job. The Job paginates the statement API (31 days/request, 61s rate limit) and publishes each batch to Redpanda. Consumer deduplicates by deterministic transaction ID — safe to re-run.
  - **Historical rate backfill** — `POST /admin/rates-backfill` triggers a K8s Job that dispatches to the matching rate provider's `fetch_historical()` via the registry. Currently NBU supports historical rate backfill (~45 currencies per date range).
- **Future bank integrations:** Architecture designed for extension via Strategy pattern — new bank sources add files under `sources/{name}/` in ingestion + normalization/detection strategies in consumer, registered in their respective registries. Generic pipeline code does not change.
- **Notification service (future):** Telegram bot — post-v1; consumer subscribes to Redpanda, sends alerts.
