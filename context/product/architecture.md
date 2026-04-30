# System Architecture Overview: Grosh

---

## 1. Application & Technology Stack

- **Main API:** Python + FastAPI — async, Pydantic models, OpenAPI docs out of the box. Serves read endpoints for the frontend (transaction queries, monthly aggregates, account listing). Owns the auth system (JWT issuance, refresh token rotation, user management). No Redpanda dependency.
- **Ingestion Service:** Python + FastAPI — dedicated service for all pipeline-feeding writes. Receives bank webhooks, handles account linking, manual transaction entry, and backfill triggers. Owns the Redpanda producer and bank-specific adapters (normalization: always-positive amounts, explicit transaction type). Validates JWTs for authenticated endpoints but does not issue tokens.
- **Streaming Consumers:** Python worker services — subscribe to Redpanda topics, bank-agnostic. Run the enrichment pipeline (deduplication → transfer detection → two-tier currency conversion → rule lookup → MCC fallback → ML classifier), write enriched transactions to TimescaleDB.
- **Frontend:** Next.js (App Router) — SSR, React ecosystem. Single unified UI for transaction feed, manual entry, feedback loop, forecast view, net worth dashboard, and family aggregate view.
- **UI Components:** shadcn/ui — accessible component library for fast, consistent UI.
- **Charts:** Recharts or Tremor — financial time-series charts, net worth, category breakdowns.
- **Data Fetching:** TanStack Query — async data fetching, caching, and background revalidation on the frontend.
- **Source Registry:** Provider registries in `registry.py` — `RATE_PROVIDERS` (dict of `RateProviderConfig` with optional `fetch_historical`) and `TRANSACTION_BACKFILL_PROVIDERS` (dict of `TransactionBackfillProvider`). Both `main.py` and standalone backfill K8s Jobs import from the same registry. Adding a new bank = adding entries here; no generic code changes.
- **Message Broker:** Redpanda — Kafka-compatible broker (single binary, no ZooKeeper/JVM). Topics: `raw_transactions` (partitioned by user_id). Consumers: enricher, family aggregator, forecast invalidator, notification service (future).
- **GraphQL (future):** Strawberry — Python GraphQL library, added alongside REST when the dashboard constructor feature is built. REST remains the primary API for webhooks, auth, and ingestion; GraphQL serves the flexible, read-heavy dashboard query layer.

---

## 2. Data & Persistence

- **Primary Database:** TimescaleDB (PostgreSQL + TimescaleDB extension) — `transactions` is a hypertable partitioned by time. Continuous aggregates for monthly/weekly summaries. Native SQL for all other queries.
- **Vector Store:** pgvector extension on the same TimescaleDB instance — stores sentence-transformer embeddings for k-NN transaction classification.
- **Row-Level Security:** Enabled on all user-scoped tables. FastAPI sets `app.current_user_id` session variable from JWT claim at request start; the database enforces isolation at query level regardless of application logic.
- **Currency Rates:** SCD Type 2 `currency_rates` table — per-source (Monobank, NBU), per-pair rates with `valid_from`/`valid_to` windows. Two independent sequences: polled rows (with `last_polled_at`, `update_cadence_seconds`) and historical rows (explicit time windows). `rate_source_config` table defines fallback chains (monobank → nbu) and base pivot currencies per source. Consumer resolves rates via two quality tiers: FRESH (actively monitored at transaction time) then CLOSEST (nearest within 7-day window). Historical rates backfillable from NBU via K8s Job.
- **Key Tables:** `users`, `refresh_tokens`, `networks`, `network_members`, `user_settings`, `bank_integrations`, `accounts`, `counterparties`, `transactions` (hypertable), `monthly_aggregates` (continuous aggregate), `currency_rates` (SCD2), `rate_source_config`, `transaction_embeddings` (pgvector), `merchant_rules`, `categories`, `scheduled_events`, `sharing_permissions`, `ml_labels`.
- **Caching:** No dedicated cache layer. TimescaleDB continuous aggregates serve pre-computed summaries; TanStack Query manages client-side freshness. Sufficient for the ~3-user scale.

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
- **All services in k3s:** Lightweight single-node Kubernetes. Stateless services (main API, ingestion, consumer, ML, Next.js) as Deployments. Stateful services (TimescaleDB, Redpanda) as StatefulSets with PVCs. Traefik ingress bundled with k3s. Docker Compose used for local dev only.
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
- **Monobank token storage:** Encrypted in DB with pgcrypto `pgp_sym_encrypt`. App encryption key stored in Infisical, never in code or environment variables.
- **Data isolation:** PostgreSQL Row-Level Security on all user-scoped tables. Enforced at the database layer, independent of application logic. Two database roles: `grosh_app` (RLS enforced, used by API and ingestion services) and `grosh_admin` (table owner, RLS bypassed, used by migrations and consumer).
- **Family aggregates:** `sharing_permissions` table controls visibility. Admin sees category-level aggregates by default; raw transactions require explicit per-user grant.

---

## 6. Observability & Monitoring

- **Logs:** Loki — structured JSON logs from all services, aggregated and queryable.
- **Metrics:** Prometheus — scrapes FastAPI (`/metrics`), k3s node exporter, TimescaleDB exporter, Redpanda metrics endpoint.
- **Dashboards:** Grafana — time-series dashboards for API latency, consumer lag, DB query times, forecast job duration.
- **Alerting:** Grafana alerting rules for consumer lag spikes, high error rates, and disk usage thresholds.

---

## 7. External Integrations

- **Monobank API:** Personal token auth (`X-Token` header). Two ingestion paths, both publishing to the same `raw_transactions` Redpanda topic:
  - **Webhook** — real-time push for new transactions. Monobank calls `POST /webhook/monobank` on the ingestion service; the Monobank adapter normalizes the payload (abs amounts, infers transaction type from sign) and publishes to Redpanda.
  - **Historical backfill** — `POST /monobank/accounts/{id}/backfill` on the ingestion service triggers a K8s Job. The Job receives all parameters as env vars, resolves the bank source from the integration record, and dispatches to the matching `TransactionBackfillProvider` from the registry. Monobank's provider paginates the statement API (31 days/request, 61s rate limit) and publishes each batch to Redpanda. Consumer deduplicates by deterministic transaction ID — safe to re-run.
  - **Historical rate backfill** — `POST /admin/rates-backfill` triggers a K8s Job that dispatches to the matching rate provider's `fetch_historical()` via the registry. Currently NBU supports historical rate backfill (~45 currencies per date range).
  - The consumer is unaware of which path or bank a transaction came from — deduplication and enrichment are identical for all sources.
- **Future bank integrations:** Architecture designed for extension — new bank sources are added under `sources/{name}/` in the ingestion service, implementing `TransactionBackfillProvider` and/or `RateProviderConfig.fetch_historical`, then registered in `registry.py`. The consumer, main API, and all generic backfill infrastructure do not change.
- **Notification service (future):** Telegram bot — post-v1; consumer subscribes to Redpanda, sends alerts.
