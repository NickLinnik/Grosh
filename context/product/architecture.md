# System Architecture Overview: Grosh

---

## 1. Application & Technology Stack

- **Main API (`services/api`):** Python + FastAPI — async, Pydantic models, OpenAPI docs out of the box. Serves read endpoints for the frontend (transaction queries, aggregates, account listing, settings, admin user management) and owns the auth system (JWT issuance, refresh token rotation, user CRUD). No Redpanda dependency.
- **Ingestion Service (`services/ingestion`):** Python + FastAPI — thin gateway for all pipeline-feeding writes. Receives Monobank webhooks, handles account linking, manual transaction entry, backfill triggers, rate-backfill triggers, and reprocess triggers. Validates webhook authenticity, resolves user/account from DB, and publishes **raw bank payloads** (not normalized) to per-source Redpanda topics. Validates JWTs using the shared `JWT_SECRET` but does not issue them. Also owns the K8s Job submission API (see §4 K8s Job control plane).
- **Normalization Service (`services/normalization`):** Python worker, **independently deployable**. Owns every producer of the `normalized_transactions` topic. Three internal producers run in one process:
  1. **Steady-state normalization** — subscribes to per-source raw topics (`raw_transactions.monobank`, `raw_transactions.manual`, etc.), dispatches to per-source `NormalizationStrategy`, publishes `NormalizedTransaction`.
  2. **Staging drain** — periodically sweeps `staging_normalized_transactions` rows whose user no longer holds a reprocess lock and republishes them to `normalized_transactions`.
  3. **Reprocess job entrypoint** — K8s Job pod (same image, `python -m grosh_normalization.reprocess_main`) reads stored `transactions`, reconstructs `NormalizedTransaction` via the inverse mapping, deletes originals, republishes.
- **Enrichment Service (`services/enrichment`):** Python worker, **independently deployable**. Pure consumer of `normalized_transactions`. Runs layered processing: transfer detection (per-source `TransferDetectionStrategy` registry) → currency conversion → classification → persistence to PostgreSQL. Knows nothing about reprocess, staging, or normalization — it just sees a stream of events from `normalized_transactions`. (Full rationale and single-writer carve-outs in `adr-consumer-pipeline-architecture.md` referenced from spec 003.)
- **ML Service (`services/ml`):** Python — sentence-transformer embeddings, pgvector k-NN classifier, active-learning feedback loop, Prophet/NeuralProphet cash-flow forecasting. Scaffolded today (Phase 1) for module structure; functional layers ship in Phase 2 (classification) and Phase 3 (forecasting).
- **Frontend (`services/frontend`):** Next.js (App Router) — **client-side SPA**. The root dashboard route is `'use client'`; no SSR, no hydration handshake. App Router is used for routing, layouts, code-splitting, route prefetching on hover, and Edge-cached HTML shell — only data-driven SSR is removed. Rationale: ≤10 authenticated household users on warm caches make SSR's first-paint benefit invisible while its tax (hydration mismatches around timezone/locale/auth, two execution environments) is real every day. See spec 004 §2.1 for the full decision record.
- **UI Components:** shadcn/ui — accessible component library for fast, consistent UI.
- **Charts:** Recharts — pinned. Diverging-bar monthly chart (spec 004), future net-worth time series, future forecast bands with uncertainty intervals all use Recharts. Tremor was an evaluated alternative; not used.
- **Data Fetching:** TanStack Query — async data fetching, caching, background revalidation, and infinite-scroll pagination on the frontend.
- **Source Registry:** Provider registries in `registry.py` — `RATE_PROVIDERS` (dict of `RateProviderConfig` with optional `fetch_historical`) and `TRANSACTION_BACKFILL_PROVIDERS` (dict of `TransactionBackfillProvider`). Both `main.py` and standalone backfill K8s Jobs import from the same registry. Adding a new bank = adding entries here + source-specific strategies (`NormalizationStrategy`, `TransferDetectionStrategy`, future `ClassificationStrategy`) in the respective service; no generic code changes.
- **Message Broker:** Redpanda — Kafka-compatible broker (single binary, no ZooKeeper/JVM). Current topics:
  - `raw_transactions.monobank` — Monobank webhook + backfill payloads (key: `user_id`)
  - `raw_transactions.manual` — manual cash-account entries (key: `user_id`)
  - `normalized_transactions` — source-agnostic `NormalizedTransaction` envelope consumed by the enrichment service
  - Future: `user_notifications` (Phase 2 SSE push)
  - The `staging_normalized_transactions` Postgres table (not a Kafka topic) buffers normalized events for users currently mid-reprocess; the normalization service's drain loop republishes them to `normalized_transactions` after the lock releases.
- **GraphQL (speculative, not on any current roadmap phase):** Strawberry — Python GraphQL library. Originally considered for a future "dashboard constructor" feature that lets users assemble custom dashboards from a typed query layer. The feature is not in Phases 1–4 of the current roadmap; the line is kept here as a forward-looking option, not a commitment.

---

## 2. Data & Persistence

- **Primary Database:** PostgreSQL 18 (from `pgvector/pgvector:pg18`) with `pgvector`, `pg_cron`, and `pg_stat_statements` extensions. Plain tables with B-tree indexes. No TimescaleDB (see `adr-drop-timescaledb.md` for rationale — composite PK tax, watermark footgun, 12× image bloat with no performance benefit at our scale).
- **Vector Store:** pgvector extension — stores sentence-transformer embeddings for k-NN transaction classification.
- **Scheduled cleanup:** `pg_cron` — in-database scheduled jobs. Currently runs (a) revoked-tokens TTL purge every 5 minutes, and (b) the `reprocessing_backups_cleanup` job daily at 03:00 UTC that drops backups older than 30 days. Migrations registering pg_cron jobs include fail-fast timezone guards so a misconfigured Postgres deploy fails loudly rather than scheduling in the wrong window. Replaces TimescaleDB retention policies.
- **Query Observability:** `pg_stat_statements` — per-query execution stats (calls, total/mean/max time, rows, buffer hits/misses). Loaded at startup via `shared_preload_libraries`. Used for ad-hoc profiling today; planned Grafana wiring for ongoing observability.
- **Aggregations:** Computed on read. Single SQL query with `date_trunc` + `FILTER` clauses. Sub-ms at per-user scale with `INDEX (user_id, time DESC)`. Supports any bucket (day/week/month/quarter/year), arbitrary date ranges, per-currency selection (UAH/USD/EUR), and `converted_pct` for data-quality visibility. Timezone-aware bucketing via `AT TIME ZONE user_settings.timezone`.
- **Row-Level Security:** Enabled on all user-scoped tables (`users`, `accounts`, `bank_integrations`, `categories`, `transactions`, `user_settings`). User-facing services (API, ingestion) set `app.current_user_id` session variable from JWT claim at request start; the database enforces isolation at query level. Every RLS policy carries both `USING` (read filter) and `WITH CHECK` (write rejection) clauses — RLS is a write barrier, not just a read filter. The enrichment service runs above RLS (full table access for background processing).
- **Database roles:** Four roles with column-level grant separation, not two. (Architecture supersedes any earlier doc claiming `grosh_app`/`grosh_admin` only.)
  - `grosh_admin` — table owner; RLS-bypassing; used by migrations only.
  - `grosh_api` — used by the API service; RLS-enforced; owns writes on `users` (most columns), `refresh_tokens`, `revoked_tokens`, `user_settings`, `categories`, `merchant_rules`, `ml_labels`.
  - `grosh_ingestion` — used by the ingestion service; RLS-enforced; owns writes on `accounts`, `bank_integrations`, `currency_rates`, and a narrow column-level grant on `users.last_reprocess_started_at` (the per-user reprocess rate-limit) and `reprocessing_locks` (INSERT only).
  - `grosh_consumer` — used by normalization and enrichment services; `BYPASSRLS`; owns writes on `transactions`, `transfer_match_anomalies`, `reprocessing_backups`, `reprocessing_locks` (DELETE only). The two services share this role; their narrow carve-outs on `transactions` (enrichment INSERT/UPDATE; normalization DELETE during reprocess) are enforced by code review + a CI carve-out test, not by DB grants.
- **Currency Rates:** SCD Type 2 `currency_rates` table — per-source (Monobank, NBU), per-pair rates with `valid_from`/`valid_to` windows. Two independent sequences: polled rows (with `last_polled_at`, `update_cadence_seconds`) and historical rows (explicit time windows). `rate_source_config` table defines fallback chains (monobank → nbu) and base pivot currencies per source. Enrichment service resolves rates via two quality tiers: FRESH (actively monitored within `K × update_cadence_seconds`, K=2) then CLOSEST (nearest within 7-day window). Both legs of a chained conversion must resolve at the same tier — never mix. Historical rates backfillable from NBU via K8s Job.
- **Key Tables:** `users`, `refresh_tokens`, `revoked_tokens`, `networks`, `network_members`, `user_settings`, `bank_integrations`, `accounts`, `counterparties`, `transactions` (regular table, PK on `id`, UUIDv7), `transfer_match_anomalies`, `staging_normalized_transactions`, `currency_rates` (SCD2), `rate_source_config`, `reprocessing_locks`, `reprocessing_backups`, `transaction_embeddings` (pgvector), `merchant_rules`, `categories`, `scheduled_events`, `sharing_permissions`, `ml_labels`.
- **Caching:** No dedicated cache layer (no Redis). Compute-on-read aggregations are sub-ms; TanStack Query manages client-side freshness. Sufficient at the ~3-user scale.

---

## 3. ML & Forecasting

- **Transaction Classifier — three-tier pipeline:**
  1. **Rule lookup:** `merchant_rules` table maps normalized merchant / counterparty IBAN → category. ~80% coverage once populated. 100% accurate.
  2. **MCC fallback:** ISO 18245 MCC code → coarse category. Free signal, no model needed.
  3. **Embedding classifier:** multilingual MiniLM (sentence-transformers) embeddings stored in pgvector; k-NN similarity search against labeled transactions. Handles unknown merchants.
- **Active learning loop:** Unclassified / low-confidence transactions surfaced in the feed UI. User confirms or corrects. High-confidence recurring merchants auto-promoted to rules. Labeled samples accumulate for periodic retraining.
- **Classifier evolution:** Start with sklearn k-NN (Level 1). Promote to fine-tuned transformer when labeled dataset reaches ~500 examples (Level 3).
- **Cash Flow Forecasting:**
  - **Deterministic component:** `scheduled_events` table (salary, rent, subscriptions) projected forward as exact future cash flows.
  - **Probabilistic component:** Prophet (NeuralProphet once history > 12 months) on monthly category aggregates. Outputs expected spend per category with confidence intervals.
  - **Combined view:** Scheduled events + probabilistic variable spend. Configurable horizon: 90-day default, up to 1 year as history accumulates.

---

## 4. Infrastructure & Deployment

- **Cloud Provider:** Hetzner (CX31 — 2 vCPU, 8 GB RAM, 80 GB SSD, ~€9/month). Single node.
- **Orchestration in production:** k3s — lightweight single-node Kubernetes runs everything. Stateless services (main API, ingestion, normalization, enrichment, ML, Next.js) as Deployments. Stateful services (PostgreSQL, Redpanda) as StatefulSets with PVCs. K8s Jobs for backfill/reprocess (see K8s Job control plane below).
- **Orchestration in local dev:** Docker Compose for application services (faster iteration loop), with Docker Desktop K8s for the K8s Job portion (backfill, reprocess) so K8s-shaped bugs are caught in dev. This dev/prod split is deliberate, not a deferred migration — `make dev-k8s-setup` builds and imports all service images into Docker Desktop K8s and regenerates secrets idempotently.
- **Reverse proxy / TLS:** Traefik — bundled with k3s, automatic Let's Encrypt certificates. (Caddy was evaluated; Traefik wins because it's already in the cluster.)
- **Provisioning:** Terraform — Hetzner VPS, DNS records, firewall rules, SSH key injection. Full "deployable from scratch" in one command.
- **CI/CD:** GitHub Actions — build → push to ghcr.io → SSH deploy → rolling restart.
- **Secrets management:** Infisical (self-hosted). No `.env` files in repos. Secrets injected at runtime.
- **Local dev webhook tunnel:** Cloudflare Tunnel (`cloudflared`) — exposes the ingestion webhook endpoint to Monobank without port forwarding. Dev Compose profile only; removed in production.

### K8s Job Control Plane

The ingestion service owns programmatic K8s Job submission and status polling. K8s Jobs are constructed at submit time by service code (`BackfillService`, `ReprocessDispatcher`) — no static YAML manifests. Every Job carries ownership labels (`grosh.app/managed-by`, `grosh.app/job-kind`, plus `grosh.app/user-id` or `grosh.app/account-id` as applicable) so the status endpoint can verify the URL's scope against the Job before responding (IDOR defense — both "doesn't exist" and "exists but wrong scope" return 404).

| Job kind                | Triggered by                                                  | Image                                | Scope        | Cleanup |
|-------------------------|---------------------------------------------------------------|--------------------------------------|--------------|---------|
| `monobank_backfill`     | `POST /v1/monobank/accounts/{id}/backfill` (user)             | `grosh-ingestion`                    | per-account  | ≥3600s `ttlSecondsAfterFinished` |
| `rates_backfill`        | `POST /v1/admin/rates-backfill` (admin)                       | `grosh-ingestion`                    | global       | ≥3600s |
| `reprocess` (per-user)  | `POST /v1/users/{id}/reprocess` (self or admin)               | `grosh-normalization` (reprocess entrypoint) | per-user | ≥3600s |
| `reprocess` (admin bulk)| `POST /v1/admin/reprocess` (admin)                            | `grosh-normalization` (reprocess entrypoint) | multi-user (snapshot) | ≥3600s |

Race-free triggering for reprocess: the API atomically INSERTs the `reprocessing_locks` row inside the trigger transaction *before* submitting the Job, closing the race where two concurrent triggers could submit duplicate jobs. The Job pod verifies the lock row at startup; if absent (network-glitch case), it exits cleanly without touching `transactions`. Full mechanics in spec 003 §2.9.

---

## 5. Security

- **Network:** Only ports 80/443 exposed publicly. All inter-service traffic on internal Docker/k3s network. Hetzner firewall + ufw block everything else.
- **TLS:** Automatic via Traefik + Let's Encrypt.
- **Auth:** JWT with refresh token rotation. Access tokens: 15-min TTL. Refresh tokens: 30 days, `httpOnly` cookies. No public registration — admin creates users manually via `POST /v1/admin/users`. The main API owns token issuance and refresh; the ingestion service validates tokens using the shared `JWT_SECRET` but does not issue them.
- **Token revocation:** `revoked_tokens` table (plain table, PK on `jti`). `pg_cron` purges expired entries every 5 minutes. Every authenticated request checks the revocation list. User deactivation (`DELETE /v1/admin/users/{id}`) sets `users.is_active = false`, which becomes an instant kill switch — `get_current_user` rejects any JWT belonging to a deactivated user with 401 `AUTHENTICATION_REQUIRED`.
- **Monobank token storage:** Encrypted in DB with pgcrypto `pgp_sym_encrypt`. App encryption key stored in Infisical, never in code or environment variables. Token is never returned to the client; webhook re-registration that doesn't require re-prompting the user is a future backend endpoint (see spec 004 §3 Out-of-Scope).
- **Data isolation:** PostgreSQL Row-Level Security on all user-scoped tables, enforced at the database layer independent of application logic. Repository methods on user-scoped tables include an explicit `WHERE user_id = $1` predicate alongside the RLS policy for query-plan stability (indexes are `(user_id, ...)`-prefixed) and survivability under ad-hoc RLS disablement during debugging.
- **Family aggregates:** `sharing_permissions` table controls visibility (Phase 4). Admin sees category-level aggregates by default; raw transactions require explicit per-user grant.

---

## 6. Observability & Monitoring

- **Logs:** Loki — structured JSON logs from all services, aggregated and queryable.
- **Metrics:** Prometheus — scrapes FastAPI (`/metrics`), k3s node exporter, PostgreSQL exporter, Redpanda metrics endpoint.
- **Dashboards:** Grafana — time-series dashboards for API latency, consumer lag, DB query times, forecast job duration.
- **Alerting:** Grafana alerting rules for consumer lag spikes, high error rates, and disk usage thresholds.
- **Application-level signals to wire up:** the normalization service's staging-drain loop emits a WARN log when the oldest staged row exceeds 5 minutes (per spec 003 §2.9). `pg_stat_statements` is loaded and queried ad-hoc today; Grafana wiring planned.

---

## 7. External Integrations

- **Monobank API:** Personal token auth (`X-Token` header). Two ingestion paths, both publishing raw bank payloads to `raw_transactions.monobank`:
  - **Webhook** — real-time push for new transactions. Monobank calls `POST /monobank/webhook/{webhook_secret}` on the ingestion service (the webhook URL is unversioned by design — re-registering with Monobank for every minor version bump is not viable; all other endpoints sit under `/v1/`). The service validates the secret against the DB, resolves user/account, publishes the raw payload + routing envelope to Redpanda. Normalization happens in the normalization service.
  - **Historical backfill** — `POST /v1/monobank/accounts/{id}/backfill` on the ingestion service triggers a K8s Job. The Job paginates the statement API (31 days/request, 1 req per 60s rate limit) and publishes each batch to Redpanda. Enrichment service deduplicates by deterministic transaction ID via `ON CONFLICT (id) DO NOTHING` — safe to re-run.
- **NBU (Ukrainian National Bank):** Daily reference rates ingested as a fallback rate source. Historical rates backfillable per currency pair via `POST /v1/admin/rates-backfill` (K8s Job dispatches to the matching rate provider's `fetch_historical()` via the registry; ~45 currencies per date range = ~3 MB).
- **Future bank integrations (PUMB, Revolut):** Architecture designed for extension via Strategy pattern — new bank sources add files under `sources/{name}/` in ingestion + `NormalizationStrategy` and `TransferDetectionStrategy` implementations + registry entries. Generic pipeline code does not change. Each new bank adds a `raw_transactions.{name}` topic.
- **Real-time UI updates (Phase 2):** Server-Sent Events (`GET /events/stream` on the main API, JWT-authenticated). A future notification consumer subscribes to a `user_notifications` Redpanda topic and pushes events to connected clients; TanStack Query invalidates chart and feed caches on receipt. Mechanism is pinned to SSE per roadmap Phase 2 commitment.
- **Notification service (Phase 5):** Telegram bot — post-v1; consumer subscribes to Redpanda, sends alerts.
