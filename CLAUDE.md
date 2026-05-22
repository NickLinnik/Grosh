# Family Finance Platform — Project Context

## Purpose

A self-hosted personal finance platform for a small family network (~3 people).
Replaces a manual Google Sheets workflow that was easy to forget and painful to backfill.

Core goals:
- Automatically ingest transactions from Monobank (Ukrainian neobank) via webhook. Planned future banks: PUMB, Revolut. Architecture must support adding new bank adapters without reworking the core pipeline.
- Let users manually record cash, debts, and planned future events
- Classify transactions automatically, with a human-in-the-loop feedback loop that improves over time
- Forecast cash flow forward using a mix of deterministic rules and ML — configurable horizon, 90-day default, up to 1 year as history grows
- Track net worth across all assets (bank accounts, cash, debts, future income)
- Support a "family network" model with multiple profiles

Secondary goals (this is also a pet project for learning):
- Get hands-on with Kafka/Redpanda streaming
- Build and own an ML classification + active learning pipeline
- Touch modern frontend (Next.js) without getting bogged down
- Practice devops: k3s, Terraform, CI/CD, secrets management

---

## Non-Goals (v1)

- Investment portfolio tracking (planned for later, design with extension in mind)
- Public signup / multi-tenant SaaS (family-only, users are created manually by admin)
- Mobile app (responsive web is enough)
- Replacing Monobank's own UI for day-to-day banking

---

## Users & Data Model Philosophy

There will be ~3 users. Each user has one or more Monobank accounts (cards). Users may
also have accounts at other banks or hold cash — these are entered manually.

A single admin user (the owner) can view household-level aggregates (total family spending
by category, net worth across all members) without seeing individual raw transactions of
others unless explicitly shared. This is enforced at the database layer via PostgreSQL
Row-Level Security (RLS), not just application logic.

---

## Tech Stack

### Ingestion / Streaming
- **Redpanda** — Kafka-compatible message broker. Chosen over Apache Kafka for simpler
  operation (single binary, no ZooKeeper/JVM) while preserving full Kafka API compatibility.
  Used for: raw transaction events from Monobank webhooks, fan-out to multiple consumers.
- Kafka Connect and Kafka Streams are NOT used — external Python consumers instead.
  This is intentional: keep streaming experience transferable to real Kafka environments
  while keeping the stack Python-native.

### Backend
- **FastAPI** (Python) — async, pydantic models, OpenAPI docs out of the box.
  Handles: webhook receiver, REST API for frontend, manual entry endpoints.
- **Python consumers** (separate services) — two-stage pipeline: normalization service
  (per-source → normalized) and enrichment service (transfer detection → currency conversion
  → classification → persistence). Write enriched transactions to PostgreSQL.

### Storage
- **PostgreSQL 18** with `pgvector`, `pg_cron`, and `pg_stat_statements` extensions. Plain
  tables with B-tree indexes (no TimescaleDB — see `adr-drop-timescaledb.md`). Aggregations
  computed on read. Native `uuidv7()` used for time-ordered UUID primary keys.
- **pgvector** extension — stores transaction description embeddings for the k-NN classifier.
- **pg_cron** — in-database scheduled jobs (TTL cleanup for revoked tokens).
- **pg_stat_statements** — per-query execution stats for observability. Loaded at Postgres
  startup via `shared_preload_libraries`; surfaces top queries by total/mean/max execution
  time, planned for Grafana wiring.
- Row-Level Security enabled on all user-scoped tables.

### ML / Forecasting
- **Transaction classifier** — three-tier priority system:
  1. Rule lookup: `merchant_rules` table maps (counterparty IBAN / masked card / normalized
     merchant name) → category. Fast, 100% accurate, covers ~80% of transactions once populated.
  2. MCC fallback: ISO 18245 MCC code → coarse category mapping. Free signal, no model needed.
  3. Embedding classifier: sentence-transformers embeddings (multilingual MiniLM) stored in
     pgvector, k-NN similarity search against labeled transactions. Handles unknown merchants.
  - Feedback loop: unclassified transactions surface in UI with prediction + confidence.
    User confirms or corrects. High-confidence recurring merchants are auto-promoted to rules.
    Labeled samples accumulate for periodic model improvement.
  - Start at sklearn / k-NN (Level 1). Promote to fine-tuned transformer when labeled
    dataset reaches ~500 examples (Level 3).

- **Cash flow forecasting** — two components combined:
  1. Deterministic: `scheduled_events` table (salary dates, rent, subscriptions, known
     future expenses). Projected forward as exact future cash flows.
  2. Stochastic: Prophet (or NeuralProphet once history > 12 months) on monthly category
     aggregates. Outputs expected spend per category with confidence intervals.
  - Combined forecast = scheduled events + probabilistic variable spend. Displayed with
    uncertainty bands. Forecast horizon is configurable: 90-day default, up to 1 year as
    transaction history accumulates (seasonality requires a full annual cycle to learn).

### Frontend
- **Next.js** (App Router) — SSR, React ecosystem, professionally transferable.
- **shadcn/ui** — component library for fast, good-looking UI.
- **Recharts or Tremor** — financial charts (time-series, net worth, category breakdowns).
- **TanStack Query** — async data fetching and caching.
- Single unified UI for: transaction feed, manual entry, category labeling (feedback loop),
  forecast view, net worth dashboard, family aggregate view (admin only).

### Infrastructure
- **Hetzner CX31** — 2 vCPU, 8GB RAM, 80GB SSD, €8.90/month. Single node.
  Chosen over AWS for cost (≈5x cheaper for equivalent compute on a long-running personal project).
- **k3s** — lightweight single-node Kubernetes. **In production, runs everything**: stateless
  services (FastAPI, ingestion, consumer, ML, Next.js) as Deployments, and stateful services
  (PostgreSQL, Redpanda) as StatefulSets with PVCs. Traefik ingress is bundled with k3s. See
  roadmap Phase 2 "Go Live" for the full deployment checklist.
- **Local dev uses Docker Compose** instead of k3s for service orchestration — the iteration
  loop (`docker compose build && docker compose up -d`) is significantly faster than the k3s
  equivalent. K8s Jobs (backfill, reprocess) are exercised on Docker Desktop K8s in dev because
  Job lifecycle is where K8s-shaped bugs surface, and dev needs to catch them. This dev/prod
  orchestrator split is deliberate, not a deferred migration.
- **Caddy or Traefik** — reverse proxy, automatic Let's Encrypt TLS.
- **Terraform** — provisions the Hetzner VPS, DNS records, firewall rules, SSH key injection.
  Full "deployable from scratch" in one command.
- **GitHub Actions** — CI/CD: build → push to ghcr.io → SSH deploy → rolling restart.
- **Infisical** — self-hosted secrets manager. No .env files in repos. Secrets injected at runtime.
- **Cloudflare Tunnel** (`cloudflared`) — used in local dev to expose webhook endpoint to
  Monobank without port forwarding. Runs as a Compose service in the dev profile only.

---

## Code Organization Principles

**Source-specific vs generic separation (ingestion service):**
The ingestion service organizes code under `sources/{source_name}/` (monobank, nbu, manual). Each source owns its full vertical: router, service, client, adapter, repo. Generic code (shared DB operations, cross-cutting services) lives in `repositories/` and `services/`. If a piece of code mentions a specific bank or source by name, it belongs in that source's folder — not in a generic layer.

**Abstraction boundaries — generic code must not know which sources exist:**
The filing rule above is necessary but not sufficient. Generic services, routers, and repositories must be written so they work with *any* source through interfaces, not by naming specific sources. Concretely:
- A generic service must never contain `if source == "nbu"` or `if source == "monobank"` branches. If behavior varies by source, define a protocol/ABC and let each source implement it.
- A generic backfill service must not hardcode source-specific parameters (API rate limits, chunk sizes, auth methods). These belong in the source's own adapter or config.
- Admin/cross-cutting routers belong in a top-level router directory or in the service root — never inside `sources/`, because they are not source-specific.
- Routers must never contain SQL. Role checks, permission checks, and data lookups go through a repo or service layer.

**The test:** before writing a generic module, ask "if I added a new bank tomorrow, would I need to modify this file?" If yes, the abstraction is wrong — source-specific logic has leaked into the generic layer.

**Layer separation:**
Routers, services, and repos are always in separate files. A router never contains business logic or SQL. A service never imports FastAPI. A repo never contains business logic. No exceptions.

**SQL belongs only in repository modules.** No module outside `repositories/` may contain raw SQL (`SELECT`, `INSERT`, `UPDATE`, `DELETE`, `WITH ... DELETE`, or `conn.fetch*`/`conn.execute` calls with literal queries). This applies to routers, services, consumers, drain tasks, FastAPI dependency callables (`deps.py`), Kafka message handlers, K8s Job entrypoints — every layer above the repo. If a feature needs a new query, add a method to the relevant repo and call it from the higher layer. If two queries must run atomically, the orchestration layer opens the `async with conn.transaction():` block and the repo methods run inside it — but the SQL strings live in the repo. (Migrations under `services/*/migrations/versions/` are SQL by design and exempt — they sit outside the repo layer entirely.)

**The test:** `grep -E "SELECT|INSERT|UPDATE|DELETE|conn\.(fetch|execute)" <file>` should return zero hits for any file outside a `repositories/` directory.

**Schema generality:**
Database tables shared across sources (like `bank_integrations`) use generic columns only. Source-specific fields go in `config JSONB`, not as top-level columns. This prevents schema changes when adding new bank integrations.

**Migration hygiene:**
Never create a new migration for changes to tables/columns from a migration that hasn't been merged to `main`. Modify the existing migration instead — it's still a draft on an unmerged branch.

**SQL style:**
Use triple-quoted strings for queries. One item per line in all clauses — SELECT columns, WHERE predicates, ORDER BY keys, GROUP BY keys. Keywords (`SELECT`, `FROM`, `WHERE`, `ORDER BY`, `GROUP BY`) on their own lines. No module-level column-list constants unless the same list is used in 3+ queries.

**Time ranges are half-open `[from, to)`:**
All time-range filters in API query params, repos, and SQL use `time >= from AND time < to` — inclusive on the lower bound, exclusive on the upper. This matches `date_trunc()` bucket boundaries, SCD2 validity windows (`valid_from <= t AND (valid_to IS NULL OR valid_to > t)`), Postgres `tstzrange '[a, b)'`, and the conventions used by Stripe, GitHub, Prometheus, ISO 8601, and every modern range-typed API. Never use `<=` on the upper bound — it creates off-by-one errors at bucket edges and forces frontends to send awkward `23:59:59` end-of-day fudges. Document `to` as exclusive in OpenAPI descriptions. The same rule applies to date columns (`valid_from`-style SCD2), pagination cursors over time, and any backfill window definitions.

**List-typed query params: repeated-key + StrEnum, never comma-separated:**
Multi-value query params use repeated keys (`?currency=UAH&currency=USD`) with `list[SomeStrEnum]` type annotations. Never comma-separated (`?currency=UAH,USD`). Why: FastAPI parses repeated keys natively, the framework's own validator handles enum membership, and the OpenAPI schema gets a proper `type: array, items: {type: string, enum: [...]}` declaration that generates correct typed SDKs (TypeScript `Currency[]`, not `string`). Comma-separated forces a hand-rolled `_parse_csv_param`-style helper, weakens the OpenAPI schema to plain `string`, and locks the project into "no value can ever contain a comma." Enum values are **canonical case only** — the framework rejects `"uah"` when the enum is `UAH`. Don't add custom case-insensitive normalization; clear 422s teach the canonical form once. This applies to anything that's not free text: currencies, status codes, modes, fields. Free-text params (descriptions, queries) stay as `str`.

**Keep docs in sync with code changes:**
When making code changes outside a planned AWOS task — refactors, reviewer fixes, ad-hoc improvements — update all affected documentation before considering the work done. The spec is the primary source of truth:
- `context/spec/{spec}/technical-considerations.md` — file structure tables, API contracts, topic lists, shared model tables
- `context/spec/{spec}/tasks.md` — task descriptions must reflect actual implementation, not the original plan
- `infra/grosh.postman_collection.json` — add/update/remove requests when endpoints change

---

## Testing Conventions

**Integration tests use real Postgres with their own lifecycle.** Each Python service has `tests/integration/conftest.py` that creates a throwaway test database (via `grosh_shared.db.testing`), runs migrations, and drops it on teardown. Individual tests get an `asyncpg` connection wrapped in a rolled-back transaction for full isolation. Never skip integration tests because "there's no DB infra" — the infra exists and is mandatory.

**Unit tests use in-memory repos.** For service-layer logic that depends on DB repos, create `InMemory*Repo` fakes in `tests/unit/conftest.py` that match production query semantics without touching `conn`. These are NOT mocks — they implement real filtering/matching logic so tests validate business behavior, not just call sequences.

---

## Architectural Invariants

Rules that apply across the entire system. Violations are bugs, not style issues.

### Data ownership matrix

Each table has exactly one write-owner service. All services may read any table.

| Table(s)                              | Write owner       | Notes                                    |
|---------------------------------------|-------------------|------------------------------------------|
| `users`                               | API service (most columns) + Ingestion (UPDATE on `last_reprocess_started_at` only) | Co-owned by design. Ingestion has a narrow column-level grant (`GRANT UPDATE (last_reprocess_started_at) ON users TO grosh_ingestion`, migration 0016) for the per-user reprocess rate-limit (spec 003 §2.11.5). The API service retains write ownership of all other mutable columns (`email`, `password_hash`, `is_active`, `last_active_at`, `display_name`, `role`). |
| `refresh_tokens`, `revoked_tokens`, `user_settings` | API service       |                                          |
| `accounts`, `bank_integrations`, `currency_rates` | Ingestion service | Accounts are created during linking; rates by polling/backfill |
| `transactions`, `transfer_match_anomalies` | Enrichment (INSERT/UPDATE) + Normalization (DELETE on reprocess) | Enrichment INSERTs/UPDATEs new rows on `transactions` (transfer-pair claim sets `related_transaction_id` and `special_category='transfer'`) and INSERTs/auto-resolves on `transfer_match_anomalies`. Normalization DELETEs `transactions` during reprocess (snapshot → DELETE → republish flow); `transfer_match_anomalies` rows are cascaded by the `transactions` FK and not written directly by the normalization service. Both `transactions` carve-outs (INSERT/UPDATE on enrichment; DELETE on normalization) are bounded to single SQL commands per service so the invariant remains auditable via `grep INSERT INTO transactions services/enrichment/` and `grep DELETE FROM transactions services/normalization/`. Enforced by the CI carve-out test in `tests/e2e/integration/test_single_writer_carveout.py` + code review, not by DB grants — both services share the `grosh_consumer` role. |
| `categories`, `merchant_rules`, `ml_labels` | API service       | User-facing CRUD                         |
| `reprocessing_backups`                | Normalization service (reprocess job) | The reprocess job's snapshot/restore pair. The normalization service owns the reprocess flow per spec 003 §2.9. |
| `reprocessing_locks`                  | Ingestion (INSERT) + Normalization (DELETE) | Co-owned by design. Ingestion API atomically INSERTs the lock-row inside the trigger transaction *before* submitting the K8s Job, closing the race where two concurrent triggers could submit duplicate jobs. The reprocess job pod (in the normalization service) verifies the row exists at startup (exits 0 cleanly if absent) and DELETEs it on completion. Neither service UPDATEs the row — it has no mutable state. This is one of two documented exceptions to the single-writer rule; the other is `transactions`/`transfer_match_anomalies` above. Rationale in spec 003 §2.9. |

A service that doesn't own a table must never INSERT, UPDATE, or DELETE rows in it. If a feature requires cross-service writes, redesign — either move the write to the owning service behind an internal API, or re-evaluate ownership.

### Row-Level Security

All user-scoped tables enforce RLS. User-facing services (API, ingestion) set `app.current_user_id` at the start of every DB transaction. The consumer is the single exception — it processes events on behalf of all users and operates above RLS with full table access.

Repository methods on user-scoped tables MUST include `WHERE user_id = $1` (with `user_id` passed from the service layer) as part of the primary WHERE clause. RLS is the safety net, not the primary isolation mechanism. Two reasons: (1) **query plan stability** — indexes on user-scoped tables are `(user_id, ...)`-prefixed, and an explicit `WHERE user_id = $1` keeps the planner hitting the index instead of scan-then-RLS-filter; (2) **survivability under temporary RLS disablement** — if RLS is disabled for ad-hoc debugging or a migration carve-out, the query must still scope correctly to one user without the policy.

### Source isolation via Strategy pattern

Bank-specific behavior is encapsulated behind Protocol interfaces with per-source implementations registered in a `dict[str, Strategy]`. This applies across the entire pipeline:

- **Ingestion:** per-source routers, clients, adapters (already organized under `sources/{bank}/`)
- **Consumer normalization:** `NormalizationStrategy` per source (raw bank payload → `NormalizedTransaction`)
- **Consumer pipeline layers:** `TransferDetectionStrategy`, future `ClassificationStrategy` — each bank implements its own or is skipped

**The rule:** no `if source == "monobank"` in generic code. If behavior varies by source, define a Protocol and register implementations. Generic layers dispatch via the registry, never by inspecting the source name. This extends beyond file organization (already covered above) to runtime dispatch — the consumer's pipeline orchestrator, the ingestion service's backfill coordinator, and any future cross-source logic must all use registry-based dispatch.

**Rationale:** PUMB and Revolut are on the roadmap. Each new bank should require only new files in `sources/{bank}/` and new strategy registrations — zero modifications to existing generic code.

### Information preservation (reprocessability)

Every stored transaction must be reprocessable through the pipeline without re-fetching from bank APIs. This means:

- The `transactions` table must persist all fields present in `NormalizedTransaction`. If a pipeline layer needs input that isn't stored, storage must be extended — not worked around.
- Pipeline layers must not silently discard information that downstream layers or future reprocessing might need.
- Metadata merging (e.g. rate conversion results into the `metadata` JSONB) must be reversible — keys added by computation must be strippable without losing original bank metadata.

**The test:** "Can I delete this user's transactions and replay them from the stored data alone, getting the same result?" If no, the information-loss rule is violated.

**Boundary:** normalization (raw bank payload → `NormalizedTransaction`) is NOT reprocessable from stored data. If the normalization service has a bug, the fix is re-fetching from the bank API. This is a separate operational procedure, not a reprocess variant.

### Per-user scoping

All background processing, locking, and batch operations are scoped to a single user. This is a load-bearing design choice, not an optimization:

- Transfer detection pairs transactions within one user's accounts only
- Reprocessing locks, deletes, and replays one user's data at a time
- Advisory locks are keyed by `user_id`
- Future operations (classification retraining, forecast refresh, scheduled event projection) must follow the same pattern

**The rule:** if a new feature processes transactions, it must accept a `user_id` and touch only that user's data. Cross-user batch operations iterate users sequentially (one at a time), never in a single query or transaction.

**Why this matters:** per-user scoping keeps the deletion window small during reprocessing, makes advisory locks granular, prevents one user's data issue from blocking another, and aligns with RLS boundaries. Changing this would require redesigning reprocessing, locking, and concurrency control — unplanned divergence is a bug.

### Shared package conventions

The `shared/src/grosh_shared/` package contains **schema-free contracts** consumed across services, organized into four sub-packages by concern. New shared modules MUST land in the right sub-package; if nothing fits, the answer is usually that the module is not actually shared and belongs in the consuming service.

| Sub-package    | Contains                                                                                  | Example modules                                                                                |
|----------------|-------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------|
| `domain/`      | Business shape — entities, enums, ISO code registries that name parts of the business     | `models` (User, Account, BankIntegration + all core enums), `normalized`, `iso_4217`, `mcc`    |
| `messaging/`   | Between-service contracts that travel over Kafka or HTTP                                  | `envelope` (Kafka), `ids` (UUID5 transaction-id generator), `jobs` (K8s Job trigger/status)    |
| `db/`          | Postgres plumbing                                                                         | `url` (DSN dialect), `rls` (session-var setters + advisory locks), `testing` (test-DB lifecycle) |
| `http/`        | HTTP-layer helpers (only used by FastAPI-serving processes)                               | `auth` (JWT decode), `errors` (RFC 7807 envelope + FastAPI exception handlers)                 |

**The exception — `domain/normalized.py` carries DB-schema awareness deliberately.** The `TransactionRow` dataclass (a persistence-row shape with column names matching the `transactions` table) and its `to_normalized()` method (the inverse mapping from a stored row back to a `NormalizedTransaction` — strips `metadata.layer`, preserves `metadata.source`) live in shared because:

1. The normalization service's reprocess flow needs the inverse mapping to reconstruct events from stored rows during replay.
2. The enrichment service's persistence layer already encodes the forward mapping.
3. Co-locating both shapes in shared keeps them in sync without forcing a normalization-imports-enrichment dependency.

The trade-off: a future `transactions` schema change touches one extra file (`shared/.../domain/normalized.py`) alongside the migration and `enrichment`'s `transaction_repo.py`. This is acceptable because the change set is small and locally co-located, and the alternative (cross-service import or duplicated mapping logic that drifts) is worse.

**Revisit trigger.** If a future change requires a third service to import `TransactionRow` without needing the reprocess inverse mapping, extract `to_normalized()` into a normalization-private module first and let only `NormalizedTransaction` stay in shared. The exception is narrow on purpose.

**The API-service no-unify rule.** `services/api/src/grosh_api/repositories/transaction_repo.py` has its own local `TransactionRow` dataclass for read-only HTTP response shapes (`GET /v1/transactions`). It does NOT import the shared `TransactionRow` and the two classes must NOT be unified despite the name overlap. The API's version is a query-result row; the shared version is the reprocess inverse-mapping shape. Unifying them would expand the schema-awareness exception to a third service that doesn't need reprocess, breaking the rationale above. Enforced at CI time by `tests/e2e/integration/test_single_writer_carveout.py` plus code review.

---

## Key Architecture Decisions & Rationale

**Why Redpanda over plain async FastAPI?**
With 3 users and multiple consumers (categorizer, family aggregator, notification service,
forecast invalidator), a broker decouples producers from consumers cleanly. Each consumer
group processes events independently. Also: genuine streaming learning value.

**Why plain PostgreSQL over TimescaleDB?**
TimescaleDB was evaluated and removed (see `adr-drop-timescaledb.md`). The composite PK tax,
continuous aggregate watermark footgun, and 5.4 GB image size were not justified by the
negligible chunk-exclusion benefit at per-user query scale. Aggregations are computed on read
(sub-ms with proper indexes at our data volume). `pg_cron` replaces retention policies.

**Why k3s over Docker Compose for production?**
Learning goal. Rolling deploys, service discovery, ingress routing, ConfigMaps/Secrets,
health probes — real K8s primitives that transfer to enterprise work. Operational overhead
is acceptable given the explicit learning intent.

**Why not AWS EKS?**
EKS control plane costs $73/month before any compute. k3s on Hetzner gives identical
learning experience at a fraction of the cost.

**Why not Actual Budget?**
Actual Budget was evaluated and rejected. It's a good budgeting app but its SQLite/CRDT
architecture conflicts with the Python ML pipeline, it has no investment tracking, and
having two UIs (Actual + custom forecasting) would be worse than one unified interface.
Building from scratch is the right call here.

---

## Streaming Consumer Topology

```
Monobank webhook (per user)
        │
  FastAPI receiver (ingestion)
        │
  Redpanda: raw_transactions.{source}
  (per-source topics, key = user_id)
        │
  [Normalization Service]
        │
  Redpanda: normalized_transactions
        │
  [Enrichment Service]
  (transfer detection → currency conversion → classification → persistence)
        │
        ▼
   PostgreSQL
```

See `adr-consumer-pipeline-architecture.md` for the full two-consumer design.

---

## Security Architecture

- **Network**: Only ports 80/443 exposed publicly. All inter-service traffic on internal
  Docker/k3s network. Hetzner firewall + ufw block everything else.
- **TLS**: Automatic via Caddy / Traefik + Let's Encrypt.
- **Auth**: JWT with refresh token rotation. Access tokens: 15min TTL. Refresh tokens: 30 days,
  httpOnly cookies. No public registration — admin creates users manually.
- **Monobank token storage**: Encrypted in DB with pgcrypto `pgp_sym_encrypt`. App encryption
  key stored in Infisical, never in code or environment files.
- **Data isolation**: PostgreSQL Row-Level Security on all user-scoped tables. FastAPI sets
  `app.current_user_id` session variable from JWT claim at request start. Database enforces
  isolation at the query level regardless of application logic.
- **Family aggregates**: A `sharing_permissions` table controls what one user can see of
  another's data. Admin can see category-level aggregates; raw transactions require explicit
  per-user grant.

---

## Key Database Tables (outline)

- `users` — user accounts, roles (admin / member)
- `accounts` — Monobank cards + manual accounts, linked to user, includes cashback_type
- `counterparties` — tagged cards/IBANs (e.g. "mom's card", "friend"), with default_category
- `transactions` — core fact table (regular table, PK on id): user_id, account_id, time,
  amount, currency, description, mcc, cashback_amount, balance, category_id, classified_by
  (rule/mcc/ml/manual), confidence
- `transaction_embeddings` — pgvector table, links to transaction, stores description embedding
- `merchant_rules` — fast-path lookup: normalized_merchant / counterparty_iban → category
- `categories` — user-defined category tree
- `scheduled_events` — recurring or one-off future cash flows (salary, rent, subscriptions)
- `sharing_permissions` — user_id, target_user_id, permission_level (aggregate / full)
- `ml_labels` — labeled training samples for classifier retraining

All user-scoped tables have `user_id` column with RLS policies.

---

## Monobank API Notes

- Personal token auth (X-Token header). Does not expire unless revoked.
- Store encrypted, never in plaintext.
- Webhook payload: `{type: "StatementItem", data: {account: "...", statementItem: {...}}}`
- Transaction fields include: `id`, `time` (unix), `description`, `mcc`, `amount` (cents),
  `operationAmount`, `currencyCode`, `cashbackAmount`, `balance`, `hold`
- For FOP accounts: `counterEdrpou`, `counterIban` available
- Rate limit: 1 statement request per 60 seconds per account — use webhooks for real-time,
  paginated statement pulls only for initial historical backfill (max 31 days per request)
- Cashback type per account (UAH, Miles, None) — fetch from client info endpoint, store
  on account record

---

## Monorepo Layout

```
services/api/        FastAPI webhook receiver and REST API
services/normalization/  Redpanda normalization consumer — raw bank payloads → NormalizedTransaction
services/enrichment/     Redpanda enrichment consumer — transfer detection, conversion, classification, persistence
services/ml/         Classifier (sentence-transformers + pgvector) and Prophet forecasting
services/frontend/   Next.js App Router UI
shared/              pip-installable Pydantic models shared across Python services
infra/               Docker Compose, Terraform (Hetzner), k3s manifests
.github/workflows/   CI/CD (GitHub Actions)
context/product/     Product definition, roadmap, architecture docs
```

### Python packaging conventions
- Build backend: `hatchling` for all Python packages (zero extra config files)
- Shared models imported as `grosh-shared @ ../shared` in each service's `pyproject.toml`
- All Python services (`api`, `normalization`, `enrichment`, `ml`) follow the same `src/` layout
- Dev extras declared under `[project.optional-dependencies] dev = [...]`

---

## Local Dev Setup

- `infra/docker-compose.yml` — base stack (PostgreSQL, Redpanda, all services)
- `infra/docker-compose.dev.yml` — dev overlay: hot reload on FastAPI + Next.js, adds `cloudflared`
- Run with: `make dev` (or `docker compose -f infra/docker-compose.yml -f infra/docker-compose.dev.yml up`)
- Dev secrets: `infra/.env` (gitignored). Infisical used in production.
- No caching layer (Redis): not needed at ~3-user scale. Compute-on-read aggregations + TanStack Query client cache are sufficient.

### Local K8s (Docker Desktop)

K8s runs locally via Docker Desktop. Jobs (backfill, reprocessing) run as K8s Jobs in the `grosh` namespace, connecting to the same Postgres and Redpanda as the Compose stack via `host.docker.internal`.

**Prerequisites (verify with `kubectl get` before recreating):**
- Namespace: `grosh`
- ServiceAccount: `grosh-ingestion` (with Role + RoleBinding from `infra/k8s/ingestion-rbac.yaml`)
- Secret: `grosh-secrets` — all env vars the Jobs need (DB URL, encryption key, Kafka bootstrap, etc.)

K8s Jobs (backfill, reprocess) are constructed programmatically at submit time by `BackfillService` and `ReprocessDispatcher` in the ingestion service — there are no static YAML manifests to apply. The operator-side trigger is the corresponding HTTP endpoint (`POST /v1/monobank/accounts/{id}/backfill`, `POST /v1/admin/rates-backfill`, `POST /v1/admin/reprocess`).

If a Job fails, check `kubectl logs` and the Secret contents first.

**Updating images after code changes:**

Docker Desktop K8s uses containerd, which does NOT share images with the Docker CLI. `docker build` produces images invisible to K8s pods. Without the import step below, pods get a stale cached version.

The canonical path is `make dev-k8s-setup` — it builds + imports `grosh-ingestion:latest`, `grosh-normalization:latest`, and `grosh-enrichment:latest` (each service has its own image; reprocess Jobs use the normalization image since they run `python -m grosh_normalization.reprocess_main`), regenerates the per-credential K8s secrets, and is fully idempotent. Run it after any change to normalization, enrichment, or ingestion code that needs to be picked up by a K8s Job.

To do it manually for a single service:

```bash
# 1. Build the service image via Compose. The -p grosh project name tags the
#    image directly as grosh-<service>:latest — no separate `docker tag` retag
#    needed (the legacy infra-<service>:latest pattern is gone).
docker compose -p grosh -f infra/docker-compose.yml build <service>

# 2. Import into containerd's k8s.io namespace — makes it visible to pods
docker save grosh-<service>:latest | docker exec -i desktop-control-plane ctr -n k8s.io images import --all-platforms -

# 3. Delete stale jobs so the next trigger picks up the new image
kubectl delete jobs -n grosh -l app=<job-label>
```

Example for the normalization service (also the image used by reprocess Jobs):
```bash
docker compose -p grosh -f infra/docker-compose.yml build normalization
docker save grosh-normalization:latest | docker exec -i desktop-control-plane ctr -n k8s.io images import --all-platforms -
kubectl delete jobs -n grosh -l app=grosh-reprocess
```

`imagePullPolicy` must be `Never` (local image, no registry). Node name `desktop-control-plane` is Docker Desktop's K8s node.

---

## Learning Context

This is partly a learning project. When implementing features, include short inline explanation
boxes for technologies, patterns, or concepts that may be unfamiliar. Format them like this:

> **What is X?**
> One or two sentences explaining what this technology/pattern does and why it's used here.

Keep them concise — they are reference hints, not tutorials. The user can ask to elaborate.
Apply this to: new libraries, infrastructure primitives (Redpanda topics, pg_cron jobs,
k3s constructs, RLS policies), ML/forecasting concepts, and non-obvious design patterns.

---

## AWOS Workflow

This project uses the **AWOS** framework for all feature development. Quick reference:

| Command | Purpose |
|---|---|
| `/awos:spec` | Define the functional spec (what & why) |
| `/awos:tech` | Define the technical spec (how) |
| `/awos:tasks` | Break the tech spec into vertical slices with agent assignments |
| `/awos:implement` | Execute the next incomplete task slice |
| `/awos:verify` | Verify a completed slice against its acceptance criteria |
| `/awos:roadmap` | Discuss and update the product roadmap |
| `/awos:architecture` | Discuss architectural decisions |

Specs live in `context/spec/[index]-[name]/`:
- `functional-spec.md` — what the feature does
- `technical-considerations.md` — how it is built
- `tasks.md` — incremental runnable slices with agent assignments

**Agents:** Always check `.claude/agents/` before assigning tasks. Available specialists:
- `python-backend` — FastAPI, Pydantic, JWT, Redpanda consumers, enrichment pipeline
- `nextjs-frontend` — Next.js App Router, React, shadcn/ui, TanStack Query, Recharts
- `postgres-database` — PostgreSQL, pgvector, pg_cron, RLS, migrations, query optimization
- `ml-forecasting` — sentence-transformers, pgvector k-NN, Prophet forecasting
- `k8s-infra` — k3s, Terraform, Docker Compose, GitHub Actions CI/CD
- `observability` — Loki, Prometheus, Grafana dashboards and alerting

Use `general-purpose` only when no specialist clearly matches.

---

## Build Order

See `context/product/roadmap.md` for the authoritative phased roadmap.

Summary of phases:
1. **Foundation** — infra provisioning, auth, transaction ingestion pipeline, basic feed UI
2. **Intelligence** — rule-based + MCC classification, ML embedding classifier, feedback loop
3. **Forecasting & Net Worth** — scheduled events, Prophet forecast, net worth dashboard
4. **Family Network** — RLS enforcement, sharing permissions, admin aggregate view
5. **Future** — Telegram notifications, additional bank integrations, investment tracking
