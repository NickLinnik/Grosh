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
- **Python consumers** (separate service or workers) — subscribe to Redpanda topics,
  run enrichment pipeline (deduplication → rule lookup → MCC fallback → ML classifier),
  write enriched transactions to TimescaleDB.

### Storage
- **TimescaleDB** (PostgreSQL + TimescaleDB extension) — `transactions` is a hypertable
  partitioned by time. Enables efficient time-range queries, continuous aggregates for
  monthly/weekly summaries, and native SQL for everything else.
- **pgvector** extension — stores transaction description embeddings for the k-NN classifier.
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
- **k3s** — lightweight single-node Kubernetes. Runs stateless services (FastAPI, ML service,
  Next.js, consumers). Traefik ingress is bundled with k3s.
- **Stateful services** (TimescaleDB, Redpanda) run as Docker Compose on the host alongside k3s,
  OR as StatefulSets with PVCs if full K8s learning is the goal. Decision deferred — start with
  Docker Compose for stateful, k3s for stateless, migrate later.
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

**Schema generality:**
Database tables shared across sources (like `bank_integrations`) use generic columns only. Source-specific fields go in `config JSONB`, not as top-level columns. This prevents schema changes when adding new bank integrations.

**Migration hygiene:**
Never create a new migration for changes to tables/columns from a migration that hasn't been merged to `main`. Modify the existing migration instead — it's still a draft on an unmerged branch.

**SQL style:**
Use triple-quoted strings for queries. One item per line in all clauses — SELECT columns, WHERE predicates, ORDER BY keys, GROUP BY keys. Keywords (`SELECT`, `FROM`, `WHERE`, `ORDER BY`, `GROUP BY`) on their own lines. No module-level column-list constants unless the same list is used in 3+ queries.

**Keep docs in sync with code changes:**
When making code changes outside a planned AWOS task — refactors, reviewer fixes, ad-hoc improvements — update all affected documentation before considering the work done. The spec is the primary source of truth:
- `context/spec/{spec}/technical-considerations.md` — file structure tables, API contracts, topic lists, shared model tables
- `context/spec/{spec}/tasks.md` — task descriptions must reflect actual implementation, not the original plan
- `infra/grosh.postman_collection.json` — add/update/remove requests when endpoints change

---

## Key Architecture Decisions & Rationale

**Why Redpanda over plain async FastAPI?**
With 3 users and multiple consumers (categorizer, family aggregator, notification service,
forecast invalidator), a broker decouples producers from consumers cleanly. Each consumer
group processes events independently. Also: genuine streaming learning value.

**Why TimescaleDB over plain PostgreSQL?**
Time-range queries and monthly aggregations on transactions are the core access pattern.
TimescaleDB hypertables with continuous aggregates make these efficient without any extra
infrastructure. It's still PostgreSQL — all tooling, ORMs, and SQL knowledge applies.

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
  FastAPI receiver
        │
  Redpanda topic: raw_transactions
  (partitioned by user_id)
        │
  ┌─────┼──────────────┬───────────────────┐
  │     │              │                   │
  ▼     ▼              ▼                   ▼
Enrich  Family      Notification       Forecast
+Classify Aggregator  Service          Invalidator
  │     │
  ▼     ▼
TimescaleDB
```

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
- `transactions` — hypertable, core fact table: id, user_id, account_id, time, amount,
  currency, description, mcc, cashback_amount, balance, category_id, classified_by
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
services/consumer/   Redpanda consumer — enrichment and classification pipeline
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
- All three Python services (`api`, `consumer`, `ml`) follow the same `src/` layout
- Dev extras declared under `[project.optional-dependencies] dev = [...]`

---

## Local Dev Setup

- `infra/docker-compose.yml` — base stack (TimescaleDB, Redpanda, all services)
- `infra/docker-compose.dev.yml` — dev overlay: hot reload on FastAPI + Next.js, adds `cloudflared`
- Run with: `make dev` (or `docker compose -f infra/docker-compose.yml -f infra/docker-compose.dev.yml up`)
- Dev secrets: `infra/.env` (gitignored). Infisical used in production.
- No caching layer (Redis): not needed at ~3-user scale. TimescaleDB continuous aggregates + TanStack Query client cache are sufficient.

---

## Learning Context

This is partly a learning project. When implementing features, include short inline explanation
boxes for technologies, patterns, or concepts that may be unfamiliar. Format them like this:

> **What is X?**
> One or two sentences explaining what this technology/pattern does and why it's used here.

Keep them concise — they are reference hints, not tutorials. The user can ask to elaborate.
Apply this to: new libraries, infrastructure primitives (Redpanda topics, TimescaleDB hypertables,
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
- `postgres-database` — TimescaleDB, pgvector, RLS, migrations, query optimization
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
