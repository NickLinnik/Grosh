# Product Roadmap: Grosh

_This roadmap outlines our strategic direction based on customer needs and business goals. It focuses on the "what" and "why," not the technical "how."_

---

### Phase 1 — Foundation: Data In

_Get real transactions flowing and visible. This alone replaces the spreadsheet._

- [x] **Infrastructure & Auth**
  - [x] **Containerization:** Dockerfile for each service; Docker Compose baseline runs the full local stack via `make dev`.
  - [x] **User auth:** JWT with refresh token rotation; admin creates accounts manually; no public registration.

- [x] **Transaction Ingestion Pipeline**
  - [x] **Monobank webhook receiver:** Ingestion service endpoint receives webhook payloads, normalizes via bank adapter, and publishes to Redpanda.
  - [x] **Monobank historical backfill:** `POST /accounts/{id}/backfill` on the ingestion service triggers a K8s Job that paginates through Monobank's statement API (max 31 days/request, 1 req/60s rate limit) and publishes each transaction to the same `raw_transactions` topic. Consumer deduplicates by transaction ID — safe to re-run.
  - [x] **Transaction consumer:** Two-stage pipeline (normalization → pipeline). Subscribes to per-source Redpanda topics, normalizes via Strategy dispatch, then runs transfer detection → currency conversion → classification → persistence to PostgreSQL.
  - [x] **Manual entry:** Users can log cash transactions and non-Monobank accounts manually.
  - [x] **Currency rate ingestion:** Cron job polls Monobank `/bank/currency` endpoint, stores rates in an SCD Type 2 `currency_rates` table (source, currency pair, buy/sell/mid rates, valid_from/valid_to). NBU daily rates as fallback. Consumer uses per-bank rates to compute `amount_uah_cents`, `amount_usd_cents`, `amount_eur_cents` on each transaction at write time.

- [ ] **Basic Transaction Feed UI**
  - [ ] **Transaction list:** Next.js feed showing transactions with amount, date, description, and raw category.
  - [ ] **Account overview:** Simple per-account balance display.
  - [ ] **Rolling monthly aggregate chart:** Clustered bar chart showing income and expenses per month over a rolling 12-month window, with a delta (savings) trend line overlaid.

---

### Phase 2 — Intelligence: Classify & Label

_Make the data meaningful. Transactions get categorized automatically; users correct what the model gets wrong._

- [ ] **Rule-Based & MCC Classification**
  - [ ] **Merchant rules engine:** `merchant_rules` table maps counterparty/IBAN/merchant → category. Fast-path covers ~80% once populated.
  - [ ] **MCC fallback:** ISO 18245 MCC code → coarse category mapping applied when no rule matches.

- [ ] **Conversion Loss Tracking**
  - [ ] **FX rate source integration:** Research and integrate a historical exchange rate API. Store reference rates alongside bank-applied rates per transaction.
  - [ ] **Conversion loss calculation:** Compute per-transaction and monthly aggregate conversion cost (difference between reference market rate and actual rate received).
  - [ ] **Conversion loss visualization:** Surface conversion loss in the monthly chart (stacked bar segment or separate chart — to be determined in spec).

- [ ] **ML Embedding Classifier**
  - [ ] **Sentence-transformer embeddings:** multilingual MiniLM embeddings stored in pgvector; k-NN similarity search for unknown merchants.
  - [ ] **Feedback loop UI:** Surface unclassified / low-confidence transactions; user confirms or corrects; corrections accumulate as labeled training samples.
  - [ ] **Auto-promotion:** High-confidence recurring merchants are auto-promoted to merchant rules.

- [ ] **Real-time UI Updates**
  - [ ] **SSE endpoint:** Main API exposes `GET /events/stream` (JWT-authenticated, Server-Sent Events). A notification consumer subscribes to a `user_notifications` Redpanda topic and pushes events to connected clients.
  - [ ] **Frontend auto-refresh:** TanStack Query invalidates chart and feed caches on SSE events. New transactions appear without manual refresh.

- [ ] **Go Live**
  - [ ] **CI/CD build pipeline:** GitHub Actions builds Docker images and pushes to ghcr.io on every push.
  - [ ] **Deployment prep:** Terraform and k3s manifests written and validated. CI/CD deploy step configured (SSH deploy + rolling restart).
  - [ ] **Hetzner VPS provisioned:** Terraform provisions VPS, firewall, DNS records, and SSH key injection.
  - [ ] **Infisical self-hosted:** Secrets manager deployed on VPS; all services pull secrets at runtime via Infisical SDK.
  - [ ] **k3s running — full deployment:** All services in k3s, including stateful: PostgreSQL as StatefulSet with PVC, Redpanda as StatefulSet with PVC, plus stateless services (FastAPI, consumer, ML, Next.js) as Deployments. Traefik ingress and TLS.
  - [ ] **CI/CD deploy activated:** GitHub Actions SSH deploy step enabled; pushes to `main` trigger rolling restart on the VPS.
  - [ ] **Database backups:** Scheduled `pg_dump` via k8s CronJob, shipped to offsite storage (Hetzner Storage Box or S3-compatible). Retention policy: 7 daily, 4 weekly. Protects manual entries and other non-reconstructable data against volume loss.

---

### Phase 3 — Forecasting & Net Worth

_Turn historical data into a forward view. Answer "where will I be in 3 months?"_

- [ ] **Scheduled Events Engine**
  - [ ] **Scheduled events CRUD:** Users define recurring or one-off future cash flows (salary, rent, subscriptions).
  - [ ] **Deterministic forward projection:** Scheduled events projected as exact future cash flows on the forecast timeline.

- [ ] **Probabilistic Forecast**
  - [ ] **Prophet-based category forecasting:** Monthly category aggregates fed to Prophet; outputs expected spend with confidence intervals.
  - [ ] **Configurable horizon:** 90-day default; extends up to 1 year as transaction history grows.
  - [ ] **Combined forecast view:** Scheduled events + probabilistic variable spend displayed as a forward chart with uncertainty bands.

- [ ] **Net Worth Dashboard**
  - [ ] **Account aggregation:** All accounts (bank cards, cash, debts) summed per user and tracked over time.
  - [ ] **Balance history chart:** Time-series chart of total net worth.

- [ ] **Assets & Liabilities Tracking:** Separate domain from transactions — covers debts (loans given/received), investments, and other non-liquid assets. Own table with lifecycle (created, expected return date, resolved). Chart overlay option combines transaction aggregates with active assets to show net impact on wealth (e.g., a \$500 loan shows as an expense in cash flow but is offset by a \$500 receivable in the combined view). Transactions that create/resolve assets are linked via category ("Loan given", "Loan received"), not by merging data models.

---

### Phase 4 — Family Network

_Share what matters, protect what doesn't._

- [ ] **Multi-user & Privacy**
  - [ ] **Row-Level Security enforcement:** All user-scoped tables protected via PostgreSQL RLS; app sets session variable from JWT.
  - [ ] **Sharing permissions:** `sharing_permissions` table controls what one user can see of another's data.
  - [ ] **Admin household aggregate view:** Admin sees category-level spend and net worth across the family; raw transactions require explicit per-user grant.
  - [ ] **Review auth deferred hardening:** Re-evaluate items in `context/spec/002-user-auth/technical-considerations.md §5` (refresh token reuse detection, access token revocation) against the current threat model. Implement those whose tradeoffs have become justified now that multiple users share data.

- [ ] **Multi-language UI:** Support Russian, Ukrainian and English in the frontend. Language selection per user profile.

---

### Phase 5 — Future Considerations

_Post-v1, subject to reprioritization._

- [ ] **Notification Service:** Telegram bot for transaction alerts and weekly summaries.
- [ ] **Additional Bank Integrations:** Extend pipeline to support PUMB and Revolut (and other banks) alongside Monobank. Each bank publishes to the same `raw_transactions` Redpanda topic; only the adapter layer differs.
- [ ] **Model Upgrade:** Fine-tune transformer classifier once labeled dataset reaches ~500 examples.
- [ ] **GraphQL API layer:** Strawberry-based GraphQL API alongside REST, for flexible data fetching in the dashboard constructor feature.
- [ ] **Dynamic query acceleration:** If profiling shows raw aggregation queries are insufficient for actual usage patterns, introduce trigger-maintained summary tables or materialized views as query accelerators. Only pursue based on measured bottlenecks, not speculation.
