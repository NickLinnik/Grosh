# Product Roadmap: Grosh

_This roadmap outlines our strategic direction based on customer needs and business goals. It focuses on the "what" and "why," not the technical "how."_

---

### Phase 1 — Foundation: Data In

_Get real transactions flowing and visible. This alone replaces the spreadsheet._

- [x] **Infrastructure & Auth**
  - [x] **Containerization:** Dockerfile for each service; Docker Compose baseline runs the full local stack via `make dev`.
  - [x] **User auth:** JWT with refresh token rotation; admin creates accounts manually; no public registration.

- [ ] **Transaction Ingestion Pipeline**
  - [ ] **Monobank webhook receiver:** FastAPI endpoint receives webhook payloads and publishes to Redpanda.
  - [ ] **Monobank historical backfill:** `POST /accounts/{id}/backfill` endpoint paginates through Monobank's statement API (max 31 days/request, 1 req/60s rate limit) and publishes each transaction to the same `raw_transactions` topic. Consumer deduplicates by transaction ID — safe to re-run.
  - [ ] **Transaction consumer:** Subscribes to Redpanda, deduplicates, writes raw transactions to TimescaleDB.
  - [ ] **Manual entry:** Users can log cash transactions and non-Monobank accounts manually.

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

- [ ] **Go Live**
  - [ ] **CI/CD build pipeline:** GitHub Actions builds Docker images and pushes to ghcr.io on every push.
  - [ ] **Deployment prep:** Terraform and k3s manifests written and validated. CI/CD deploy step configured (SSH deploy + rolling restart).
  - [ ] **Hetzner VPS provisioned:** Terraform provisions VPS, firewall, DNS records, and SSH key injection.
  - [ ] **Infisical self-hosted:** Secrets manager deployed on VPS; all services pull secrets at runtime via Infisical SDK.
  - [ ] **k3s running:** Stateless services (FastAPI, consumer, ML, Next.js) deployed as k3s workloads with Traefik ingress and TLS.
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

---

### Phase 4 — Family Network

_Share what matters, protect what doesn't._

- [ ] **Multi-user & Privacy**
  - [ ] **Row-Level Security enforcement:** All user-scoped tables protected via PostgreSQL RLS; app sets session variable from JWT.
  - [ ] **Sharing permissions:** `sharing_permissions` table controls what one user can see of another's data.
  - [ ] **Admin household aggregate view:** Admin sees category-level spend and net worth across the family; raw transactions require explicit per-user grant.
  - [ ] **Review auth deferred hardening:** Re-evaluate items in `context/spec/002-user-auth/technical-considerations.md §5` (refresh token reuse detection, access token revocation) against the current threat model. Implement those whose tradeoffs have become justified now that multiple users share data.

---

### Phase 5 — Future Considerations

_Post-v1, subject to reprioritization._

- [ ] **Notification Service:** Telegram bot for transaction alerts and weekly summaries.
- [ ] **Additional Bank Integrations:** Extend pipeline to support PUMB and Revolut (and other banks) alongside Monobank. Each bank publishes to the same `raw_transactions` Redpanda topic; only the adapter layer differs.
- [ ] **Multi-language UI:** Support Russian, Ukrainian and English in the frontend. Language selection per user profile.
- [ ] **Investment Portfolio Tracking:** Track assets beyond cash and bank accounts.
- [ ] **Model Upgrade:** Fine-tune transformer classifier once labeled dataset reaches ~500 examples.
- [ ] **GraphQL API layer:** Strawberry-based GraphQL API alongside REST, for flexible data fetching in the dashboard constructor feature.
- [ ] **Dynamic continuous aggregates:** Runtime creation of TimescaleDB continuous aggregates as shared query accelerators for the dashboard constructor. Registry table maps query signatures to materialized views; unused views are garbage-collected. Fallback to raw `time_bucket` queries when aggregate constraints don't fit. Only pursue if profiling shows raw queries are insufficient for actual usage patterns.
