# Functional Specification: Transaction Ingestion Pipeline

- **Roadmap Item:** Transaction Ingestion Pipeline (Phase 1)
- **Status:** Draft
- **Author:** Nick

---

## 1. Overview and Rationale (The "Why")

The spreadsheet workflow requires manual entry of every transaction — easy to forget, painful to backfill, and impossible to forecast from. The ingestion pipeline replaces this by automatically pulling transactions from Monobank in real time (webhook) and on demand (historical backfill), routing them through a two-stage streaming pipeline (normalization → enrichment), and storing them in PostgreSQL.

Users also need to record cash transactions that don't flow through any bank. All transaction sources — automated and manual — converge into the same Redpanda-based pipeline so that downstream consumers and future features have a single, consistent data source.

A key challenge is the user's FOP (sole proprietor) account structure: salary arrives on a USD FOP account, moves to a UAH FOP account (taxes paid there), then gets transferred in small batches to a UAH credit card for spending. Without transfer detection, these internal movements pollute income/expense numbers with false signals. The pipeline must distinguish real income/expense from internal transfers from the start — using a deterministic 3-tier algorithm that handles both IBAN-visible transfers and card-to-card movements with no API-level correlation.

**Success criteria:**

- All Monobank transactions arrive automatically via webhook — zero manual entry for bank transactions.
- Historical transactions importable via backfill on first setup; re-running backfill produces no duplicates.
- Internal transfers between user's own accounts are correctly detected and tagged — including card-to-card transfers that have no counterparty IBAN in the API response.
- Cash transactions recordable manually through the same pipeline.
- REST API endpoints expose paginated transactions, flexible time-window aggregates (any bucket size, any currency, any date range), and account listings.
- Aggregations are computed on read from a single SQL query — no materialized views, no staleness after backfill or reclassification. Cross-currency totals (UAH, USD, EUR) powered by per-bank exchange rates stored in an SCD Type 2 table.
- Exchange rates are polled from Monobank (every 5 minutes) and NBU (daily). Stale or missing rates fall back through a configurable chain. Historical rates are backfillable from NBU for any past date range.
- Users can trigger reprocessing of their own transactions to benefit from pipeline improvements (new transfer detection logic, rate backfills, future classification) without re-fetching from bank APIs.

---

## 2. Functional Requirements (The "What")

### 2.1 Monobank Account Linking

Each user can link their Monobank account from a Settings page.

- **Acceptance Criteria:**
  - [ ] User pastes their Monobank personal API token on the Settings page.
  - [ ] Grosh calls Monobank `/personal/client-info`, fetches all accounts (cards), and displays them to the user for confirmation.
  - [ ] On confirmation, Grosh registers the webhook URL with Monobank (`POST /personal/webhook`).
  - [ ] Account records are created in the database with account type, currency, and cashback type from Monobank's response.
  - [ ] The Monobank token is stored encrypted (pgcrypto) — never in plaintext.
  - [ ] After successful linking, the user is prompted: "Import recent transactions?" with a button to trigger backfill.
  - [ ] If webhook registration fails, the user sees a clear error message and can retry.
  - [ ] User can re-link (update token and re-register webhook) without recreating accounts via `POST /monobank/relink`.
  - [ ] Admin can re-register webhooks for all active integrations after a domain change via `make dev-reregister-webhooks` (local) or `scripts/reregister-webhooks/prod.sh` (production). Source-agnostic: each bank implements `WebhookReregistrationProvider`, dispatched via registry.

### 2.2 Monobank Webhook Receiver

FastAPI endpoint receives real-time transaction pushes from Monobank.

- **Acceptance Criteria:**
  - [ ] `GET /webhook/monobank` responds with `200 OK` (Monobank's verification request).
  - [ ] `POST /webhook/monobank` accepts Monobank's `StatementItem` payload.
  - [ ] The endpoint validates that the account ID in the payload matches a registered account in Grosh.
  - [ ] Valid transactions are published to the Redpanda `raw_transactions.monobank` topic as raw bank payloads, partitioned by `user_id`.
  - [ ] The endpoint responds within Monobank's timeout — publishing to Redpanda is fast; no synchronous processing.
  - [ ] Monobank does not provide webhook payload signatures. Authentication relies on the opaque webhook secret embedded in the URL path (`/webhook/{secret}`), validated against the DB. The secret is generated per-integration and is unguessable (UUID4). This is the only verification mechanism beyond the initial GET handshake.

### 2.3 Historical Backfill

Users can import historical transactions from Monobank.

- **Acceptance Criteria:**
  - [ ] A "Backfill" button is available on the Settings page per linked Monobank account.
  - [ ] Triggering backfill starts a background job that paginates through Monobank's statement API (max 31 days per request, 1 request per 60 seconds rate limit per account).
  - [ ] Each batch of transactions is published to the same per-source Redpanda topic (`raw_transactions.monobank`).
  - [ ] The user sees a progress indicator (or at minimum a status: "Backfilling..." / "Complete").
  - [ ] Backfill is safe to re-run — the consumer deduplicates by Monobank transaction ID.
  - [ ] Backfill covers the maximum available history (up to 31 days per request, paginating backward).

### 2.4 Transaction Consumer (Two-Stage Pipeline)

The consumer is split into two stages connected by an intermediate Redpanda topic (`normalized_transactions`).

**Stage 1 — Normalization consumer:** subscribes to per-source raw topics (`raw_transactions.monobank`, `raw_transactions.manual`, etc.), dispatches to per-source normalization strategies, and publishes a source-agnostic `NormalizedTransaction` to `normalized_transactions`.

**Stage 2 — Pipeline consumer:** subscribes to `normalized_transactions`, runs transfer detection → currency conversion → classification → persistence.

- **Acceptance Criteria:**
  - [ ] Consumer deduplicates by transaction source ID (Monobank ID or manual entry ID) — duplicate publishes are silently dropped via `ON CONFLICT (id) DO NOTHING`.
  - [ ] Consumer writes transactions to the `transactions` table (regular PostgreSQL table, PK on `id`) with all fields: user_id, account_id, time, amount (account currency), operation_amount (original currency), currency_code, description, mcc, cashback_amount, balance, hold status.
  - [ ] All three display-currency amounts (`amount_uah_cents`, `amount_usd_cents`, `amount_eur_cents`) are denormalized at write time using per-bank exchange rates from the `currency_rates` SCD2 table.
  - [ ] Consumer detects internal transfers using a deterministic 3-tier algorithm (see §2.4.1).
  - [ ] Each transaction has two orthogonal classification axes: `direction` (immutable money flow: `income`, `expense`, `zero`) set by the normalizer from the amount sign, and `special_category` (pipeline enrichment: NULL for ordinary transactions, `transfer` for internal movements, future values: `cancellation`, `hold`). These are independent — a transfer leg is still directionally `income` or `expense`. Aggregation queries filter on `special_category IS NULL` to exclude non-ordinary transactions.
  - [ ] Consumer processes messages from all sources (webhook, backfill, manual) identically — bank-specific logic is confined to normalization strategies and transfer detection strategies.
  - [x] The `hold` flag is stored as-is from the bank but not used for filtering. Monobank's historical API returns unreliable hold values — the flag reflects the internal processing pipeline, not settlement status. Aggregates filter on `special_category IS NULL` to include only ordinary income/expense. Deduplication uses `ON CONFLICT DO NOTHING`.

#### 2.4.1 Transfer Detection (3-Tier Algorithm)

Internal transfers between the user's own accounts are detected deterministically. Monobank does not provide a transfer correlation ID — particularly for card-to-card transfers, both legs look identical to external P2P in the raw API response. The algorithm uses three tiers tried in priority order:

| Tier | Signal | What it detects |
|------|--------|-----------------|
| A | `counterparty_iban` matches an own account | FOP↔FOP, FOP→card (sender side) |
| B | Another existing unclaimed tx has `counterparty_iban` pointing at my account | Receiver side where sender's IBAN names me |
| C | Both legs have NULL IBAN; `operation_amount_cents` cross-match ±2s + semantic description validation | Pure card↔card transfers (including FX) |

- **Acceptance Criteria:**
  - [ ] Tier A: if `counterparty_iban` matches an own account, search for the unclaimed partner on that account (opposite type, ±2s, `related_transaction_id IS NULL`). Exactly 1 → claim pair.
  - [ ] Tier B: if another unclaimed tx already has `counterparty_iban = my account's IBAN`, that tx is the partner. Exactly 1 → claim pair.
  - [ ] Tier C: both legs have NULL IBAN. Match by `operation_amount_cents` cross-match (expense's `operation_amount = income's amount` and vice versa) within ±2s, different account, opposite type. Requires semantic description validation against known internal transfer patterns.
  - [ ] Ambiguity rejection: if any tier finds >1 valid candidate → reject all, record anomaly (never guess).
  - [ ] Claim lock: paired transactions are linked via `related_transaction_id` (self-FK) and `special_category` set to `'transfer'`. The `direction` field is preserved (`in`/`out`) — a transfer leg remains directional. The claim query uses `FOR UPDATE SKIP LOCKED` to prevent concurrent double-claims.
  - [ ] Unpaired transfer-like descriptions (starting with "З " or "На ") that remain unmatched → record anomaly for visibility (the partner may not have arrived yet or the account isn't linked).
  - [ ] Anomalies auto-resolve: when a partner arrives later and the pair is successfully claimed, any `unpaired_*` anomaly on either leg is deleted.
  - [ ] External P2P (person names, masked card numbers) correctly excluded — no false positives.
  - [ ] Source-agnostic design: the transfer detection strategy is registered per-source. Each bank implements its own detection logic (or relies on explicit transfer flags in normalized data). Generic pipeline code dispatches via registry.

### 2.5 Manual Entry

Users can log cash transactions not captured by any bank.

- **Acceptance Criteria:**
  - [ ] User can create a manual account of type `cash` from the Settings page.
  - [ ] User can create a manual transaction with: amount, date, description, transaction type, and account (cash account).
  - [ ] Manual transactions are published to the `raw_transactions.manual` Redpanda topic.
  - [ ] Manual transactions appear alongside Monobank transactions in all views.
  - [ ] Manual transactions are included in chart aggregates.

### 2.6 REST API Endpoints

The pipeline exposes data to frontend consumers via REST.

- **Acceptance Criteria:**
  - [ ] `GET /transactions` — paginated list of transactions, filterable by direction (`income`/`expense`), `special_category` (`transfer`, or NULL for ordinary), account, date range, and `unconverted_currency` (shows only rows where the specified currency amount is NULL).
  - [ ] `GET /transactions/aggregates` — flexible time-window aggregation (see §2.7 for full design).
  - [ ] `GET /accounts` — list of the authenticated user's accounts (Monobank and manual).
  - [ ] `GET /rates` — paginated list of currency rates, filterable by source, currency pair, and date range. Rates are global (not user-scoped).
  - [ ] `GET /rates/at` — all rates active at a given timestamp (defaults to now), with optional source filter. SCD2 point-in-time query.
  - [ ] All endpoints are scoped to the authenticated user via JWT + RLS (except rates, which are global).

### 2.7 Aggregation API (Compute-on-Read)

Time-window aggregations are computed on read from a single SQL query — no materialized views, no refresh policies, no staleness after backfill or reclassification. With an index on `(user_id, time DESC)`, any aggregation pattern completes in under 1ms at per-user scale.

- **Acceptance Criteria:**
  - [ ] `GET /transactions/aggregates` accepts parameters: `currency` (comma-separated, default: UAH,USD,EUR), `from`/`to` (date range, default: all history), `bucket` (day/week/month/quarter/year, default: month), `fields` (income/expense/delta, default: all three).
  - [ ] Response groups results by `period_start`, with a `currencies` dict containing requested currencies. Each currency object has `total_income_cents`, `total_expense_cents`, `delta_cents`, and `converted_pct`.
  - [ ] Internal transfers and other special categories are excluded from aggregation by default (`WHERE special_category IS NULL` — includes only ordinary income/expense).
  - [ ] `converted_pct` reports the percentage of transactions in each bucket that have a non-NULL value for that currency's amount column. When < 100%, the frontend can link to the transaction list with `?unconverted_currency=X` to show the specific unconverted rows.
  - [ ] Timezone-aware bucketing: `date_trunc` uses `AT TIME ZONE` with the user's timezone (stored in `user_settings`, default UTC, auto-detected from browser on first load) so that a transaction at 23:30 on Jan 31 falls into January, not February.
  - [ ] `fields` parameter is a presentation concern — the query always computes all values; `fields` controls which are included in the JSON response. Allows the frontend to request only `delta` for sparklines or only `income,expense` for bar charts.
  - [ ] All requested currencies are computed in a single query (one index scan, one grouping pass).

### 2.8 Currency Rate Reliability

Exchange rate sources may be unavailable (app downtime, API outage). The system must handle stale or missing rates gracefully.

- **Acceptance Criteria:**
  - [ ] Each rate source has a configured fallback chain (e.g., Monobank → NBU) loaded per-transaction via a recursive CTE. The consumer walks the chain in priority order.
  - [ ] Rate resolution uses two quality tiers (FRESH → CLOSEST). Both legs of a chained conversion must resolve at the same tier.
  - [ ] The system tracks when each rate was last confirmed (polled). FRESH rates have `last_polled_at` within `K * update_cadence_seconds` of the transaction time (K=2). CLOSEST is a last resort within a 7-day window, ranked by proximity to the transaction time.
  - [ ] Currency conversion uses liquidation-side pricing: `rate_buy` when multiplying (bank buys from user), `rate_sell` when dividing (bank sells to user), `rate_mid` as fallback when the chosen side is NULL. Never substitutes the opposite side.
  - [ ] The full rate path is recorded in transaction metadata with per-step traceability: `from`, `to`, `source`, `rate_id`, `rate`, `rate_side` (buy/sell/mid), `tier` (fresh/stale/closest), `op` (multiply/divide). Path-level: `effective_rate`, `hops`, `quality` (worst tier), `sides` used.
  - [ ] Pivot currencies for chained conversions are derived from `rate_source_config.base_currencies` (no hard-coded pivot). Pivots are ordered by chain depth then array position.
  - [ ] Historical exchange rates can be backfilled from NBU for any past date range. NBU supports date-range queries per currency (`?start=YYYYMMDD&end=YYYYMMDD&valcode=USD`), so backfilling 2 years of all ~45 currencies requires ~45 HTTP requests (~3 MB storage).
  - [ ] Historical rate backfill is triggered by an admin via a K8s Job (same pattern as transaction backfill).
  - [ ] The fallback chain configuration is stored in the database, editable without redeployment. Cyclic or excessively deep chains (>10 hops) raise `RateSourceChainError` at query time.
  - [ ] Rate fallback is restricted to configured sources — unconfigured sources cannot affect conversions.
  - [ ] Missing rates result in NULL amounts, not zero or approximations from untrusted sources. NULL amounts can be repaired later via reprocessing after rates are backfilled.

### 2.9 Transaction Reprocessing

The consumer pipeline evolves over time — new layers are added (transfer detection, classification), rate backfills provide previously missing conversion data, and bug fixes improve stored results. Users need a way to re-run the full pipeline on their historical data without re-fetching from Monobank (rate-limited, slow, unnecessary).

- **Acceptance Criteria:**
  - [ ] Users can trigger reprocessing of their own transactions via `POST /reprocess` (authenticated, acts on current user). No admin role required.
  - [ ] Reprocessing reads existing transaction rows from the DB, reconstructs the intermediate `NormalizedTransaction` from stored columns (source-agnostic — one format regardless of which bank originated the data), deletes original rows, and republishes to the pipeline topic for fresh processing through all downstream layers.
  - [ ] The reprocessing job backs up all affected data before deletion. If anything goes wrong during replay, the system can restore from backup.
  - [ ] During reprocessing, the user sees a "refreshing" indicator (data is temporarily incomplete).
  - [ ] Reprocessing is safe — concurrent webhook events for the same user are serialized against the reprocess job via advisory locks (no data loss from race conditions).
  - [ ] After reprocessing completes, all transactions are verified against the pre-delete snapshot. Missing rows trigger automatic restore from backup.
  - [ ] Batch reprocessing (all users or a subset) is available as a K8s Job for maintenance operations (new pipeline layer deployed, logic change, bulk fix).
  - [ ] Rate-limited: one reprocess per hour per user. Returns 429 if cooldown has not elapsed.
  - [ ] Idempotent: if interrupted and re-triggered, produces the same result without duplication or data loss.

---

## 3. Scope and Boundaries

### In-Scope

- Monobank account linking (self-service via Settings page)
- Monobank webhook receiver (GET verification + POST transaction ingestion)
- Monobank historical backfill with manual trigger and progress indication
- Two-stage Redpanda consumer pipeline: normalization (per-source) → enrichment (source-agnostic)
- Deterministic 3-tier transfer detection (IBAN match, reverse IBAN, operation_amount cross-match with description guard)
- Transfer match anomaly recording and auto-resolution
- Manual cash account creation and manual transaction entry (through Redpanda)
- Tri-currency storage (UAH + USD + EUR equivalents) on every transaction, denormalized at write time
- Currency rate ingestion from Monobank and NBU with configurable polling cadence
- Rate fallback chain (monobank → nbu) with stale rate detection via `last_polled_at`
- Admin-triggered historical rate backfill from NBU via K8s Job
- Rate source traceability in transaction metadata (per-step path with tier, side, operation)
- Compute-on-read aggregation API (any bucket, any currency, any date range, timezone-aware)
- REST API endpoints for transactions, aggregates, accounts, rates
- Transaction reprocessing (user-triggered + admin batch via K8s Job)
- Per-service database roles with write-privilege separation
- Access token revocation for instant logout
- Encrypted Monobank token storage

### Out-of-Scope

- Transaction feed UI and rolling monthly chart — separate spec (004)
- Transaction classification (rule engine, MCC fallback, ML classifier) — Phase 2
- Feedback loop UI for category labeling — Phase 2
- Per-account transaction view — future enhancement
- Scheduled events and forecasting — Phase 3
- Net worth dashboard and balance history — Phase 3
- Family sharing and RLS enforcement — Phase 4
- Debt tracking — deferred
- Non-Monobank bank integrations (PUMB, Revolut) — Phase 5
- Notification service — Phase 5
