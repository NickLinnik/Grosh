# Functional Specification: Transaction Ingestion Pipeline

- **Roadmap Item:** Transaction Ingestion Pipeline (Phase 1)
- **Status:** Draft
- **Author:** Nick

---

## 1. Overview and Rationale (The "Why")

The spreadsheet workflow requires manual entry of every transaction — easy to forget, painful to backfill, and impossible to forecast from. The ingestion pipeline replaces this by automatically pulling transactions from Monobank in real time (webhook) and on demand (historical backfill), routing them through a two-stage streaming pipeline (normalization → enrichment), and storing them in PostgreSQL.

Users also need to record cash transactions that don't flow through any bank. All transaction sources — automated and manual — converge into the same Redpanda-based pipeline so that downstream consumers and future features have a single, consistent data source.

A key challenge is the user's FOP (sole proprietor) account structure: salary arrives on a USD FOP account, moves to a UAH FOP account (taxes paid there), then gets transferred in small batches to a UAH credit card for spending. Without transfer detection, these internal movements pollute income/expense numbers with false signals. The pipeline must distinguish real income/expense from internal transfers from the start. For Monobank, this uses a deterministic 7-step algorithm (the v2 strategy — see `references/adr-transfer-detection-v2.md`) that fetches a single universal candidate set, ranks pairs by IBAN evidence + description evidence, and decides with a count-and-decide rule that handles both IBAN-visible transfers and card-to-card movements with no API-level correlation. Other banks register their own strategy; the orchestrator dispatches per source.

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
  - [ ] Consumer detects internal transfers using a per-source strategy. Monobank uses the v2 7-step algorithm (see §2.4.1). Other banks register their own; the orchestrator dispatches via registry — no `if source == "monobank"` branches in generic code.
  - [ ] Each transaction has two orthogonal classification axes: `direction` (immutable money flow: `income`, `expense`, `zero`) set by the normalizer from the amount sign, and `special_category` (pipeline enrichment: NULL for ordinary transactions, `transfer` for internal movements, future values: `cancellation`, `hold`). These are independent — a transfer leg is still directionally `income` or `expense`. Aggregation queries filter on `special_category IS NULL` to exclude non-ordinary transactions.
  - [ ] Consumer processes messages from all sources (webhook, backfill, manual) identically — bank-specific logic is confined to normalization strategies and transfer detection strategies.
  - [x] The `hold` flag is stored as-is from the bank but not used for filtering. Monobank's historical API returns unreliable hold values — the flag reflects the internal processing pipeline, not settlement status. Aggregates filter on `special_category IS NULL` to include only ordinary income/expense. Deduplication uses `ON CONFLICT DO NOTHING`.

#### 2.4.1 Transfer Detection (Monobank v2 — 7-Step Algorithm)

Internal transfers between the user's own accounts are detected deterministically. Monobank does not provide a transfer correlation ID — particularly for card-to-card transfers, both legs look identical to external P2P in the raw API response. The Monobank strategy implements a 7-step algorithm (full design in `references/adr-transfer-detection-v2.md`):

1. **MCC + idempotency gate** — only `mcc='4829'` rows enter; already-stored IDs no-op.
2. **Row flags** — derive `description_matched`, `multi_hop_description`, and `cp_iban_status` (`null` / `transitive` / `unlinked` / `honest`).
3a. **Unlinked-partner short-circuit** — if `cp_iban_status='unlinked'`, the partner is on an account the user hasn't linked. Skip the universal fetch; emit a row block; record an unpaired anomaly if the description looks like a transfer.
3. **Universal candidate fetch** — one `SELECT ... FOR UPDATE SKIP LOCKED` retrieves all unclaimed opposite-direction MCC 4829 rows on different accounts within the ±2s window matching the amount predicate.
4. **Hard IBAN consistency filter** — drops candidates whose IBAN evidence contradicts the incoming row (e.g., honest cp_iban points at a third party).
5. **Per-pair evidence classification** — each surviving candidate is labelled with the IBAN evidence class (bilateral / unilateral / none) and a description-evidence flag.
6. **Decide (count-and-decide + bucket-locked)** — bilateral evidence beats unilateral beats none; within the winning bucket, exactly one candidate → claim; more → ambiguous anomaly.
7. **Translate Decision → result** — `claim` produces a paired write + auto-resolve sweep of any prior `unpaired_*` anomaly on either leg; `anomaly` records a row; `skip` returns clean.

- **Acceptance Criteria:**
  - [ ] Single universal fetch replaces the v1 three-tier ladder — no separate per-tier queries.
  - [ ] Claim lock: paired transactions are linked via `related_transaction_id` (self-FK) and `special_category` set to `'transfer'`. The `direction` field is preserved (`income`/`expense`) — a transfer leg remains directional. The claim query uses `FOR UPDATE SKIP LOCKED` to prevent concurrent double-claims.
  - [ ] Both legs carry a byte-equal `metadata.layer.transfer.pair` sub-block (under the wrapped-metadata convention from `references/adr-transaction-reprocessing.md`).
  - [ ] Bucket-locked decision: bilateral IBAN evidence outranks unilateral outranks none. Within the winning bucket, exactly 1 candidate → claim; 0 → skip; >1 → record `ambiguous_pair_match` anomaly (never guess).
  - [ ] Unpaired transfer-like descriptions (`З ...` / `На ...` family) that remain unmatched → record `unpaired_from_description` or `unpaired_to_description` anomaly for visibility.
  - [ ] Anomalies auto-resolve: when a partner arrives later and the pair is successfully claimed, any `unpaired_*` anomaly on either leg is deleted in the same orchestrator transaction.
  - [ ] Multi-hop FOP↔FOP transfers (chain-end IBAN quirk) correctly pair end-to-end — Issue 2 stays resolved.
  - [ ] External P2P (person names, salary `Provectus IT, Inc`, tax authorities `ГУ ДПС`/`ГУК`, masked card numbers) correctly excluded — no false positives.
  - [ ] **Card-to-card definition (Monobank).** A "card-to-card transfer" is an MCC 4829 row with `counterparty_iban IS NULL` (Monobank does not expose the partner IBAN for direct card transfers) whose description matches the `З %` / `На %` transfer phrase family. The universal fetch pairs both legs by `operation_amount_cents` cross-match within ±2s, gated by description evidence. External P2P (person names like "Денис", merchant names, salary, tax authorities) lacks the transfer phrase and is therefore not pairable by the description-evidence path — guaranteeing the no-false-positive property above.
  - [ ] Anomaly enum is exactly the v2 set: `unpaired_from_description`, `unpaired_to_description`, `ambiguous_pair_match`, `description_account_mismatch`, `description_consistency_mismatch`. The v1 enum values are gone from the type itself.
  - [ ] Source-agnostic dispatch: the transfer detection strategy is registered per-source in a `dict[str, TransferDetectionStrategy]`. Generic pipeline code looks up by `source` and calls the protocol method — no `if source == "monobank"` branches.

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
  - [ ] `GET /transactions` — paginated list of transactions. Filter parameters:
    - `direction` (single value: `income`/`expense`)
    - `category` (repeated-key list of `special_category` enum values, e.g. `?category=transfer&category=cancellation`) — **whitelist**. Narrows the result to rows whose `special_category` is in the supplied set. Use for category-focused views (a "Transfers" tab, an audit drill-down). Rows with `special_category IS NULL` are never matched by this filter — pass them through by omitting the parameter or by using `exclude_category` instead.
    - `exclude_category` (repeated-key list of `special_category` enum values, e.g. `?exclude_category=transfer`) — **blacklist**. Hides rows whose `special_category` is in the supplied set; rows with `special_category IS NULL` (ordinary transactions) are always included. This is the canonical way for the frontend feed to ask "show me my spending, hide the noise" without breaking when new special categories ship later (a future `cancellation` or `hold` value automatically appears in such a feed instead of being silently dropped).
    - `account_id` (single UUID)
    - `from` / `to` (half-open date range `[from, to)`)
    - Both `category` and `exclude_category` accept repeated keys and use the existing `SpecialCategory` StrEnum (canonical case only; `?category=Transfer` returns 422). Omitted = no filter on that axis.
    - **Conflict rule.** If any value appears in both `category` and `exclude_category` for the same request (e.g., `?category=transfer&exclude_category=transfer`), the server returns 422 with a descriptive error. The two params combined otherwise (`?category=transfer&exclude_category=hold`) are allowed and compose as "include whitelist AND exclude blacklist," which only matters once more than one special category exists.
  - [ ] When `category` and `exclude_category` both contain the same value (e.g., `?category=transfer&exclude_category=transfer`), the server returns 422 with a descriptive error naming the conflicting value. Disjoint combinations (`?category=transfer&exclude_category=hold`) are allowed and return 200.
  - [ ] `GET /transactions/aggregates` — flexible time-window aggregation (see §2.7 for full design).
  - [ ] `GET /accounts` — list of the authenticated user's accounts (Monobank and manual).
  - [ ] `GET /rates` — paginated list of currency rates, filterable by source, currency pair, and date range. Rates are global (not user-scoped).
  - [ ] `GET /rates/at` — all rates active at a given timestamp (defaults to now), with optional source filter. SCD2 point-in-time query.
  - [ ] All endpoints are scoped to the authenticated user via JWT + RLS (except rates, which are global).

### 2.7 Aggregation API (Compute-on-Read)

Time-window aggregations are computed on read from a single SQL query — no materialized views, no refresh policies, no staleness after backfill or reclassification. With an index on `(user_id, time DESC)`, any aggregation pattern completes in under 1ms at per-user scale.

- **Acceptance Criteria:**
  - [ ] `GET /transactions/aggregates` accepts parameters: `currency` (repeated-key list of `UAH`/`USD`/`EUR`, default: all three), `from`/`to` (date range, default: all history), `bucket` (day/week/month/quarter/year, default: month), `fields` (repeated-key list of `income`/`expense`/`delta`, default: all three).
  - [ ] Response groups results by `period_start`, with a `currencies` dict containing requested currencies. Each currency object has `total_income_cents`, `total_expense_cents`, `delta_cents`, and `converted_pct`.
  - [ ] Internal transfers and other special categories are excluded from aggregation by default (`WHERE special_category IS NULL` — includes only ordinary income/expense).
  - [ ] `converted_pct` reports the percentage of transactions in each bucket that have a non-NULL value for that currency's amount column. When < 100%, the frontend can surface the unconverted rows by filtering the transaction list client-side; a dedicated drill-down endpoint will be added if access patterns warrant it.
  - [ ] Timezone-aware bucketing: `date_trunc` uses `AT TIME ZONE` with the user's timezone (stored in `user_settings`, default UTC, auto-detected from browser on first load) so that a transaction at 23:30 on Jan 31 falls into January, not February.
  - [ ] `fields` parameter is a presentation concern — the query always computes all values; `fields` controls which are included in the JSON response. Allows the frontend to request only `delta` for sparklines or only `income,expense` for bar charts.
  - [ ] All requested currencies are computed in a single query (one index scan, one grouping pass).

### 2.8 Currency Rate Reliability

Exchange rate sources may be unavailable (app downtime, API outage). The system must handle stale or missing rates gracefully.

- **Acceptance Criteria:**
  - [ ] Each rate source has a configured fallback chain (e.g., Monobank → NBU) loaded per-transaction via a recursive CTE. The consumer walks the chain in priority order.
  - [ ] Rate resolution uses two quality tiers (FRESH → CLOSEST). Both legs of a chained conversion must resolve at the same tier — never mix tiers (a FRESH leg + a CLOSEST leg yields NULL rather than a falsely "FRESH" conversion). Within a single tier, when multiple candidate sources/pairs exist, the system picks the candidate with the most recent `last_polled_at`. K (the FRESH grace multiplier on `update_cadence_seconds`) is a fixed per-source constant; current value is `K=2`.
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
  - [ ] After reprocessing completes, the job verifies that every transaction ID from the pre-delete snapshot exists in the `transactions` table again. If any IDs are missing, the job restores from backup, releases the lock, and surfaces the failure. The verification checks **presence by ID only** — pipeline-derived fields (`special_category`, `related_transaction_id`, converted amount columns, `metadata.layer.*`) are expected to change after reprocess. Bank-derived fields (per `references/adr-transaction-reprocessing.md`) must round-trip byte-identical. The full operational mechanics (advisory lock layout, staging buffer, drain task, backup retention) live in that ADR.
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
- Per-source transfer detection strategy registry. Monobank uses the v2 7-step algorithm (universal candidate fetch + IBAN/description evidence ranking + count-and-decide). Other banks plug in their own strategies without modifying generic code.
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
