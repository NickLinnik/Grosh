# Functional Specification: Transaction Ingestion Pipeline

- **Roadmap Item:** Transaction Ingestion Pipeline (Phase 1)
- **Status:** Slices 1–28 shipped. The consumer was split into separate `normalization` and `enrichment` services. A final pre-merge hardening pass (§2.11) closes a small set of defense-in-depth gaps, schema-contract lies, and silently-failing observability holes before merge to `main`.
- **Author:** Nick

---

## 1. Overview and Rationale (The "Why")

The spreadsheet workflow requires manual entry of every transaction — easy to forget, painful to backfill, and impossible to forecast from. The ingestion pipeline replaces this by automatically pulling transactions from Monobank in real time (webhook) and on demand (historical backfill), routing them through a two-stage streaming pipeline (normalization → enrichment), and storing them in PostgreSQL.

Users also need to record cash transactions that don't flow through any bank. All transaction sources — automated and manual — converge into the same Redpanda-based pipeline so that downstream consumers and future features have a single, consistent data source.

A key challenge is the user's FOP (sole proprietor) account structure: salary arrives on a USD FOP account, moves to a UAH FOP account (taxes paid there), then gets transferred in small batches to a UAH credit card for spending. Without transfer detection, these internal movements pollute income/expense numbers with false signals. The pipeline must distinguish real income/expense from internal transfers from the start. For Monobank, this uses a deterministic 7-step algorithm (the v2 strategy — see `references/adr-transfer-detection-v2.md`) that fetches a single universal candidate set, ranks pairs by IBAN evidence + description evidence, and decides with a count-and-decide rule that handles both IBAN-visible transfers and card-to-card movements with no API-level correlation. Other banks register their own strategy; the orchestrator dispatches per source.

**Success criteria — delivered by slices 1–28 (shipped):**

- All Monobank transactions arrive automatically via webhook — zero manual entry for bank transactions.
- Historical transactions importable via backfill on first setup; re-running backfill produces no duplicates.
- Internal transfers between user's own accounts are correctly detected and tagged — including card-to-card transfers that have no counterparty IBAN in the API response.
- Cash transactions recordable manually through the same pipeline.
- REST API endpoints expose paginated transactions, flexible time-window aggregates (any bucket size, any currency, any date range), and account listings.
- Aggregations are computed on read from a single SQL query — no materialized views, no staleness after backfill or reclassification. Cross-currency totals (UAH, USD, EUR) powered by per-bank exchange rates stored in an SCD Type 2 table.
- Exchange rates are polled from Monobank (every 5 minutes) and NBU (daily). Stale or missing rates fall back through a configurable chain. Historical rates are backfillable from NBU for any past date range.
- Users can trigger reprocessing of their own transactions to benefit from pipeline improvements (new transfer detection logic, rate backfills, future classification) without re-fetching from bank APIs.

**Success criteria — pre-merge hardening pass (§2.11, in progress):**

- Every RLS-protected user-scoped table rejects writes (INSERT/UPDATE) where `user_id ≠ app.current_user_id()`, not just hides reads — RLS becomes a write barrier, not just a read filter.
- A CI test fails on any future user-scoped table added without RLS enabled and at least one policy.
- The OpenAPI schema for manual entry endpoints reflects what the handlers actually accept — no `direction = "zero"` admitted at parse time, no arbitrary `type` string accepted on cash accounts.
- The `reprocessing_backups` 30-day retention is enforced by `pg_cron`, not by documentation alone.
- A misconfigured frontend cannot trigger more than one reprocess per user per hour through the public endpoint; admin retains a `force: true` escape hatch.
- If the staging-drain sweep stalls, the failure is visible in logs within 5 minutes — not silent until a user notices missing transactions.
- The CLAUDE.md "Row-Level Security" subsection states the "WHERE user_id = $1 is still mandatory in repos; RLS is defense in depth" convention explicitly.

---

## 2. Functional Requirements (The "What")

### 2.1 Monobank Account Linking

Each user can link, list, and unlink their Monobank account from a Settings page. The lifecycle has full CRUD-shaped endpoints (list / link / delete); `link` is idempotent on the Monobank `clientId` so re-linking the same Monobank user rebinds rather than duplicates.

**Endpoints (all under `/v1/`, per §2.10):**
- `GET /v1/monobank/integrations` — list the current user's Monobank integrations.
- `POST /v1/monobank/link` — link a new integration OR rotate the token on an existing one (idempotent).
- `DELETE /v1/monobank/integrations/{integration_id}` — hard delete the integration record.

There is no separate `relink` endpoint — token rotation is subsumed by idempotent `link`.

- **Acceptance Criteria:**
  - [ ] User pastes their Monobank personal API token on the Settings page.
  - [ ] Grosh calls Monobank `/personal/client-info`, fetches all accounts (cards), and displays them to the user for confirmation. On Monobank API timeout, 5xx, or 429: return 502 with `code: MONOBANK_API_UNAVAILABLE`. On 401/403 (token rejected): return 422 with `code: MONOBANK_TOKEN_INVALID`. No DB rows are created before client-info validation succeeds.
  - [ ] On confirmation, Grosh registers the webhook URL with Monobank (`PATCH /personal/webhook`).
  - [ ] Account records are created in the database with account type, currency, and cashback type from Monobank's response.
  - [ ] The Monobank token is stored encrypted (pgcrypto) — never in plaintext. The Monobank `clientId` is persisted on `bank_integrations.config` as `{"monobank_client_id": "..."}` so future `link` calls can match for idempotency.
  - [ ] After successful linking, the user is prompted: "Import recent transactions?" with a button to trigger backfill.
  - [ ] `POST /v1/monobank/link` is idempotent on `monobank_client_id`:
    - **No existing integration for the user:** insert a fresh row. If accounts with matching `(source='monobank', external_account_id)` exist (left over from a prior hard-deleted integration of the *same* Monobank user), they are **rebound** to the new integration row — historical transaction continuity preserved. Match by `external_account_id` AND `monobank_client_id`; accounts from a previously-deleted *different* Monobank user are never rebound.
    - **Existing integration with the same `monobank_client_id`:** rotate the encrypted token in-place; no new accounts created. Returns 200 OK.
    - **Existing integration with a *different* `monobank_client_id`:** return 409 with `code: INTEGRATION_ALREADY_LINKED`. The user must DELETE the existing integration before linking a different Monobank account — prevents accidental rebinding of one user's account history to another Monobank account.
  - [ ] `POST /v1/monobank/link` returns 201 Created when a fresh `bank_integrations` row is inserted; 200 OK when only the token is rotated. The HTTP status reflects DB-row-level state, not webhook-registration outcome.
  - [ ] If webhook registration with Monobank fails (network error, 5xx, timeout), the integration row remains in DB but the webhook is inert. The response's `webhook_registered: false` field signals this to the frontend. **The frontend treats this as a non-blocking warning, not an error**: shows a banner ("Live transaction sync is paused — retry connection") with a manual-retry button that re-POSTs `/v1/monobank/link` (idempotent rebind). The frontend does not auto-retry. The platform does not retry in the background.
  - [ ] `POST /v1/monobank/link` returns a typed `MonobankLinkResponse` (no bare dict). Shape: `integration_id`, `is_new` (bool — whether a fresh row was inserted), `webhook_registered` (bool), `accounts: [{account_id, external_account_id, currency_code, was_rebound}]`. `was_rebound` is `false` for accounts created in this call, `true` for accounts that already existed and were rebound from a prior deleted integration.
  - [ ] `GET /v1/monobank/integrations` returns the current user's Monobank integrations as `list[MonobankIntegrationResponse]`. A flat list — cursor pagination is unnecessary (a user typically has 0 or 1 integration; the family scale will never exceed single digits).
  - [ ] `DELETE /v1/monobank/integrations/{integration_id}` hard-deletes the `bank_integrations` row. The integration's `user_id` must equal the calling user (or the caller must be admin); otherwise return 404 with `code: INTEGRATION_NOT_FOUND` (do not reveal existence of integrations owned by other users).
  - [ ] On DELETE, accounts and historical transactions remain in the DB untouched. They become inert — no new webhook events arrive. A subsequent `POST /v1/monobank/link` with the same Monobank user rebinds them per the idempotency contract above.
  - [ ] On DELETE, Monobank webhook de-registration is best-effort with a 10-second timeout. The local delete proceeds regardless. If Monobank's API call fails (timeout, 5xx), a WARN is logged and the orphan webhook remains on Monobank's side; orphan delivery attempts hit our endpoint with a now-unknown `webhook_secret` and are rejected with 404 — acceptable log noise, not a data integrity issue.
  - [ ] Admin can re-register webhooks for all active integrations after a domain change via `make dev-reregister-webhooks` (local) or `scripts/reregister-webhooks/prod.sh` (production). Source-agnostic: each bank implements `WebhookReregistrationProvider`, dispatched via registry.
  - [ ] Integration test exercises continuity: link as `client_id` A → backfill → DELETE → re-link as `client_id` A → confirm new transactions land on the original `account_id` rows.
  - [ ] Integration test exercises isolation: link as `client_id` A → DELETE → link as `client_id` B → confirm B's accounts are fresh and A's accounts remain bound to the deleted integration's row (not rebound to B).

### 2.2 Monobank Webhook Receiver

FastAPI endpoint receives real-time transaction pushes from Monobank.

- **Acceptance Criteria:**
  - [ ] `GET /monobank/webhook/{webhook_secret}` responds with `200 OK` (Monobank's verification request). The path is **unversioned by design** — the URL is registered with Monobank and cannot be changed without re-registering every active integration's webhook. All other endpoints are versioned under `/v1/` (per §2.10); webhook receivers are the documented exception.
  - [ ] `POST /monobank/webhook/{webhook_secret}` accepts Monobank's `StatementItem` payload.
  - [ ] The endpoint validates that the account ID in the payload matches a registered account in Grosh.
  - [ ] Valid transactions are published to the Redpanda `raw_transactions.monobank` topic as raw bank payloads, partitioned by `user_id`.
  - [ ] The endpoint responds within Monobank's timeout — publishing to Redpanda is fast; no synchronous processing.
  - [ ] Monobank does not provide webhook payload signatures. Authentication relies on the opaque webhook secret embedded in the URL path (`/monobank/webhook/{secret}`), validated against the DB. The secret is generated per-integration and is unguessable (UUID4). This is the only verification mechanism beyond the initial GET handshake.

### 2.3 Historical Backfill

Users can import historical transactions from Monobank. Backfill runs as a K8s Job; the frontend polls a status endpoint for progress.

**Endpoints:**
- `POST /v1/monobank/accounts/{account_id}/backfill` — trigger backfill for a single account; returns 202 with `JobTriggerResponse`.
- `GET /v1/monobank/accounts/{account_id}/backfill/{job_id}` — poll job status; returns `JobStatusResponse` (per §2.10).

- **Acceptance Criteria:**
  - [ ] A "Backfill" button is available on the Settings page per linked Monobank account.
  - [ ] `POST /v1/monobank/accounts/{account_id}/backfill` accepts query parameters `from` (date, inclusive) and `to` (date, **exclusive** — half-open `[from, to)`, per CLAUDE.md). Validation:
    - `from < to` — otherwise 422 with `code: INVALID_DATE_RANGE`.
    - `(to - from).days <= 31` — otherwise 422 with `code: BACKFILL_WINDOW_TOO_LARGE` and a `detail` naming the requested span vs. the 31-day Monobank limit. The 31-day cap is the per-request Monobank constraint; the job pod is responsible for splitting valid windows into chunks per the rate limit (1 request per 60s), but the API rejects any single trigger that asks for more than 31 days upfront.
    - `to` is treated as the start of the named day (00:00:00 UTC) — exclusive. An integration test exercises `from=2025-01-01&to=2025-02-01` (exactly 31 days, half-open) accepted and `from=2025-01-01&to=2025-02-02` (32 days) rejected.
  - [ ] On success, returns 202 Accepted with `JobTriggerResponse` (`job_id` + `status_url`). The `status_url` is the relative path the frontend uses verbatim — never construct it by string concatenation.
  - [ ] The job paginates through Monobank's statement API (max 31 days per request, 1 request per 60 seconds rate limit per account).
  - [ ] Each batch of transactions is published to the same per-source Redpanda topic (`raw_transactions.monobank`).
  - [ ] The user sees progress by polling `GET /v1/monobank/accounts/{account_id}/backfill/{job_id}`. The response carries native K8s status (pending/running/succeeded/failed) plus a `failure_reason` when failed. Application-level progress (e.g. "47/120 batches imported") is deferred — Phase 2 enhancement.
  - [ ] Backfill is safe to re-run — the consumer deduplicates by Monobank transaction ID.
  - [ ] Backfill covers the maximum available history (up to 31 days per request, paginating backward).
  - [ ] The K8s Job carries labels `app.kubernetes.io/managed-by=grosh-ingestion`, `grosh.app/job-kind=monobank_backfill`, `grosh.app/account-id={uuid}`, and `grosh.app/user-id={uuid}`. Backfill is inherently per-account-and-per-user-scoped (an account belongs to exactly one user via the `accounts.user_id` FK), so both labels are always present — distinct from admin bulk reprocess jobs which span multiple users and therefore omit the `user-id` label (the user list lives in the `USER_IDS_JSON` env var on the pod instead). The status endpoint verifies both `account_id` (URL scope) and `user_id` (caller scope) against the labels before returning.
  - [ ] Authorization: the account must be owned by the caller (or caller is admin). Otherwise 404 with `code: ACCOUNT_NOT_FOUND` (don't reveal existence). A job_id that doesn't match the URL's account scope returns 404 with `code: JOB_NOT_FOUND` (don't reveal existence).
  - [ ] If the K8s API is unreachable when the status endpoint is called, return 503 with `code: JOB_STATUS_UNAVAILABLE`. Frontend should retry with exponential backoff (1s, 2s, 4s, 8s, capped at 60s); after 5 consecutive 503s, surface "Job status temporarily unavailable" and stop polling.
  - [ ] If the K8s API is unreachable when the trigger endpoint is called (Job cannot be submitted), return 502 with `code: JOB_SUBMISSION_FAILED`. The user may immediately retry.
  - [ ] Completed Jobs are kept by K8s for at least 3600 seconds (`ttlSecondsAfterFinished >= 3600`) so a polling client can capture the terminal status. After GC, status endpoint returns 404 with `code: JOB_NOT_FOUND`.

### 2.4 Transaction Consumer (Normalization + Enrichment Services)

The consumer runs as **two separate long-lived services** connected by an intermediate Redpanda topic (`normalized_transactions`). Each service is independently deployable, restartable, and observable; their failure domains are isolated.

**Normalization service** (`grosh-normalization`): owns every producer of `normalized_transactions`. Three internal producers run in one process:
- Steady-state normalization — subscribes to per-source raw topics (`raw_transactions.monobank`, `raw_transactions.manual`, etc.), dispatches to per-source normalization strategies, publishes `NormalizedTransaction`.
- Staging drain — periodically sweeps `staging_normalized_transactions` rows whose `user_id` no longer holds a reprocessing lock and republishes them to `normalized_transactions`.
- Reprocess job (K8s Job) — reads stored `transactions` rows, reconstructs `NormalizedTransaction` via the inverse mapping, deletes originals, republishes, and polls for catchup. Runs as a separate pod from the same image with a distinct entrypoint.

**Enrichment service** (`grosh-enrichment`): a pure consumer of `normalized_transactions`. Runs transfer detection → currency conversion → classification → persistence. Knows nothing about reprocess, staging, or normalization.

**Cohesion rule:** "Which service owns each module?" is answered by "who produces events on `normalized_transactions`?" The normalization service owns every producer (steady-state normalization, staging drain, reprocess job); the enrichment service owns the single consumer of that topic plus the downstream enrichment layers. The reprocess job belongs to the normalization service because it republishes events to the topic — it does not invoke enrichment layers itself; the enrichment service picks the republished events up through its normal consumer path. See `references/adr-consumer-pipeline-architecture.md` §"Service split (Slice 28)" for the full cohesion rationale, the image-and-deployment strategy (one image, three entrypoints — `python -m grosh_normalization.main`, `python -m grosh_enrichment.main`, `python -m grosh_normalization.reprocess_main`), and the single-writer carve-outs the split introduces.

- **Acceptance Criteria:**
  - [ ] Consumer deduplicates by transaction source ID (Monobank ID or manual entry ID) — duplicate publishes are silently dropped via `ON CONFLICT (id) DO NOTHING`.
  - [ ] Consumer writes transactions to the `transactions` table (regular PostgreSQL table, PK on `id`) with all fields: user_id, account_id, time, `amount_cents` (account currency), `operation_amount_cents` (original currency), `currency_code`, `description`, `mcc`, `cashback_amount_cents`, `balance_cents`, hold status. **All monetary amounts in this spec and in the database are integers in the minor currency unit (cents for UAH/USD/EUR — i.e. 1/100 of the major unit). Float storage is never used for amounts.**
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

**Endpoints (all under `/v1/`, per §2.10):**
- `POST /v1/manual/accounts` — create a manual account (201 Created, returns `ManualAccountResponse`).
- `PUT /v1/manual/accounts/{account_id}` — rename/edit a manual account (200 OK, returns `ManualAccountResponse`).
- `POST /v1/manual/transactions` — create a manual transaction (201 Created, returns `ManualTransactionResponse`).

- **Acceptance Criteria:**
  - [ ] User can create a manual account of type `cash` from the Settings page. `POST /v1/manual/accounts` returns a typed `ManualAccountResponse` (no bare dict).
  - [ ] User can rename a manual account via `PUT /v1/manual/accounts/{account_id}` returning `ManualAccountResponse`.
  - [ ] User can create a manual transaction with: amount, date, description, transaction type, and account (cash account). `POST /v1/manual/transactions` returns a typed `ManualTransactionResponse`.
  - [ ] Manual transactions are published to the `raw_transactions.manual` Redpanda topic.
  - [ ] Manual transactions appear alongside Monobank transactions in all views.
  - [ ] Manual transactions are included in chart aggregates.
  - [ ] Authorization: the account referenced by `POST /v1/manual/transactions` must be owned by the caller; otherwise 404 with `code: ACCOUNT_NOT_FOUND`.

### 2.6 REST API Endpoints

The pipeline exposes data to frontend consumers via REST. All endpoints are mounted under `/v1/` (per §2.10). Every endpoint declares a `response_model` and produces a fully-resolved OpenAPI schema — no bare-dict returns anywhere.

- **Acceptance Criteria:**
  - [ ] `GET /v1/transactions` — paginated list of transactions. Filter parameters:
    - `direction` (single value: `income`/`expense`)
    - `category` (repeated-key list of `special_category` enum values, e.g. `?category=transfer&category=cancellation`) — **whitelist**. Narrows the result to rows whose `special_category` is in the supplied set. Use for category-focused views (a "Transfers" tab, an audit drill-down). Rows with `special_category IS NULL` are never matched by this filter — pass them through by omitting the parameter or by using `exclude_category` instead.
    - `exclude_category` (repeated-key list of `special_category` enum values, e.g. `?exclude_category=transfer`) — **blacklist**. Hides rows whose `special_category` is in the supplied set; rows with `special_category IS NULL` (ordinary transactions) are always included. This is the canonical way for the frontend feed to ask "show me my spending, hide the noise" without breaking when new special categories ship later (a future `cancellation` or `hold` value automatically appears in such a feed instead of being silently dropped).
    - `account_id` (single UUID)
    - `from` / `to` (half-open date range `[from, to)`)
    - Both `category` and `exclude_category` accept repeated keys and use the existing `SpecialCategory` StrEnum (canonical case only; `?category=Transfer` returns 422). Omitted = no filter on that axis.
    - **Conflict rule.** If any value appears in both `category` and `exclude_category` for the same request (e.g., `?category=transfer&exclude_category=transfer`), the server returns 422 with a descriptive error. The two params combined otherwise (`?category=transfer&exclude_category=hold`) are allowed and compose as "include whitelist AND exclude blacklist," which only matters once more than one special category exists.
  - [ ] When `category` and `exclude_category` both contain the same value (e.g., `?category=transfer&exclude_category=transfer`), the server returns 422 with a descriptive error naming the conflicting value. Disjoint combinations (`?category=transfer&exclude_category=hold`) are allowed and return 200.
  - [ ] `GET /v1/transactions/aggregates` — flexible time-window aggregation (see §2.7 for full design).
  - [ ] `GET /v1/accounts` — list of the authenticated user's accounts (Monobank and manual).
  - [ ] `GET /v1/rates` — paginated list of currency rates, filterable by source, currency pair, and date range. Rates are global (not user-scoped).
  - [ ] `GET /v1/rates/at` — all rates active at a given timestamp (defaults to now), with optional source filter. SCD2 point-in-time query (`valid_from <= at AND (valid_to IS NULL OR valid_to > at)`). Distinct from `GET /v1/rates` which filters on `valid_from`; this endpoint answers "what rates applied *then*" rather than "what rates were *published* in this window."
  - [ ] `GET /v1/admin/users` — admin-only paginated list of users with activity information (see §2.6.1).
  - [ ] All endpoints are scoped to the authenticated user via JWT + RLS (except rates, which are global, and the admin user list, which is admin-only).
  - [ ] Every endpoint declares an explicit `response_model` — no bare `dict` returns. An OpenAPI completeness check (CI) parses the live `/openapi.json` from both services and asserts every operation has a non-empty `responses[*].content[*].schema` (no `additionalProperties: true` stubs).

#### 2.6.1 Admin User List

Admin endpoint for listing users with activity information. Used by future household-aggregate views (Phase 4) but exposed in Phase 1 for operational visibility.

- **Acceptance Criteria:**
  - [ ] `GET /v1/admin/users` returns `CursorPage[AdminUserResponse]`. Fields: `id`, `email`, `role`, `created_at`, `last_active_at`.
  - [ ] Admin-only. Non-admin callers receive 403 with `code: INSUFFICIENT_PERMISSIONS`.
  - [ ] Pagination uses the existing `CursorPage` envelope (consistent with `GET /v1/transactions` and `GET /v1/rates`). Sort order: `created_at DESC, id DESC`. The cursor encodes the composite `(created_at, id)` tuple — opaque to clients. Successor predicate in SQL: `(created_at, id) < (cursor_created_at, cursor_id)`.
  - [ ] Page size: `limit=50` default, `ge=1, le=200` bounds.
  - [ ] An invalid (undecodable) cursor returns 400 with `code: INVALID_CURSOR`.
  - [ ] `last_active_at` definition: time the user last issued an access token (sign-in or refresh-token rotation). NOT time of last individual API request — that would be too noisy for the DB. NULL means the user has not authenticated since the column was added.
  - [ ] A new migration adds `users.last_active_at TIMESTAMPTZ NULL` (no default — backfill is NULL). The migration does NOT add an `UPDATE` trigger on this column — the auth service updates it explicitly on each access-token issuance.

### 2.7 Aggregation API (Compute-on-Read)

Time-window aggregations are computed on read from a single SQL query — no materialized views, no refresh policies, no staleness after backfill or reclassification. With an index on `(user_id, time DESC)`, any aggregation pattern completes in under 1ms at per-user scale.

- **Acceptance Criteria:**
  - [ ] `GET /v1/transactions/aggregates` accepts parameters: `currency` (repeated-key list of `UAH`/`USD`/`EUR`, default: all three), `from`/`to` (date range, default: all history), `bucket` (day/week/month/quarter/year, default: month), `fields` (repeated-key list of `income`/`expense`/`delta`, default: all three).
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

**Endpoints (all under `/v1/`, per §2.10):**
- `POST /v1/users/{user_id}/reprocess` — per-user trigger. Authorized as `caller.id == user_id OR caller.role == admin`. Empty request body. Returns 202 with `JobTriggerResponse`.
- `GET /v1/users/{user_id}/reprocess/{job_id}` — poll status. Same authorization.
- `POST /v1/admin/reprocess` — admin-only bulk trigger. Body: `{"user_ids": list[UUID] | null}` (null = "all users"). Returns 202 with `BulkReprocessResponse`.
- `GET /v1/admin/reprocess/{job_id}` — poll bulk status. Admin-only.

**Lock ownership and race-free triggering.** The `reprocessing_locks` table has schema `(user_id PK, locked_at TIMESTAMPTZ)` — the **presence of a row** is the lock (no `status` column). The triggering API owns lock creation, not the job pod, so that two concurrent triggers cannot both pass a stale "is there a lock?" check and submit duplicate jobs.

Concrete sequence per trigger:
1. API receives the request.
2. API begins a DB transaction and attempts `INSERT INTO reprocessing_locks (user_id) VALUES ($1)`. On `unique_violation`: return 409 with `code: REPROCESS_LOCKED`. On success, the API now owns the lock within its transaction.
3. API submits the K8s Job (with `USER_IDS_JSON` env var carrying the target user_id(s)). If submission fails (K8s API unreachable, sync exception): the API rolls back the lock-row insert and returns 502 with `code: JOB_SUBMISSION_FAILED`. The user may immediately retry.
4. API commits and returns 202.
5. The job pod starts. At startup it **checks for the lock-row presence** for each user it is processing. If the lock is absent (the rare network-glitch case where the K8s Job was created but the API lost the ack and rolled back), the pod logs `"Reprocessing lock not found for user_id={uuid}; exiting cleanly"`, exits with status code 0, and the K8s Job ends in `status: succeeded`. No `transactions` rows are touched.
6. On normal completion (or fatal failure), the pod deletes its lock row(s).

- **Acceptance Criteria:**
  - [ ] Users can trigger reprocessing of their own transactions via `POST /v1/users/{user_id}/reprocess` where `user_id == caller.id`. Admins can additionally trigger reprocess of any user's data via the same URL with `user_id == target_user.id`.
  - [ ] Admin can trigger bulk reprocess of any subset (or all users) via `POST /v1/admin/reprocess` with body `{"user_ids": list[UUID] | null}`. `null` means "every current user."
  - [ ] Rate-limit: **1 reprocess per hour per `user_id`**, enforced by `users.last_reprocess_started_at` and an atomic UPDATE-RETURNING. Returns 429 with `code: RATE_LIMITED` when blocked. Admin retains a `force: true` escape hatch on the bulk endpoint. Full design in §2.11.5 (added by the pre-merge hardening pass). The advisory lock is still the primary correctness mechanism — the rate-limit is a separate defense against retry storms and a hostile authenticated user cycling "trigger → complete → trigger" repeatedly.
  - [ ] On concurrent trigger for an already-locked user, return 409 with `code: REPROCESS_LOCKED`. The response body's `detail` follows the exact format `"Reprocessing already in progress for user {user_id}. Existing job: {job_id}. Poll {status_url} for progress."` so a client can parse the running job's identity and status URL from a single human-readable string without requiring extra envelope fields. The job identity is resolved via K8s label selector `grosh.app/job-kind=reprocess,grosh.app/user-id={uuid}` (returns at most one active job per user by lock invariant).
  - [ ] The API owns lock-row creation (atomic INSERT within the trigger transaction); the job pod does NOT insert into `reprocessing_locks` on startup. Pod startup **asserts** the row exists; if absent, exits 0 cleanly without touching any `transactions` rows. Integration test exercises this path: (a) trigger a reprocess, (b) before the pod starts, DELETE the lock row manually, (c) confirm the pod logs the expected message and exits 0.
  - [ ] Bulk admin reprocess processes the target user list one-by-one inside the API request (not inside the job pod): for each `user_id`, attempt the lock insert. Users whose insert fails are added to `skipped` with reason `REPROCESS_LOCKED`. Users whose insert succeeds become the actual reprocess targets. The K8s Job is then submitted with the successful user_ids as its `USER_IDS_JSON` env var.
  - [ ] Bulk admin reprocess with `user_ids: null` enumerates current user IDs via a single `SELECT id FROM users` **snapshot**, then runs the per-user lock-insert loop on the snapshot list. Users created after the snapshot is taken are NOT included in this job — deliberate snapshot semantic ("reprocess everyone who existed at trigger time"). Later invocations can pick up new users.
  - [ ] Bulk admin response: `BulkReprocessResponse` with `job_id: str | None`, `status_url: str | None`, `skipped: list[{user_id, reason}]`. `skipped` is always present, always a list (empty when no users skipped). A model validator asserts `job_id` and `status_url` are either both null or both non-null — never one without the other.
  - [ ] When every user in a bulk request is skipped (every target had an active lock), the API returns 202 with `job_id: null`, `status_url: null`, and `skipped` containing the full list. Clients branch on `job_id is null` to skip polling and show the skipped list directly.
  - [ ] Reprocessing reads existing transaction rows from the DB, reconstructs the intermediate `NormalizedTransaction` from stored columns (source-agnostic — one format regardless of which bank originated the data), deletes original rows, and republishes to the pipeline topic for fresh processing through all downstream layers.
  - [ ] The reprocessing job backs up all affected data before deletion. If anything goes wrong during replay, the system can restore from backup.
  - [ ] During reprocessing, the user sees a "refreshing" indicator (data is temporarily incomplete).
  - [ ] Reprocessing is safe — concurrent webhook events for the same user are serialized against the reprocess job via advisory locks (no data loss from race conditions; full mechanics in `references/adr-transaction-reprocessing.md`).
  - [ ] After reprocessing completes, the job verifies that every transaction ID from the pre-delete snapshot exists in the `transactions` table again. If any IDs are missing, the job restores from backup, releases the lock, and surfaces the failure. The verification checks **presence by ID only** — pipeline-derived fields (`special_category`, `related_transaction_id`, converted amount columns, `metadata.layer.*`) are expected to change after reprocess. Bank-derived fields (per `references/adr-transaction-reprocessing.md`) must round-trip byte-identical. The full operational mechanics (advisory lock layout, staging buffer, drain task, backup retention) live in that ADR.
  - [ ] K8s Jobs carry labels `app.kubernetes.io/managed-by=grosh-ingestion`, `grosh.app/job-kind=reprocess`. Per-user reprocess jobs additionally carry `grosh.app/user-id={uuid}`; admin bulk reprocess jobs do not (the user list is in the env var instead).
  - [ ] The status endpoint authorization: per-user URL requires `caller.id == user_id OR caller.role == admin`; admin-bulk URL requires admin. A job that doesn't exist OR doesn't match the URL's scope returns 404 with `code: JOB_NOT_FOUND` (IDOR defense — never reveal existence outside scope).
  - [ ] Idempotent: if interrupted and re-triggered, produces the same result without duplication or data loss.
  - [ ] The bare-verb `POST /reprocess` endpoint at the ingestion service root (no `/v1/`, no `/users/` or `/admin/` scope) does not exist; all reprocess triggers go through the two endpoints above.

### 2.10 Cross-Cutting API Contract

Rules that apply to every endpoint in both the api and ingestion services. Each is a top-level invariant of the HTTP surface.

#### 2.10.1 Versioning

All HTTP endpoints across both services are mounted under a `/v1` prefix, **except** webhook receivers (URLs already registered with external systems like Monobank cannot be changed without re-registration). The webhook stays at `/monobank/webhook/{webhook_secret}` (per §2.2) for the lifetime of the v1 API.

- **Acceptance Criteria:**
  - [ ] Every route in `services/api` is reachable under `/v1/...`.
  - [ ] Every route in `services/ingestion` is reachable under `/v1/...` except the Monobank webhook.
  - [ ] OpenAPI documentation at `/docs` reflects the versioned URLs.
  - [ ] Versioning is implemented at the FastAPI `APIRouter` prefix level: **both** `services/api/main.py` and `services/ingestion/main.py` mount their routers via `app.include_router(router, prefix="/v1")`, not by hand-editing every route declaration. The webhook router in `services/ingestion` is the one router mounted without the `/v1` prefix; every other `include_router` call in either service uses it.
  - [ ] The Postman collection (`infra/grosh.postman_collection.json`) uses `/v1` prefixes for all non-webhook requests.

#### 2.10.2 Error Envelope (RFC 7807, Pragmatic)

All error responses across both services share one envelope based on RFC 7807 Problem Details. The content type stays `application/json` (not `application/problem+json`) to avoid breaking frontend code generators and TanStack Query defaults. Field-level validation errors preserve per-field detail under a dedicated key.

**Envelope shape (every non-2xx response):**

```json
{
  "type": "https://docs.grosh.app/errors/account-not-found",
  "title": "Account not found",
  "status": 404,
  "code": "ACCOUNT_NOT_FOUND",
  "detail": "Account 12345 does not exist or you don't have access to it.",
  "instance": "/v1/accounts/12345",
  "validation_errors": null
}
```

- `type`: stable URL identifying the error class (placeholder URL is fine for now; need not resolve to a page yet — frontend uses `code` for branching).
- `title`: short human-readable summary (English, frontend may localize via `code`).
- `status`: HTTP status code, duplicated in the body.
- `code`: stable machine-readable code from `grosh_shared.errors.ErrorCode` StrEnum.
- `detail`: human-readable explanation, never contains tokens or secrets.
- `instance`: the request path that produced the error.
- `validation_errors`: `null` for non-422 errors; on 422, list of `{loc: list[str|int], msg: str, type: str}` (Pydantic v2's native `.errors()` shape, with `loc` tuple→list converted for JSON compatibility).

**`ErrorCode` enumeration (complete, exhaustive — not "etc."):**

| Code                          | HTTP status | Domain                                        |
|-------------------------------|-------------|-----------------------------------------------|
| `VALIDATION_ERROR`            | 422         | Pydantic request validation failure           |
| `AUTHENTICATION_REQUIRED`     | 401         | Missing or invalid JWT                        |
| `INSUFFICIENT_PERMISSIONS`    | 403         | Authenticated but not authorized              |
| `ACCOUNT_NOT_FOUND`           | 404         | Account does not exist or not owned by caller |
| `INTEGRATION_NOT_FOUND`       | 404         | Monobank integration not found or not owned   |
| `USER_NOT_FOUND`              | 404         | Referenced user does not exist                |
| `JOB_NOT_FOUND`               | 404         | K8s Job not found in namespace                |
| `RATE_NOT_FOUND`              | 404         | Currency rate not found for query             |
| `INVALID_CURSOR`              | 400         | Pagination cursor malformed                   |
| `INVALID_DATE_RANGE`          | 422         | `from >= to` or window exceeds source limits  |
| `REPROCESS_LOCKED`            | 409         | A reprocess for this user is already running  |
| `BACKFILL_WINDOW_TOO_LARGE`   | 422         | Backfill window exceeds Monobank's 31-day cap |
| `MONOBANK_TOKEN_INVALID`      | 422         | Token rejected by Monobank during link        |
| `MONOBANK_API_UNAVAILABLE`    | 502         | Monobank API timed out or returned 5xx        |
| `INTEGRATION_ALREADY_LINKED`  | 409         | User already has an active integration for a different Monobank client_id |
| `JOB_STATUS_UNAVAILABLE`      | 503         | K8s API unreachable while fetching job status |
| `JOB_SUBMISSION_FAILED`       | 502         | K8s API unreachable while creating a new Job  |
| `INTERNAL_ERROR`              | 500         | Unhandled exception (last-resort catch-all)   |

This list is exhaustive for v1. Adding a new code requires a documentation update in this section. Code names are SCREAMING_SNAKE_CASE; never abbreviate.

- **Acceptance Criteria:**
  - [ ] `grosh_shared/errors.py` defines `ErrorCode(StrEnum)` containing exactly the codes above — no more, no fewer.
  - [ ] `grosh_shared` exposes a `ProblemDetail` Pydantic model and a `raise_problem(...)` helper that constructs and raises an `HTTPException` whose body conforms to the envelope.
  - [ ] Both `services/api` and `services/ingestion` register `error_handlers.py` overrides for: `HTTPException`, Pydantic `RequestValidationError`, and a catch-all 500 handler. Each emits the envelope.
  - [ ] On 422 validation errors, `validation_errors` contains the Pydantic v2 `.errors()` output serialized as `list[{loc: list[str|int], msg: str, type: str}]`. On every other error, `validation_errors` is `null`.
  - [ ] No domain error returns raw `{"detail": "..."}` — every raised error has a stable `code` from the enum above.
  - [ ] Existing code that calls `raise HTTPException(status_code=404, detail="...")` is migrated to `raise_problem(404, code=ErrorCode.ACCOUNT_NOT_FOUND, detail="...")` or equivalent.
  - [ ] Frontend never sees an error response without a `code` field.
  - [ ] Logged error context never includes credentials, refresh tokens, or Monobank tokens. An integration test asserts that calling `/v1/monobank/link` with a malformed token returns an error envelope whose `detail` does not contain the token substring.
  - [ ] Pydantic validators on sensitive fields (`monobank_token`, password) emit generic error messages — no user input is echoed in the validation error.

#### 2.10.3 Job Status Response Shape

Endpoints that trigger K8s Jobs return `JobTriggerResponse` (`job_id`, `status_url`) with HTTP 202 Accepted. The corresponding `GET` status endpoint returns `JobStatusResponse`:

```json
{
  "job_id": "grosh-backfill-acc12345-1716123456",
  "kind": "monobank_backfill",
  "status": "running",
  "started_at": "2026-05-20T12:00:00Z",
  "completed_at": null,
  "pods": { "active": 1, "succeeded": 0, "failed": 0 },
  "failure_reason": null
}
```

- `kind`: one of `monobank_backfill`, `rates_backfill`, `reprocess`. Must equal the Job's `grosh.app/job-kind` label value.
- `status`: one of `pending`, `running`, `succeeded`, `failed`. Derived from K8s Job `status.conditions` and pod counters.
- `pods`: counter snapshot from `status.active` / `status.succeeded` / `status.failed`.
- `failure_reason`: populated only when `status == "failed"`; sourced from `status.conditions[?type=='Failed'].message`. Null otherwise.

`status_url` is the **full relative path** the client polls via `GET` to retrieve job status. It is a concrete URL, not a template — the client uses it verbatim, never substituting placeholders.

**Authorization model — IDOR defense:**

Both "job does not exist" and "job exists but does not match the URL's scope" return **404 with `code: JOB_NOT_FOUND`**. This is the conventional IDOR defense — never reveal existence of resources outside the caller's scope. K8s Jobs created by trigger endpoints carry ownership labels (`grosh.app/user-id` and/or `grosh.app/account-id`); the status endpoint verifies these against the URL's scope before responding.

Job-status endpoints are not cached — every request makes a fresh K8s API call. At our scale this is acceptable; if it ever becomes a bottleneck, a short-lived (1-2s) in-memory cache keyed by `job_id` is a future improvement.

#### 2.10.4 OpenAPI Completeness

Every endpoint in both services exposes a fully-resolved response schema at `/docs`.

- **Acceptance Criteria:**
  - [ ] No endpoint shows `anyOf: [{}]`, `additionalProperties: true`-only, or empty `$ref` in the OpenAPI schema.
  - [ ] A CI check parses the live `/openapi.json` from both services and asserts every operation has a non-empty `responses[*].content[*].schema`.

### 2.11 Pre-Merge Hardening

A final tightening pass before merging this branch to `main`. Seven small items distilled from `uncommitted/pre-merge-003-action-items.md` and the eight non-ML assessments under `uncommitted/architecture-assessments/`. The high-priority architectural choices held up under independent review (Redpanda, two-stage consumer, delete+replay reprocess, staging+drain, RLS, four-service split, k3s+Compose hybrid). What remains is **tighten-the-current-design**: a defense-in-depth gap in RLS, a silently-failing observability hole in the staging drain, an OpenAPI contract that lies, a missing retention policy, and a reprocess endpoint that lets a misconfigured client spam K8s Jobs.

Each item is small in isolation. Bundled, they bring the branch to a state where merging to `main` does not leave known correctness/security gaps that would be expensive to retrofit once Phase 2 (classification) and Phase 4 (multi-user RLS surface area) land on top.

The intentional non-goals (deferred to later branches): `TransactionEnvelope.payload` discriminated unions, per-source DLQ topics, `producer.flush()` on the webhook critical path, `EXPLAIN` on the sweep query, BYPASSRLS tightening, Phase-4 family-aggregate RLS design, at-most-once offset docstring polish.

#### 2.11.1 RLS write protection on all user-scoped tables

Today every RLS policy on a user-scoped table is `USING (user_id = app.current_user_id())` only. `USING` filters reads and limits which rows an UPDATE/DELETE can target, but **does not** check the new row image on INSERT or UPDATE. A bug that writes `user_id = <wrong_uuid>` from inside an authenticated session passes RLS today. The `users` table already got per-command `WITH CHECK` policies in migration `0013` (and `revoked_tokens` is RBAC-only, no `user_id` column, no RLS needed); the remaining five user-scoped tables haven't.

- **Acceptance Criteria:**
  - [ ] A new migration adds `WITH CHECK (user_id = app.current_user_id())` to the existing RLS policies on `accounts`, `bank_integrations`, `transactions`, `user_settings`.
  - [ ] The `categories` policy retains its `OR user_id IS NULL` system-category branch in both `USING` and `WITH CHECK`. `WITH CHECK` form: `WITH CHECK (user_id = app.current_user_id() OR (user_id IS NULL AND app.current_user_role() = 'admin'))` — system categories can only be inserted by admin role. **Note:** this closes a pre-existing security gap — today the `USING (... OR user_id IS NULL)` policy combined with no `WITH CHECK` lets any authenticated user insert a row with `user_id = NULL`, polluting the global system-category namespace. The new `WITH CHECK` makes admin role mandatory for that branch.
  - [ ] Integration test: with `app.current_user_id` set to user A, an attempt to `INSERT ... user_id = <user_B_uuid>` into each of the five tables raises a Postgres RLS error (sqlstate `42501`).
  - [ ] Integration test: same setup, `UPDATE` of an existing row to change `user_id` to user B raises sqlstate `42501`.
  - [ ] Integration test (categories specifically): with `app.current_user_id` set to user A and `app.current_user_role` set to `'member'`, a `SELECT` of rows where `user_id IS NULL` (system categories) still succeeds — read access to system categories is preserved for non-admins. The same `INSERT ... user_id = NULL` as a member is rejected with sqlstate `42501`; as `'admin'` it succeeds.
  - [ ] Existing passing tests for legitimate writes (user A inserting their own rows) still pass — the new clause does not break the happy path.
  - [ ] The enrichment service (which runs under `grosh_consumer` with `BYPASSRLS`) continues to write `transactions` rows without RLS interference. Verified by the existing carve-out test in `services/runtime/tests/integration/test_single_writer_carveout.py`.

#### 2.11.2 CI invariant: every user-scoped table has RLS enabled and at least one policy

The most common production RLS failure is "we added table X, forgot the policy." There is no automated guard today. The "every table with a `user_id` column must have RLS" rule is convention, not invariant.

- **Acceptance Criteria:**
  - [ ] A pytest integration test queries `pg_class.relrowsecurity` and `pg_policies` after migrations run on a fresh DB.
  - [ ] The test enumerates every table in the `public` schema that has a `user_id` column.
  - [ ] For each such table, the test asserts `relrowsecurity = true` AND at least one policy exists in `pg_policies`.
  - [ ] The test produces a readable failure message that lists the offending table(s) and which condition failed.
  - [ ] An explicit allowlist of tables exempt from the rule exists in the test (currently empty; documented inline for future maintainers).
  - [ ] The test lives under `services/runtime/tests/integration/` so it runs against the same fresh-DB fixture used by other cross-service invariant tests (e.g. `test_single_writer_carveout.py`).
  - [ ] CI runs this test on every branch (it's already covered by the `pytest` step in the existing pipeline — no workflow changes needed).

#### 2.11.3 Tighten request schemas on manual entry endpoints

`CreateAccountRequest.type` is declared `str` but the handler rejects everything except `"cash"`. `CreateTransactionRequest.direction` is declared `TransactionDirection` (which advertises `income | expense | zero`) but the handler rejects `zero`. The OpenAPI schema is therefore a lie — the frontend SDK generator will produce a `Direction` type that includes a value the API never accepts, and `type: string` will be wide open in TypeScript.

- **Acceptance Criteria:**
  - [ ] `CreateAccountRequest.type` is narrowed from `str` to `Literal["cash"]`. The handler's explicit `if body.type != "cash"` check is removed because Pydantic now rejects non-`"cash"` values at parse time with a 422.
  - [ ] `CreateTransactionRequest.direction` is narrowed from `TransactionDirection` (full enum) to `Literal[TransactionDirection.income, TransactionDirection.expense]`, admitting only the two valid manual-entry values. The handler's explicit `if body.direction not in (income, expense)` check is removed.
  - [ ] The shared `TransactionDirection` enum keeps its `zero` member — it's used elsewhere in the pipeline for bank events (balance-only adjustments). The narrowing happens **only** at the manual-entry request schema, not on the shared enum.
  - [ ] OpenAPI snapshot: regenerated `infra/grosh.postman_collection.json` (if it carries schema fragments) and the OpenAPI completeness CI check (§2.10.4) both pass with the tightened types.
  - [ ] Existing integration tests for the happy path still pass. New tests assert that sending `type: "checking"` or `direction: "zero"` returns 422 with a Pydantic validation error (RFC-7807 envelope), not a custom handler-level error.
  - [ ] Test-migration step: any existing test that asserted on the **custom** handler-level error messages ("Only 'cash' account type is supported...", "direction must be 'income' or 'expense'.") is updated to assert on the Pydantic 422 shape — `validation_errors[*].loc == ["body", "type"]` and `validation_errors[*].type == "literal_error"` (or equivalent Pydantic v2 discriminator). The global `RequestValidationError` handler in `shared/src/grosh_shared/errors.py` already converts Pydantic errors into the RFC-7807 envelope, so the outer envelope shape is preserved — only the `detail` string and `validation_errors` payload change.

#### 2.11.4 pg_cron retention for `reprocessing_backups`

The `reprocessing_backups` table accumulates a snapshot of every reprocessed transaction set, keyed by `user_id` + `created_at`. Per the reprocess assessment, retention policy is **30 days**. There is no enforcement today.

- **Acceptance Criteria:**
  - [ ] A new migration registers a `pg_cron` job named `reprocessing_backups_cleanup`. The job runs **daily at 03:00 server time**, which equals 03:00 UTC iff the Postgres server runs with `TZ=UTC` (pg_cron schedules use the server's configured timezone, not an absolute offset). Same off-peak slot as `revoked_tokens_cleanup`.
  - [ ] **TZ guard: fail-fast, not silent warning.** The migration's `DO $$ ... $$` block opens with `IF current_setting('TimeZone') <> 'UTC' THEN RAISE EXCEPTION 'pg_cron job requires server TZ=UTC, got %', current_setting('TimeZone'); END IF;`. Rationale: a silent `RAISE WARNING` gets buried in deploy logs and goes unnoticed until backups grow to 200 days. A `RAISE EXCEPTION` fails the migration loudly, forcing the operator to either set `TZ=UTC` on the Postgres container or explicitly opt out by editing the migration. This is consistent with the project's "errors must be explicit and loud" rule.
  - [ ] The job deletes rows from `reprocessing_backups` where `created_at < now() - interval '30 days'`.
  - [ ] The migration follows the same shape as `0008_revoked_tokens.py`: `CREATE EXTENSION IF NOT EXISTS pg_cron` is idempotent; the job uses `cron.schedule_in_database`; the migration's `DO $$ ... EXCEPTION ... $$` block tolerates the test DB where `pg_cron` is not installed (logs a NOTICE and skips).
  - [ ] The migration's `downgrade()` removes the cron job via `cron.unschedule`.
  - [ ] `infra/docker-compose.yml`'s postgres service gains `TZ: UTC` in its `environment:` block as part of the same slice — without it, the TZ guard would fail every local-dev migration run.
  - [ ] Integration test: a row with `created_at = now() - interval '31 days'` is deleted after manual invocation of the cleanup SQL (the test invokes the DELETE directly because pytest can't wait for a scheduled cron tick). A row with `created_at = now() - interval '29 days'` survives.

#### 2.11.5 Reprocess endpoint rate-limit

Today the advisory lock in `reprocess_dispatcher.py` handles **correctness** (two concurrent requests don't both submit K8s Jobs — the second sees a `LOCK_HELD` 409). But a misconfigured frontend retry loop or a hostile authenticated user could cycle through "trigger → completes → trigger" repeatedly, churning K8s Jobs and disk for backup snapshots. The advisory lock won't stop this because it releases on Job completion.

This section supersedes the previous §2.9 "no cooldown" decision: rate-limit is now in scope as the second layer of defense (lock = correctness, rate-limit = retry-storm prevention).

**Storage and per-request flow:**

- **Acceptance Criteria:**
  - [ ] Both reprocess endpoints (`POST /v1/users/{user_id}/reprocess` and `POST /v1/admin/reprocess`) enforce a per-user rate-limit of **1 reprocess per hour per `user_id`**.
  - [ ] Storage: a new column `last_reprocess_started_at TIMESTAMPTZ NULL` on `users` (no separate rate-limit table — the cardinality is low and the existing `users` table is the natural owner).
  - [ ] **Atomic check-and-set.** The rate-limit check and timestamp update are implemented as a **single SQL statement** to eliminate the TOCTOU window: `UPDATE users SET last_reprocess_started_at = now() WHERE id = $1 AND (last_reprocess_started_at IS NULL OR last_reprocess_started_at < now() - interval '1 hour') RETURNING last_reprocess_started_at`. This runs at the start of the endpoint handler, inside the same transaction that subsequently acquires the advisory lock and submits the K8s Job. The row-count gates the next step: 0 rows ⇒ user is rate-limited ⇒ return 429; 1 row ⇒ proceed to advisory-lock acquisition.
  - [ ] **Rollback discipline.** The timestamp UPDATE, advisory-lock INSERT, and K8s Job submission are wrapped in one `async with conn.transaction():` block. The transaction commits only if all three succeed. If lock acquisition fails, or the K8s API call raises (`kubernetes.client.exceptions.ApiException`, timeout, connection error), the transaction rolls back and the timestamp update is reverted. The user is not falsely rate-limited by a failed attempt.
  - [ ] **Bounded transaction duration.** The K8s API call (`BatchV1Api.create_namespaced_job`) is given a socket-level timeout via the kubernetes client's `_request_timeout` kwarg, with a module-level constant `K8S_JOB_SUBMIT_TIMEOUT_SECONDS = 10`. On timeout, urllib3 aborts the HTTP request and raises `urllib3.exceptions.ReadTimeoutError` (a subclass of `urllib3.exceptions.TimeoutError` — the kubernetes client does NOT wrap urllib3 timeouts as `ApiException`; only `SSLError` gets wrapped). The dispatcher catches both `ApiException` AND `urllib3.exceptions.TimeoutError` and re-raises as `K8sDispatchError`; the router's transaction rolls back via the same path as a normal K8s failure. Without this cap, a hung K8s API call would hold row locks on `users` and `reprocessing_locks` for the OS TCP timeout (~2 min) or longer, blocking concurrent endpoints (e.g., `users.last_active_at` updates). *(`asyncio.wait_for(asyncio.to_thread(...))` is NOT used here — it cancels the awaitable but not the thread, leaking the K8s call into the background after rollback and risking orphan Jobs on retry.)*
  - [ ] **429 response shape.** When the rate-limit blocks the request, the endpoint returns **429 Too Many Requests** with code `RATE_LIMITED` in the RFC-7807 envelope. The `detail` field includes the time at which the user becomes eligible again, computed as `last_reprocess_started_at + 1 hour` (e.g. "Next reprocess allowed at 2026-05-22T14:23:00Z"). `RATE_LIMITED` is added to the `ErrorCode` enum in §2.10.2.

**Admin endpoint specifics:**

- **Acceptance Criteria:**
  - [ ] Admin-bulk path (`POST /v1/admin/reprocess`): the rate-limit applies per-target-user. If a bulk reprocess of 50 users finds that 3 of them are rate-limited, those 3 appear in the response's `skipped` array with `reason: "RATE_LIMITED"` (consistent with the existing `skipped` shape for already-locked users — `SkippedUser.reason` literal extends from `["REPROCESS_LOCKED"]` to `["REPROCESS_LOCKED", "RATE_LIMITED"]`). The other 47 proceed normally.
  - [ ] **`force: true` field.** A new boolean field `force: bool = False` is added to `BulkReprocessRequest`. When the **caller** is admin AND `force: true`, the per-hour check is bypassed: the SQL becomes `UPDATE users SET last_reprocess_started_at = now() WHERE id = $1 RETURNING ...` (no time-window predicate). The bypass applies per-target-user. The default and the user-level endpoint never bypass.
  - [ ] **`force: true` bypasses the check, not the update or the advisory lock.** The timestamp is still updated for each target user. Consequence: a non-force trigger within 1 hour of a force trigger is still rate-limited, but **subsequent force triggers are NOT rate-limited** by the per-user quota — admins passing `force: true` repeatedly will keep succeeding, subject only to the advisory lock (one Job at a time per user) and the per-call K8s submit cost.
  - [ ] **This is intentional.** The spec treats `force: true` as a **manual override for admin discretion** ("I know what I'm doing, run it now"), not as a guaranteed-rate-limited path. Restricting force-call rate is left to operational discipline (admin role is held by ~1 person — the system owner — and abuse would be self-inflicted). If admin-rate restriction ever becomes necessary (e.g., multiple admin operators), it should be a separate per-caller rate-limit on the admin endpoint, not a coupling between `force: true` and the per-user quota.

**User-level endpoint specifics:**

- **Acceptance Criteria:**
  - [ ] The user-level endpoint (`POST /v1/users/{user_id}/reprocess`) has no `force` field. Admins hitting the user-level endpoint follow the same rate-limit as the target user (they should use the admin endpoint with `force: true` for the escape hatch).

**Tests:**

- **Acceptance Criteria:**
  - [ ] Integration test: 1st trigger succeeds (202), 2nd within an hour returns 429 with `RATE_LIMITED`. After advancing `last_reprocess_started_at` to >1h ago, a 3rd trigger succeeds.
  - [ ] Integration test: admin calls `POST /v1/admin/reprocess` with `{"user_ids": [<user_A>], "force": true}` when user A's `last_reprocess_started_at` is 5 minutes ago — succeeds, and user A's timestamp is updated to now. A subsequent non-force call within 1 hour returns 429 / appears in `skipped` array.
  - [ ] Integration test: trigger reprocess, mock the K8s API to raise `ApiException`, assert the transaction rolls back — `users.last_reprocess_started_at` is unchanged from before the attempt.
  - [ ] Integration test: trigger reprocess, mock `_batch_api.create_namespaced_job` to raise `urllib3.exceptions.ReadTimeoutError(...)` (the actual exception urllib3 raises on socket timeout — the kubernetes client does NOT wrap it as `ApiException`); assert the dispatcher catches it as `urllib3.exceptions.TimeoutError` and raises `K8sDispatchError`, the transaction rolls back, and `last_reprocess_started_at` is unchanged. (We test the *handling* of the timeout, not whether urllib3 actually triggers it — that's library behavior.)

#### 2.11.6 Staging-drain age observability

The staging-drain sweep loop is the only producer of `normalized_transactions` for transactions whose user was locked during reprocess. If the drain stalls — bug, deadlock, exception silently caught, dead consumer — staged rows accumulate forever and the user's transactions never reach the pipeline. There is no symptom in logs today.

- **Acceptance Criteria:**
  - [ ] The `StagingDrainService` sweep loop logs, on every iteration, the age of the oldest staged row: `staging_drain.oldest_staged_age_seconds=<int> staging_drain.staged_row_count=<int>`.
  - [ ] The age is computed by a new `StagingRepo` method returning `(age_seconds, row_count)` from one query; `age_seconds` is `None` if the table is empty.
  - [ ] If the age exceeds **300 seconds (5 minutes)**, the log line is emitted at WARN level. Otherwise DEBUG.
  - [ ] When the table is empty, the log line is `staging_drain.staged_row_count=0` at DEBUG level (no age field). The sweep is healthy when there's nothing to do — that's not a warning.
  - [ ] The threshold (300s) is a module-level constant, not a magic number scattered through the code.
  - [ ] Integration test: insert a staged row with `created_at = now() - interval '6 minutes'`, run one sweep iteration, assert the log capture contains a WARN-level record with the `oldest_staged_age_seconds` field ≥ 360.

#### 2.11.7 Document the RLS defense-in-depth convention

The "Row-Level Security" subsection in CLAUDE.md states that user-scoped tables enforce RLS and that user-facing services set `app.current_user_id` at the start of every transaction. It does not state that **`WHERE user_id = $1` in repository code is still mandatory** — RLS is defense in depth, not the primary filter.

The risk: a future repository author trusts RLS as the primary filter and writes `SELECT * FROM accounts WHERE id = $1` without the `AND user_id = $1`. The query plan degrades (no index hit on user_id, scan + RLS filter at the end), correctness is fine but performance silently regresses. Worse, if RLS is ever disabled for debugging, the query becomes cross-user.

- **Acceptance Criteria:**
  - [ ] The "Row-Level Security" subsection under the `## Architectural Invariants` heading in `CLAUDE.md` (project-level, not the user-global one) gains one new paragraph immediately after the existing "All user-scoped tables enforce RLS..." sentence. The paragraph reads approximately: "Repository methods on user-scoped tables MUST include `WHERE user_id = $1` (with `user_id` passed from the service layer) as part of the primary WHERE clause. RLS is the safety net, not the primary isolation mechanism. Two reasons: (1) **query plan stability** — indexes on user-scoped tables are `(user_id, ...)`-prefixed, and an explicit `WHERE user_id = $1` keeps the planner hitting the index instead of scan-then-RLS-filter; (2) **survivability under temporary RLS disablement** — if RLS is disabled for ad-hoc debugging or a migration carve-out, the query must still scope correctly to one user without the policy."
  - [ ] No code changes — this is a documentation-only acceptance criterion.

#### 2.11.8 Integration test coverage for high-risk untested endpoints

A pre-merge integration-test coverage audit (run after Slice 36) found that four production endpoints in the API service and one service-layer utility in the ingestion service exist with no direct integration test: `GET /v1/settings`, `PUT /v1/settings`, `GET /v1/rates`, `GET /v1/rates/at`, and `MonobankLinkingService.reregister_webhooks`. The Monobank webhook receiver (`POST /monobank/webhook/{secret}` — unversioned by design per §2.10.1) is exercised only indirectly through `test_monobank_lifecycle.py`, which never POSTs a real `MonobankWebhookPayload` to the endpoint or inspects the resulting Kafka producer call.

The risk these gaps create: each of these code paths handles either money (currency conversion via `default_rate_source`, SCD2 rate windows used by aggregates), authentication-coupled webhook delivery (a wrong producer call shape would silently drop transactions), or operator recovery (`reregister_webhooks` is the post-domain-change rescue path). A refactor mistake in any of them would not be caught by mypy/ruff and would not be caught by any existing integration test — it would only surface in production or during manual smoke. The hardening goal of spec 003 includes closing these gaps before the merge.

- **Acceptance Criteria:**
  - [ ] `services/api/tests/integration/test_settings.py` exists and contains at least one test for `GET /v1/settings` (asserting per-user isolation across two users) and at least one for `PUT /v1/settings` (asserting both the happy-path upsert and 422 validation rejections on unknown rate source + invalid timezone).
  - [ ] `services/api/tests/integration/test_rates_endpoints.py` exists and contains at least one test for `GET /v1/rates` (covering filters, cursor pagination, and the half-open `[from, to)` boundary) and at least one for `GET /v1/rates/at` (covering the SCD2 `valid_from <= at AND (valid_to IS NULL OR valid_to > at)` semantics, including the boundary case where `valid_to == at` does NOT match).
  - [ ] `services/ingestion/tests/integration/test_monobank_webhook.py` exists and exercises `POST /monobank/webhook/{secret}` (unversioned route per §2.10.1) with: (a) a happy-path payload that produces exactly one `producer.produce(topic=Topic.raw_transactions_monobank, ...)` call with the correct `TransactionEnvelope` shape, (b) a 404 path for unknown webhook_secret, (c) a 404 path for unknown account.
  - [ ] `services/ingestion/tests/integration/test_monobank_reregister.py` exists and exercises `MonobankLinkingService.reregister_webhooks` with: (a) a happy-path test asserting the DB config is updated with a new webhook_secret/url AND the stubbed `MonobankClient.set_webhook` is called exactly once with the new URL, (b) a failure-path test where `set_webhook` raises and the test asserts the DB config was *still* updated (the "DB before network" ordering invariant), (c) a multi-integration test asserting non-Monobank integrations are skipped, (d) a token-decrypt-failure test asserting the result entry reports `status='error'` without calling the client.
  - [ ] All new tests pass under `uv run pytest services/api -q` and `uv run pytest services/ingestion -q`.
  - [ ] Zero production-code changes — `git diff` on `services/api/src/` and `services/ingestion/src/` shows no modifications. This is a test-only hardening slice.

---

## 3. Scope and Boundaries

### In-Scope

- Monobank account linking (self-service via Settings page)
- Monobank webhook receiver (GET verification + POST transaction ingestion)
- Monobank historical backfill with manual trigger and progress indication
- Two-service consumer pipeline: normalization service (owns every producer of `normalized_transactions` — steady-state normalization, staging drain, reprocess job) and enrichment service (pure consumer of `normalized_transactions` running transfer detection → currency conversion → classification → persistence)
- Per-source transfer detection strategy registry. Monobank uses the v2 7-step algorithm (universal candidate fetch + IBAN/description evidence ranking + count-and-decide). Other banks plug in their own strategies without modifying generic code.
- Transfer match anomaly recording and auto-resolution
- Manual cash account creation and manual transaction entry (through Redpanda)
- Tri-currency storage (UAH + USD + EUR equivalents) on every transaction, denormalized at write time
- Currency rate ingestion from Monobank and NBU with configurable polling cadence
- Rate fallback chain (monobank → nbu) with stale rate detection via `last_polled_at`
- Admin-triggered historical rate backfill from NBU via K8s Job
- Rate source traceability in transaction metadata (per-step path with tier, side, operation)
- Compute-on-read aggregation API (any bucket, any currency, any date range, timezone-aware)
- REST API endpoints for transactions, aggregates, accounts, rates, and admin users — all under `/v1/` prefix, all returning typed response models
- Monobank integration lifecycle CRUD: list / link (idempotent on `monobank_client_id`) / delete
- K8s Job status endpoints (per-account backfill, rates backfill, per-user reprocess, admin bulk reprocess) returning native K8s status (no application-level progress in v1)
- Transaction reprocessing (per-user self-service + admin bulk via K8s Job) with race-free lock ownership (API inserts the lock atomically before submitting the Job; pod asserts the lock at startup)
- Cross-cutting API contract: `/v1` versioning, RFC 7807 error envelope with stable `ErrorCode` enum, OpenAPI completeness CI check
- `users.last_active_at` column (set on access-token issuance) and admin user list endpoint
- Per-service database roles with write-privilege separation
- Access token revocation for instant logout
- Encrypted Monobank token storage
- **Pre-merge hardening (§2.11):**
  - RLS `WITH CHECK` on `accounts`, `bank_integrations`, `categories`, `transactions`, `user_settings`
  - pytest CI invariant: every `public.*` table with a `user_id` column has RLS + ≥1 policy
  - Narrow `CreateAccountRequest.type` to `Literal["cash"]` and `CreateTransactionRequest.direction` to `Literal["income", "expense"]`
  - `pg_cron` daily cleanup job for `reprocessing_backups` (30-day retention) + `TZ: UTC` on the dev Postgres container
  - Per-user 1-per-hour rate-limit on `POST /v1/users/{user_id}/reprocess` and `POST /v1/admin/reprocess`, implemented as a single atomic UPDATE-RETURNING on `users.last_reprocess_started_at`
  - New `force: bool = false` field on `BulkReprocessRequest` (admin endpoint only) that bypasses the rate-limit check but still consumes the user's quota
  - `last_reprocess_started_at TIMESTAMPTZ NULL` column on `users` (storage for the rate-limit)
  - WARN-level log line from the staging-drain sweep when oldest staged row age > 5 min
  - Documenting the "RLS is defense in depth" convention in CLAUDE.md

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
- **Pre-merge hardening (§2.11) deferred items:**
  - `TransactionEnvelope.payload` discriminated union by source — defer to first PUMB adapter slice
  - Per-source DLQ topic on `raw_transactions` — defer to first PUMB adapter slice
  - `producer.flush()` on the ingestion webhook critical path — defer (nice-to-have, not blocker)
  - `EXPLAIN` on `select_unlocked_user_ids_with_staged_rows` — sanity check, defer
  - Tightening `grosh_consumer BYPASSRLS` to per-message `set_config` — defer until observability lands
  - Phase-4 family-aggregate RLS policy design sketch — separate spec, drafted closer to Phase 4
  - At-most-once offset semantic on the normalization consumer — docstring polish, defer
  - Grafana/Prometheus wiring for the staging-drain age metric — observability phase; §2.11.6 only adds the log line
  - Backfill of historical `last_reprocess_started_at` values — column is nullable; existing users get NULL and can trigger immediately on first use after deploy
