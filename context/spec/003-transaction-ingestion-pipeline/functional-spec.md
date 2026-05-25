# Functional Specification: Transaction Ingestion Pipeline

- **Roadmap Item:** Transaction Ingestion Pipeline (Phase 1)
- **Author:** Nick

---

## 1. Overview and Rationale

The spreadsheet workflow requires manual entry of every transaction — easy to forget, painful to backfill, impossible to forecast from. The ingestion pipeline replaces this by automatically pulling transactions from Monobank in real time (webhook) and on demand (historical backfill), routing them through a two-stage streaming pipeline (normalization → enrichment), and storing them in PostgreSQL.

Users also need to record cash transactions that don't flow through any bank. All transaction sources — automated and manual — converge into the same Redpanda-based pipeline so downstream consumers and future features have a single, consistent data source.

A key challenge is the primary user's FOP (sole proprietor) account structure: salary arrives on a USD FOP account, moves to a UAH FOP account (taxes paid there), then gets transferred in small batches to a UAH credit card for spending. Without transfer detection, these internal movements pollute income/expense numbers. The pipeline must distinguish real income/expense from internal transfers from the start. For Monobank, this uses a deterministic 7-step algorithm (see `references/adr-transfer-detection.md`) that fetches a single universal candidate set, ranks pairs by IBAN evidence + description evidence, and decides with a count-and-decide rule that handles both IBAN-visible transfers and card-to-card movements with no API-level correlation. Other banks register their own strategy; the orchestrator dispatches per source.

**Success criteria:**

- All Monobank transactions arrive automatically via webhook — zero manual entry for bank transactions.
- Historical transactions importable via backfill on first setup; re-running backfill produces no duplicates.
- Internal transfers between user's own accounts are correctly detected and tagged — including card-to-card transfers that have no counterparty IBAN in the API response.
- Cash transactions recordable manually through the same pipeline.
- REST API endpoints expose paginated transactions, flexible time-window aggregates (any bucket size, any currency, any date range), and account listings.
- Aggregations are computed on read from a single SQL query — no materialized views, no staleness after backfill or reclassification. Cross-currency totals (UAH, USD, EUR) powered by per-bank exchange rates stored in an SCD Type 2 table.
- Exchange rates are polled from Monobank (every 5 minutes) and NBU (daily). Stale or missing rates fall back through a configurable chain. Historical rates are backfillable from NBU for any past date range.
- Users can trigger reprocessing of their own transactions to benefit from pipeline improvements (new transfer detection logic, rate backfills, future classification) without re-fetching from bank APIs.
- Every RLS-protected user-scoped table rejects writes (INSERT/UPDATE) where `user_id ≠ app.current_user_id()`. RLS is a write barrier, not just a read filter. A CI test fails on any future user-scoped table added without RLS enabled and at least one policy.
- The OpenAPI schema reflects what the handlers actually accept — no `direction = "zero"` admitted at parse time on manual entry, no arbitrary `type` string accepted on cash accounts.

---

## 2. Functional Requirements

### 2.1 Monobank Account Linking

Each user can link, list, and unlink their Monobank account from a Settings page. The lifecycle has full CRUD-shaped endpoints (list / link / delete); `link` is idempotent on the Monobank `clientId` so re-linking the same Monobank user rebinds rather than duplicates.

**Endpoints (all under `/v1/`, per §2.10):**

- `GET /v1/monobank/integrations` — list the current user's Monobank integrations.
- `POST /v1/monobank/link` — link a new integration OR rotate the token on an existing one (idempotent).
- `DELETE /v1/monobank/integrations/{integration_id}` — hard delete the integration record.

There is no separate `relink` endpoint — token rotation is subsumed by idempotent `link`.

- **Acceptance Criteria:**
  - [x] User pastes their Monobank personal API token on the Settings page.
  - [x] Grosh calls Monobank `/personal/client-info`, fetches all accounts (cards), and displays them to the user for confirmation. On Monobank API timeout, 5xx, or 429: return 502 with `code: MONOBANK_API_UNAVAILABLE`. On 401/403 (token rejected): return 422 with `code: MONOBANK_TOKEN_INVALID`. No DB rows are created before client-info validation succeeds.
  - [x] On confirmation, Grosh registers the webhook URL with Monobank (`PATCH /personal/webhook`).
  - [x] Account records are created in the database with account type, currency, and cashback type from Monobank's response.
  - [x] The Monobank token is stored encrypted (pgcrypto) — never in plaintext. The Monobank `clientId` is persisted on `bank_integrations.config` as `{"monobank_client_id": "..."}` so future `link` calls can match for idempotency.
  - [x] After successful linking, the user is prompted: "Import recent transactions?" with a button to trigger backfill.
  - [x] `POST /v1/monobank/link` is idempotent on `monobank_client_id`:
    - **No existing integration for the user:** insert a fresh row. If accounts with matching `(source='monobank', external_account_id)` exist (left over from a prior hard-deleted integration of the *same* Monobank user), they are **rebound** to the new integration row — historical transaction continuity preserved. Match by `external_account_id` AND `monobank_client_id`; accounts from a previously-deleted *different* Monobank user are never rebound.
    - **Existing integration with the same `monobank_client_id`:** rotate the encrypted token in-place; no new accounts created. Returns 200 OK.
    - **Existing integration with a *different* `monobank_client_id`:** return 409 with `code: INTEGRATION_ALREADY_LINKED`. The user must DELETE the existing integration before linking a different Monobank account — prevents accidental rebinding of one user's account history to another Monobank account.
  - [x] `POST /v1/monobank/link` returns 201 Created when a fresh `bank_integrations` row is inserted; 200 OK when only the token is rotated. The HTTP status reflects DB-row-level state, not webhook-registration outcome.
  - [x] If webhook registration with Monobank fails (network error, 5xx, timeout), the integration row remains in DB but the webhook is inert. The response's `webhook_registered: false` field signals this to the frontend. The frontend treats this as a non-blocking warning, not an error: shows a banner ("Live transaction sync is paused — retry connection") with a manual-retry button that re-POSTs `/v1/monobank/link` (idempotent rebind). The frontend does not auto-retry. The platform does not retry in the background.
  - [x] `POST /v1/monobank/link` returns a typed `MonobankLinkResponse` (no bare dict). Shape: `integration_id`, `is_new` (bool — whether a fresh row was inserted), `webhook_registered` (bool), `accounts: [{account_id, external_account_id, currency_code, was_rebound}]`. `was_rebound` is `false` for accounts created in this call, `true` for accounts that already existed and were rebound from a prior deleted integration.
  - [x] `GET /v1/monobank/integrations` returns the current user's Monobank integrations as `list[MonobankIntegrationResponse]`. A flat list — cursor pagination is unnecessary (a user typically has 0 or 1 integration; family scale will never exceed single digits).
  - [x] `DELETE /v1/monobank/integrations/{integration_id}` hard-deletes the `bank_integrations` row. The integration's `user_id` must equal the calling user (or the caller must be admin); otherwise return 404 with `code: INTEGRATION_NOT_FOUND` (do not reveal existence of integrations owned by other users).
  - [x] On DELETE, accounts and historical transactions remain in the DB untouched. They become inert — no new webhook events arrive. A subsequent `POST /v1/monobank/link` with the same Monobank user rebinds them per the idempotency contract above.
  - [x] On DELETE, Monobank webhook de-registration is best-effort with a 10-second timeout. The local delete proceeds regardless. If Monobank's API call fails, a WARN is logged and the orphan webhook remains on Monobank's side; orphan delivery attempts hit our endpoint with a now-unknown `webhook_secret` and are rejected with 404 — acceptable log noise, not a data integrity issue.
  - [x] Admin can re-register webhooks for all active integrations after a domain change via `make dev-reregister-webhooks` (local) or `scripts/reregister-webhooks/prod.sh` (production). Source-agnostic: each bank implements `WebhookReregistrationProvider`, dispatched via registry.
  - [x] Integration test exercises continuity: link as `client_id` A → backfill → DELETE → re-link as `client_id` A → confirm new transactions land on the original `account_id` rows.
  - [x] Integration test exercises isolation: link as `client_id` A → DELETE → link as `client_id` B → confirm B's accounts are fresh and A's accounts remain bound to the deleted integration's row (not rebound to B).

### 2.2 Monobank Webhook Receiver

FastAPI endpoint receives real-time transaction pushes from Monobank.

- **Acceptance Criteria:**
  - [x] `GET /monobank/webhook/{webhook_secret}` responds with `200 OK` (Monobank's verification request). The path is **unversioned by design** — the URL is registered with Monobank and cannot be changed without re-registering every active integration's webhook. All other endpoints are versioned under `/v1/` (per §2.10); webhook receivers are the documented exception.
  - [x] `POST /monobank/webhook/{webhook_secret}` accepts Monobank's `StatementItem` payload.
  - [x] The endpoint validates that the account ID in the payload matches a registered account in Grosh.
  - [x] Valid transactions are published to the Redpanda `raw_transactions.monobank` topic as raw bank payloads, partitioned by `user_id`.
  - [x] The endpoint responds within Monobank's timeout — publishing to Redpanda is fast; no synchronous processing.
  - [x] Monobank does not provide webhook payload signatures. Authentication relies on the opaque webhook secret embedded in the URL path (`/monobank/webhook/{secret}`), validated against the DB. The secret is generated per-integration and is unguessable (UUID4). This is the only verification mechanism beyond the initial GET handshake.

### 2.3 Historical Backfill

Users can import historical transactions from Monobank. Backfill runs as a K8s Job; the frontend polls a status endpoint for progress.

**Endpoints:**

- `POST /v1/monobank/accounts/{account_id}/backfill` — trigger backfill for a single account; returns 202 with `JobTriggerResponse`.
- `GET /v1/monobank/accounts/{account_id}/backfill/{job_id}` — poll job status; returns `JobStatusResponse` (per §2.10).

- **Acceptance Criteria:**
  - [x] A "Backfill" button is available on the Settings page per linked Monobank account.
  - [x] `POST /v1/monobank/accounts/{account_id}/backfill` accepts query parameters `from` (date, inclusive) and `to` (date, **exclusive** — half-open `[from, to)`, per CLAUDE.md). Validation:
    - `from < to` — otherwise 422 with `code: INVALID_DATE_RANGE`.
    - `(to - from).days <= 31` — otherwise 422 with `code: BACKFILL_WINDOW_TOO_LARGE` and a `detail` naming the requested span vs. the 31-day Monobank limit. The 31-day cap is the per-request Monobank constraint; the job pod is responsible for splitting valid windows into chunks per the rate limit (1 request per 60s), but the API rejects any single trigger that asks for more than 31 days upfront.
    - `to` is treated as the start of the named day (00:00:00 UTC) — exclusive. An integration test exercises `from=2025-01-01&to=2025-02-01` (exactly 31 days, half-open) accepted and `from=2025-01-01&to=2025-02-02` (32 days) rejected.
  - [x] On success, returns 202 Accepted with `JobTriggerResponse` (`job_id` + `status_url`). The `status_url` is the relative path the frontend uses verbatim — never construct it by string concatenation.
  - [x] The job paginates through Monobank's statement API (max 31 days per request, 1 request per 60 seconds rate limit per account).
  - [x] Each batch of transactions is published to the same per-source Redpanda topic (`raw_transactions.monobank`).
  - [x] The user sees progress by polling `GET /v1/monobank/accounts/{account_id}/backfill/{job_id}`. The response carries native K8s status (pending/running/succeeded/failed) plus a `failure_reason` when failed. Application-level progress (e.g. "47/120 batches imported") is deferred — Phase 2 enhancement.
  - [x] Backfill is safe to re-run — the consumer deduplicates by Monobank transaction ID.
  - [x] Backfill covers the maximum available history (up to 31 days per request, paginating backward).
  - [x] The K8s Job carries labels `app.kubernetes.io/managed-by=grosh-ingestion`, `grosh.app/job-kind=monobank_backfill`, `grosh.app/account-id={uuid}`, and `grosh.app/user-id={uuid}`. Backfill is inherently per-account-and-per-user-scoped (an account belongs to exactly one user via the `accounts.user_id` FK), so both labels are always present. The status endpoint verifies both `account_id` (URL scope) and `user_id` (caller scope) against the labels before returning.
  - [x] Authorization: the account must be owned by the caller (or caller is admin). Otherwise 404 with `code: ACCOUNT_NOT_FOUND` (don't reveal existence). A job_id that doesn't match the URL's account scope returns 404 with `code: JOB_NOT_FOUND`.
  - [x] If the K8s API is unreachable when the status endpoint is called, return 503 with `code: JOB_STATUS_UNAVAILABLE`. Frontend should retry with exponential backoff (1s, 2s, 4s, 8s, capped at 60s); after 5 consecutive 503s, surface "Job status temporarily unavailable" and stop polling.
  - [x] If the K8s API is unreachable when the trigger endpoint is called (Job cannot be submitted), return 502 with `code: JOB_SUBMISSION_FAILED`. The user may immediately retry.
  - [x] Completed Jobs are kept by K8s for at least 3600 seconds (`ttlSecondsAfterFinished >= 3600`) so a polling client can capture the terminal status. After GC, status endpoint returns 404 with `code: JOB_NOT_FOUND`.
  - [x] If Monobank returns 401/403 mid-backfill (user revoked the token at Monobank's end), the backfill pod transitions the integration to `bank_integrations.status='error'` before re-raising. The Job ends in `failed` state with a `failure_reason` naming token rejection. The webhook receiver's RLS policy (`config->>'webhook_secret' AND status='active'`) automatically stops routing inbound webhooks to error-state integrations. The user must re-link via `POST /v1/monobank/link` before further activity resumes.

### 2.4 Transaction Consumer (Normalization + Enrichment Services)

The consumer runs as **two independently-deployable services** connected by an intermediate Redpanda topic (`normalized_transactions`). Each service is independently restartable and observable; their failure domains are isolated.

**Normalization service** (`grosh-normalization`) owns every producer of `normalized_transactions`. Three internal producers run in one process plus one K8s Job entrypoint:

- Steady-state normalization — subscribes to per-source raw topics (`raw_transactions.monobank`, `raw_transactions.manual`, etc.), dispatches to per-source normalization strategies, publishes `NormalizedTransaction`.
- Staging drain — periodically sweeps `staging_normalized_transactions` rows whose `user_id` no longer holds a reprocessing lock and republishes them to `normalized_transactions`.
- Reprocess job (K8s Job) — reads stored `transactions` rows, reconstructs `NormalizedTransaction` via the inverse mapping, deletes originals, republishes, and polls for catchup. Runs as a separate pod from the same image with a distinct entrypoint.

**Enrichment service** (`grosh-enrichment`) is a pure consumer of `normalized_transactions`. Runs transfer detection → currency conversion → classification → persistence. Knows nothing about reprocess, staging, or normalization.

**Cohesion rule:** "Which service owns each module?" is answered by "who produces events on `normalized_transactions`?" — normalization owns every producer (steady-state, staging drain, reprocess job); enrichment owns the single consumer plus the downstream enrichment layers. The reprocess job belongs to the normalization service because it republishes events to the topic; it does not invoke enrichment layers itself. The enrichment service picks the republished events up through its normal consumer path. Full architectural rationale, image-and-deployment strategy (one image, three entrypoints — `python -m grosh_normalization.main`, `python -m grosh_enrichment.main`, `python -m grosh_normalization.reprocess_main`), and the single-writer carve-outs the split introduces all live in `references/adr-consumer-pipeline-architecture.md`.

- **Acceptance Criteria:**
  - [x] Consumer deduplicates by transaction source ID (Monobank ID or manual entry ID) — duplicate publishes are silently dropped via `ON CONFLICT (id) DO NOTHING`.
  - [x] Consumer writes transactions to the `transactions` table (regular PostgreSQL table, PK on `id`) with all fields: user_id, account_id, time, `amount_cents` (account currency), `operation_amount_cents` (original currency), `currency_code`, `description`, `mcc`, `cashback_amount_cents`, `balance_cents`, hold status. **All monetary amounts in this spec and in the database are integers in the minor currency unit (cents — 1/100 of the major unit).** Float storage is never used for amounts.
  - [x] All three display-currency amounts (`amount_uah_cents`, `amount_usd_cents`, `amount_eur_cents`) are denormalized at write time using per-bank exchange rates from the `currency_rates` SCD2 table.
  - [x] Consumer detects internal transfers using a per-source strategy. Monobank uses the 7-step algorithm (see §2.4.1). Other banks register their own; the orchestrator dispatches via registry — no `if source == "monobank"` branches in generic code.
  - [x] Each transaction has two orthogonal classification axes: `direction` (immutable money flow: `income`, `expense`, `zero`) set by the normalizer from the amount sign, and `special_category` (pipeline enrichment: NULL for ordinary transactions, `transfer` for internal movements, future values: `cancellation`, `hold`). These are independent — a transfer leg is still directionally `income` or `expense`. Aggregation queries filter on `special_category IS NULL` to exclude non-ordinary transactions.
  - [x] Consumer processes messages from all sources (webhook, backfill, manual) identically — bank-specific logic is confined to normalization strategies and transfer detection strategies.
  - [x] The `hold` flag is stored as-is from the bank but not used for filtering. Monobank's historical API returns unreliable hold values — the flag reflects the internal processing pipeline, not settlement status.

#### 2.4.1 Transfer Detection (Monobank — 7-Step Algorithm)

Internal transfers between the user's own accounts are detected deterministically. Monobank does not provide a transfer correlation ID — particularly for card-to-card transfers, both legs look identical to external P2P in the raw API response. The Monobank strategy implements a 7-step algorithm (full design in `references/adr-transfer-detection.md`):

1. **MCC + idempotency gate** — only `mcc='4829'` rows enter; already-stored IDs no-op.
2. **Row flags** — derive `description_matched`, `multi_hop_description`, and `cp_iban_status` (`null` / `transitive` / `unlinked` / `honest`).
3a. **Unlinked-partner short-circuit** — if `cp_iban_status='unlinked'`, the partner is on an account the user hasn't linked. Skip the universal fetch; emit a row block; record an unpaired anomaly only if the description looks like a transfer but isn't in the known phrase set.
3. **Universal candidate fetch** — one `SELECT ... FOR UPDATE SKIP LOCKED` retrieves all unclaimed opposite-direction MCC 4829 rows on different accounts within the ±2s window matching the amount predicate.
4. **Hard IBAN consistency filter** — drops candidates whose IBAN evidence contradicts the incoming row (e.g., honest cp_iban points at a third party).
5. **Per-pair evidence classification** — each surviving candidate is labelled with the IBAN evidence class (`bilateral` / `unilateral` / `none`).
6. **Decide (count-and-decide + bucket-locked)** — bilateral evidence beats unilateral beats none; within the winning bucket, exactly one candidate → claim; more → ambiguous anomaly.
7. **Translate Decision → result** — `claim` produces a paired write + auto-resolve sweep of any prior `unpaired_*` anomaly on either leg; `anomaly` records a row; `skip` returns clean.

- **Acceptance Criteria:**
  - [x] Single universal fetch — no per-tier queries.
  - [x] Claim lock: paired transactions are linked via `related_transaction_id` (self-FK) and `special_category` set to `'transfer'`. The `direction` field is preserved (`income`/`expense`) — a transfer leg remains directional. The claim query uses `FOR UPDATE SKIP LOCKED` to prevent concurrent double-claims.
  - [x] Both legs carry a byte-equal `metadata.layer.transfer.pair` sub-block (under the wrapped-metadata convention from `references/adr-transaction-reprocessing.md`).
  - [x] Bucket-locked decision: bilateral IBAN evidence outranks unilateral outranks none. Within the winning bucket, exactly 1 candidate → claim; 0 → skip; >1 → record `ambiguous_pair_match` anomaly (never guess).
  - [x] Unpaired transfer-like descriptions (`З ...` / `На ...` family) that remain unmatched → record `unpaired_from_description` or `unpaired_to_description` anomaly for visibility.
  - [x] Anomalies auto-resolve: when a partner arrives later and the pair is successfully claimed, any `unpaired_*` anomaly on either leg is deleted in the same orchestrator transaction.
  - [x] Multi-hop FOP↔FOP transfers (chain-end IBAN quirk) correctly pair end-to-end.
  - [x] External P2P (person names, salary `Provectus IT, Inc`, tax authorities `ГУ ДПС`/`ГУК`, masked card numbers) correctly excluded — no false positives.
  - [x] **Card-to-card definition (Monobank).** A "card-to-card transfer" is an MCC 4829 row with `counterparty_iban IS NULL` (Monobank does not expose the partner IBAN for direct card transfers) whose description matches the `З %` / `На %` transfer phrase family. The universal fetch pairs both legs by `operation_amount_cents` cross-match within ±2s, gated by description evidence. External P2P lacks the transfer phrase and is therefore not pairable by the description-evidence path.
  - [x] Anomaly enum: `unpaired_from_description`, `unpaired_to_description`, `ambiguous_pair_match`, `description_account_mismatch`, `description_consistency_mismatch`.
  - [x] Source-agnostic dispatch: the transfer detection strategy is registered per-source in a `dict[str, TransferDetectionStrategy]`. Generic pipeline code looks up by `source` and calls the protocol method — no `if source == "monobank"` branches.

### 2.5 Manual Entry

Users can log cash transactions not captured by any bank.

**Endpoints (all under `/v1/`, per §2.10):**

- `POST /v1/manual/accounts` — create a manual account (201 Created, returns `ManualAccountResponse`).
- `PUT /v1/manual/accounts/{account_id}` — rename/edit a manual account (200 OK, returns `ManualAccountResponse`).
- `POST /v1/manual/transactions` — create a manual transaction (201 Created, returns `ManualTransactionResponse`).
- `DELETE /v1/manual/accounts/{account_id}` — soft-delete a manual account (204 No Content or 404).

- **Acceptance Criteria:**
  - [x] User can create a manual account of type `cash` from the Settings page. `POST /v1/manual/accounts` returns a typed `ManualAccountResponse` (no bare dict).
  - [x] `CreateAccountRequest.type` is narrowed to `Literal["cash"]` — the only valid value. Pydantic rejects anything else at parse time with a 422.
  - [x] User can rename a manual account via `PUT /v1/manual/accounts/{account_id}` returning `ManualAccountResponse`.
  - [x] User can create a manual transaction with: amount, date, description, direction (income or expense), and account (cash account). `POST /v1/manual/transactions` returns a typed `ManualTransactionResponse`.
  - [x] `CreateTransactionRequest.direction` is narrowed to `Literal[TransactionDirection.income, TransactionDirection.expense]` — manual entry never produces a `zero`-direction transaction. (The shared `TransactionDirection` enum keeps its `zero` member for bank events that represent balance-only adjustments; the narrowing happens at the request schema, not on the shared enum.)
  - [x] Manual transactions are published to the `raw_transactions.manual` Redpanda topic.
  - [x] Manual transactions appear alongside Monobank transactions in all views.
  - [x] Manual transactions are included in chart aggregates.
  - [x] Authorization: the account referenced by `POST /v1/manual/transactions` must be owned by the caller; otherwise 404 with `code: ACCOUNT_NOT_FOUND`.
  - [x] `DELETE /v1/manual/accounts/{account_id}` soft-deletes the account (sets `is_active = false`). Historical transactions on the account are preserved. The account remains in `GET /v1/accounts` results with `is_active: false`; clients filter on the field to hide deactivated accounts in default views. (Server-side filtering is a deferred enhancement — surfacing the flag keeps audit views and unhide-flows possible without a separate "include_inactive" query param.)
  - [x] DELETE rejects bank-connected accounts: if the account's `source != 'manual'`, return 403 with `code: INSUFFICIENT_PERMISSIONS`. Bank accounts are managed via `DELETE /v1/monobank/integrations/{integration_id}`.
  - [x] DELETE returns 404 with `code: ACCOUNT_NOT_FOUND` when the account does not exist or is not owned by the caller (no existence leak).

### 2.6 REST API Endpoints

The pipeline exposes data to frontend consumers via REST. All endpoints are mounted under `/v1/` (per §2.10). Every endpoint declares a `response_model` and produces a fully-resolved OpenAPI schema — no bare-dict returns anywhere.

- **Acceptance Criteria:**
  - [x] `GET /v1/transactions` — paginated list of transactions. Filter parameters:
    - `direction` (single value: `income`/`expense`)
    - `category` (repeated-key list of `special_category` enum values, e.g. `?category=transfer&category=cancellation`) — **whitelist**. Narrows the result to rows whose `special_category` is in the supplied set. Use for category-focused views (a "Transfers" tab, an audit drill-down). Rows with `special_category IS NULL` are never matched by this filter.
    - `exclude_category` (repeated-key list of `special_category` enum values) — **blacklist**. Hides rows whose `special_category` is in the supplied set; rows with `special_category IS NULL` (ordinary transactions) are always included. This is the canonical way for the frontend feed to ask "show me my spending, hide the noise" without breaking when new special categories ship later.
    - `account_id` (single UUID).
    - `from` / `to` (half-open date range `[from, to)`).
    - **Conflict rule.** If any value appears in both `category` and `exclude_category` for the same request (e.g., `?category=transfer&exclude_category=transfer`), the server returns 422 with a descriptive error naming the conflicting value. Disjoint combinations are allowed.
  - [x] `GET /v1/transactions/aggregates` — flexible time-window aggregation (see §2.7).
  - [x] `GET /v1/accounts` — list of the authenticated user's accounts (Monobank and manual).
  - [x] `GET /v1/rates` — paginated list of currency rates, filterable by source, currency pair, and date range. Rates are global (not user-scoped).
  - [x] `GET /v1/rates/at` — all rates active at a given timestamp (defaults to now), with optional source filter. SCD2 point-in-time query (`valid_from <= at AND (valid_to IS NULL OR valid_to > at)`). Distinct from `GET /v1/rates` which filters on `valid_from`; this endpoint answers "what rates applied *then*" rather than "what rates were *published* in this window."
  - [x] `GET /v1/admin/users` — admin-only paginated list of users with activity information (see §2.6.1).
  - [x] `GET /v1/settings` and `PUT /v1/settings` — per-user preferences (default `rate_source`, timezone). PUT validates `rate_source` against `rate_source_config`; invalid value → 422.
  - [x] All endpoints are scoped to the authenticated user via JWT + RLS (except rates, which are global, and the admin user list, which is admin-only).
  - [x] Every endpoint declares an explicit `response_model` — no bare `dict` returns. An OpenAPI completeness check (CI) parses the live `/openapi.json` from both services and asserts every operation has a non-empty `responses[*].content[*].schema` (no `additionalProperties: true` stubs).

#### 2.6.1 Admin User List

Admin endpoint for listing users with activity information. Used by future household-aggregate views (Phase 4) but exposed in Phase 1 for operational visibility.

- **Acceptance Criteria:**
  - [x] `GET /v1/admin/users` returns `CursorPage[AdminUserResponse]`. Fields: `id`, `email`, `role`, `created_at`, `last_active_at`.
  - [x] Admin-only. Non-admin callers receive 403 with `code: INSUFFICIENT_PERMISSIONS`.
  - [x] Pagination uses the existing `CursorPage` envelope (consistent with `GET /v1/transactions` and `GET /v1/rates`). Sort order: `created_at DESC, id DESC`. The cursor encodes the composite `(created_at, id)` tuple — opaque to clients. Successor predicate in SQL: `(created_at, id) < (cursor_created_at, cursor_id)`.
  - [x] Page size: `limit=50` default, `ge=1, le=200` bounds.
  - [x] An invalid (undecodable) cursor returns 400 with `code: INVALID_CURSOR`.
  - [x] `last_active_at` definition: time the user last issued an access token (sign-in or refresh-token rotation). NOT time of last individual API request — that would be too noisy for the DB. NULL means the user has not authenticated since the column was added.

#### 2.6.2 Admin User Management

Admin endpoints for creating and removing users. There is no public signup; user provisioning is admin-only by design (per CLAUDE.md "Non-Goals").

**Endpoints (under `/v1/`, admin-only):**

- `POST /v1/admin/users` — create a user. Body: `{email, password, display_name, role?}` (`role` defaults to `member`). Returns 201 with `UserCreatedResponse(id, email, display_name, role)`.
- `DELETE /v1/admin/users/{user_id}` — soft-delete (deactivate) a user. Returns 204.

- **Acceptance Criteria:**
  - [x] Both endpoints require admin role. Non-admin callers receive 403 with `code: INSUFFICIENT_PERMISSIONS`.
  - [x] `POST /v1/admin/users` validates `email` as RFC-compliant and lowercases it before persistence. Password hashing uses the same `AuthService.hash_password` as login.
  - [x] Email uniqueness: re-using an existing email returns 409 with `code: VALIDATION_ERROR` and detail `"A user with this email already exists."` The check is racy-safe — a concurrent INSERT that hits the underlying `UNIQUE` constraint surfaces the same response.
  - [x] `POST /v1/admin/users` returns `UserCreatedResponse` (typed model — no bare dict). The created row's `user_settings` row is auto-created by the `AFTER INSERT ON users` trigger; the admin RLS carve-out (`users_admin_all`, `user_settings_admin_all`) is what makes the trigger succeed in the admin's session.
  - [x] `DELETE /v1/admin/users/{user_id}` soft-deletes by setting `users.is_active = false` and deleting all refresh tokens for the target. Deactivation is **instant logout for the access token too** — `get_current_user` rejects any request whose JWT belongs to a user with `is_active = false`, returning 401 `AUTHENTICATION_REQUIRED`. No `revoked_tokens` entry is needed for the deactivated user's JTI; the `is_active` gate is the kill switch.
  - [x] Self-delete is forbidden: if `user_id == caller.id`, return 400 with `code: VALIDATION_ERROR` and detail `"Cannot delete your own account."` A second admin must perform the deletion.
  - [x] Deleting a non-existent user returns 404 with `code: USER_NOT_FOUND`.
  - [x] Soft-deletion preserves all the user's accounts and transactions in the DB. Monobank webhook registrations live on the bank side and are NOT de-registered by user deactivation, so events for the deactivated user's integrations keep arriving at `POST /monobank/webhook/{secret}`. The receiver publishes them to Kafka as normal; the enrichment consumer writes the resulting transactions to the DB. Inertness is only enforced at the API layer (via the `is_active` gate on every authenticated endpoint) — the user can no longer log in to see, edit, or trigger anything on those rows. Hard-stopping new transactions requires explicitly DELETEing the Monobank integration before deactivating the user.

### 2.7 Aggregation API (Compute-on-Read)

Time-window aggregations are computed on read from a single SQL query — no materialized views, no refresh policies, no staleness after backfill or reclassification. With an index on `(user_id, time DESC)`, any aggregation pattern completes in under 1ms at per-user scale.

- **Acceptance Criteria:**
  - [x] `GET /v1/transactions/aggregates` accepts parameters: `currency` (repeated-key list of `UAH`/`USD`/`EUR`, default: all three), `from`/`to` (date range, default: all history), `bucket` (day/week/month/quarter/year, default: month), `fields` (repeated-key list of `income`/`expense`/`delta`, default: all three).
  - [x] Response groups results by `period_start`, with a `currencies` dict containing requested currencies. Each currency object has `total_income_cents`, `total_expense_cents`, `delta_cents`, and `converted_pct`.
  - [x] Internal transfers and other special categories are excluded from aggregation by default (`WHERE special_category IS NULL` — includes only ordinary income/expense).
  - [x] `converted_pct` reports the percentage of transactions in each bucket that have a non-NULL value for that currency's amount column. When < 100%, the frontend can surface the unconverted rows by filtering the transaction list client-side.
  - [x] Timezone-aware bucketing: `date_trunc` uses `AT TIME ZONE` with the user's timezone (stored in `user_settings`, default UTC, auto-detected from browser on first load) so that a transaction at 23:30 on Jan 31 falls into January, not February.
  - [x] `fields` parameter is a presentation concern — the query always computes all values; `fields` controls which are included in the JSON response.
  - [x] All requested currencies are computed in a single query (one index scan, one grouping pass).

### 2.8 Currency Rate Reliability

Exchange rate sources may be unavailable (app downtime, API outage). The system handles stale or missing rates gracefully.

- **Acceptance Criteria:**
  - [x] Each rate source has a configured fallback chain (e.g., Monobank → NBU) loaded per-transaction via a recursive CTE. The consumer walks the chain in priority order.
  - [x] Rate resolution uses two quality tiers (FRESH → CLOSEST). Both legs of a chained conversion must resolve at the same tier — never mix tiers (a FRESH leg + a CLOSEST leg yields NULL rather than a falsely "FRESH" conversion). K (the FRESH grace multiplier on `update_cadence_seconds`) is a fixed per-source constant; current value is `K=2`.
  - [x] The system tracks when each rate was last confirmed (polled). FRESH rates have `last_polled_at` within `K * update_cadence_seconds` of the transaction time. CLOSEST is a last resort within a 7-day window, ranked by proximity to the transaction time.
  - [x] Currency conversion uses liquidation-side pricing: `rate_buy` when multiplying (bank buys from user), `rate_sell` when dividing (bank sells to user), `rate_mid` as fallback when the chosen side is NULL. Never substitutes the opposite side.
  - [x] The full rate path is recorded in transaction metadata with per-step traceability: `from`, `to`, `source`, `rate_id`, `rate`, `rate_side` (buy/sell/mid), `tier` (fresh/closest), `op` (multiply/divide). Path-level: `effective_rate`, `hops`, `quality` (worst tier), `sides` used.
  - [x] Pivot currencies for chained conversions are derived from `rate_source_config.base_currencies` (no hard-coded pivot). Pivots are ordered by chain depth then array position.
  - [x] Historical exchange rates can be backfilled from NBU for any past date range. NBU supports date-range queries per currency (`?start=YYYYMMDD&end=YYYYMMDD&valcode=USD`), so backfilling 2 years of all ~45 currencies requires ~45 HTTP requests (~3 MB storage).
  - [x] Historical rate backfill is triggered by an admin via a K8s Job (same pattern as transaction backfill).
  - [x] The fallback chain configuration is stored in the database, editable without redeployment. Cyclic or excessively deep chains (>10 hops) raise `RateSourceChainError` at query time.
  - [x] Rate fallback is restricted to configured sources — unconfigured sources cannot affect conversions.
  - [x] Missing rates result in NULL amounts, not zero or approximations from untrusted sources. NULL amounts can be repaired later via reprocessing after rates are backfilled.

### 2.9 Transaction Reprocessing

The enrichment pipeline evolves over time — new layers are added (transfer detection, classification), rate backfills provide previously missing conversion data, and bug fixes improve stored results. Users need a way to re-run the full pipeline on their historical data without re-fetching from Monobank (rate-limited, slow, unnecessary).

**Endpoints (all under `/v1/`, per §2.10):**

- `POST /v1/users/{user_id}/reprocess` — per-user trigger. Authorized as `caller.id == user_id OR caller.role == admin`. Empty request body. Returns 202 with `JobTriggerResponse`.
- `GET /v1/users/{user_id}/reprocess/{job_id}` — poll status. Same authorization.
- `POST /v1/admin/reprocess` — admin-only bulk trigger. Body: `{"user_ids": list[UUID] | null, "force": bool = false}` (null = "all users"). Returns 202 with `BulkReprocessResponse`.
- `GET /v1/admin/reprocess/{job_id}` — poll bulk status. Admin-only.

**Lock ownership and race-free triggering.** The `reprocessing_locks` table has schema `(user_id PK, locked_at TIMESTAMPTZ)` — the **presence of a row** is the lock (no `status` column). The triggering API owns lock creation, not the job pod, so that two concurrent triggers cannot both pass a stale "is there a lock?" check and submit duplicate jobs.

Concrete sequence per trigger:

1. API receives the request.
2. API applies the per-user rate-limit via atomic UPDATE-RETURNING on `users.last_reprocess_started_at`. 0 rows ⇒ 429 with `code: RATE_LIMITED`; 1 row ⇒ proceed.
3. API begins a DB transaction and attempts `INSERT INTO reprocessing_locks (user_id) VALUES ($1)`. On `unique_violation`: return 409 with `code: REPROCESS_LOCKED`. On success, the API now owns the lock within its transaction.
4. API submits the K8s Job (with `USER_IDS_JSON` env var carrying the target user_id(s)). If submission fails (K8s API unreachable, timeout, sync exception): the API rolls back the lock-row insert and the rate-limit timestamp, then returns 502 with `code: JOB_SUBMISSION_FAILED`. The user may immediately retry.
5. API commits and returns 202.
6. The job pod starts. At startup it **checks for the lock-row presence** for each user it is processing. If absent (the rare network-glitch case where the K8s Job was created but the API lost the ack and rolled back), the pod logs `"Reprocessing lock not found for user_id={uuid}; exiting cleanly"`, exits with status code 0, and the K8s Job ends in `status: succeeded`. No `transactions` rows are touched.
7. On normal completion (or fatal failure), the pod deletes its lock row(s).

- **Acceptance Criteria:**
  - [x] Users can trigger reprocessing of their own transactions via `POST /v1/users/{user_id}/reprocess` where `user_id == caller.id`. Admins can additionally trigger reprocess of any user's data via the same URL with `user_id == target_user.id`.
  - [x] Admin can trigger bulk reprocess of any subset (or all users) via `POST /v1/admin/reprocess` with body `{"user_ids": list[UUID] | null, "force": bool = false}`. `null` means "every current user."
  - [x] **Rate-limit: 1 reprocess per hour per `user_id`**, enforced by `users.last_reprocess_started_at` and an atomic UPDATE-RETURNING that combines check-and-set in one SQL statement to eliminate TOCTOU. Returns 429 with `code: RATE_LIMITED` and a `detail` field naming when the user becomes eligible again (`last_reprocess_started_at + 1 hour`). Admin retains a `force: true` escape hatch on the bulk endpoint that bypasses the time-window predicate (but still consumes the user's hourly slot).
  - [x] **Bounded K8s submit.** The kubernetes client call is given a socket-level timeout (`K8S_JOB_SUBMIT_TIMEOUT_SECONDS = 10`). The dispatcher catches both `kubernetes.client.exceptions.ApiException` AND `urllib3.exceptions.TimeoutError` (the kubernetes client does NOT wrap urllib3 timeouts as `ApiException`) and re-raises as `K8sDispatchError`; the router's transaction rolls back on either.
  - [x] On concurrent trigger for an already-locked user, return 409 with `code: REPROCESS_LOCKED`. The response body's `detail` follows the format `"Reprocessing already in progress for user {user_id}. Existing job: {job_id}. Poll {status_url} for progress."` so a client can parse the running job's identity and status URL from a single human-readable string. The job identity is resolved via K8s label selector `grosh.app/job-kind=reprocess,grosh.app/user-id={uuid}`.
  - [x] The API owns lock-row creation (atomic INSERT within the trigger transaction); the job pod does NOT insert into `reprocessing_locks` on startup. Pod startup **asserts** the row exists; if absent, exits 0 cleanly without touching any `transactions` rows. Integration test exercises this path: (a) trigger a reprocess, (b) before the pod starts, DELETE the lock row manually, (c) confirm the pod logs the expected message and exits 0.
  - [x] Bulk admin reprocess processes the target user list one-by-one inside the API request (not inside the job pod): for each `user_id`, attempt the rate-limit update then the lock insert. Users whose rate-limit blocks land in `skipped` with reason `RATE_LIMITED`; users already-locked land with reason `REPROCESS_LOCKED`. Users whose insert succeeds become the actual reprocess targets. The K8s Job is then submitted with the successful user_ids as its `USER_IDS_JSON` env var.
  - [x] Bulk admin reprocess with `user_ids: null` enumerates current user IDs via a single `SELECT id FROM users` **snapshot**, then runs the per-user lock-insert loop on the snapshot list. Users created after the snapshot is taken are NOT included in this job — deliberate snapshot semantic ("reprocess everyone who existed at trigger time"). Later invocations can pick up new users.
  - [x] Bulk admin response: `BulkReprocessResponse` with `job_id: str | None`, `status_url: str | None`, `skipped: list[{user_id, reason}]`. `skipped` is always present, always a list (empty when no users skipped). A model validator asserts `job_id` and `status_url` are either both null or both non-null — never one without the other.
  - [x] When every user in a bulk request is skipped, the API returns 202 with `job_id: null`, `status_url: null`, and `skipped` containing the full list. Clients branch on `job_id is null` to skip polling.
  - [x] **`force: true` semantics.** When the caller is admin AND `force: true`, the per-hour check is bypassed: the rate-limit UPDATE drops the time-window predicate. The bypass applies per-target-user. The timestamp is still updated. **Subsequent force triggers are NOT rate-limited** — by design; admin discretion is the constraint.
  - [x] Reprocessing reads existing transaction rows from the DB, reconstructs the intermediate `NormalizedTransaction` from stored columns (source-agnostic — one format regardless of which bank originated the data), deletes original rows, and republishes to the pipeline topic for fresh processing through all downstream layers.
  - [x] The reprocessing job backs up all affected data before deletion. If anything goes wrong during replay, the system can restore from backup. A `pg_cron` job (`reprocessing_backups_cleanup`) deletes backups older than 30 days, runs daily at 03:00 UTC; the migration that registers it includes a fail-fast TZ guard so a misconfigured Postgres deploy fails the migration loudly rather than scheduling the job in the wrong window.
  - [x] During reprocessing, the user sees a "refreshing" indicator (data is temporarily incomplete).
  - [x] Reprocessing is safe — concurrent webhook events for the same user are routed to a staging buffer by the normalization service and drained after the lock releases. No data loss from race conditions; full mechanics in `references/adr-transaction-reprocessing.md`.
  - [x] After reprocessing completes, the job verifies that every transaction ID from the pre-delete snapshot exists in the `transactions` table again. If any IDs are missing, the job restores from backup, releases the lock, and surfaces the failure. The verification checks **presence by ID only** — pipeline-derived fields (`special_category`, `related_transaction_id`, converted amount columns, `metadata.layer.*`) are expected to change after reprocess.
  - [x] **Staging-drain observability.** The staging drain sweep loop logs the age of the oldest staged row on every iteration. If the age exceeds 300 seconds (5 minutes), the log line is emitted at WARN level. Otherwise DEBUG. Empty table → `staging_drain.staged_row_count=0` at DEBUG (no age field).
  - [x] K8s Jobs carry labels `app.kubernetes.io/managed-by=grosh-ingestion`, `grosh.app/job-kind=reprocess`. Per-user reprocess jobs additionally carry `grosh.app/user-id={uuid}`; admin bulk reprocess jobs do not (the user list is in the env var instead).
  - [x] The status endpoint authorization: per-user URL requires `caller.id == user_id OR caller.role == admin`; admin-bulk URL requires admin. A job that doesn't exist OR doesn't match the URL's scope returns 404 with `code: JOB_NOT_FOUND` (IDOR defense — never reveal existence outside scope).
  - [x] Idempotent: if interrupted and re-triggered, produces the same result without duplication or data loss.

### 2.10 Cross-Cutting API Contract

Rules that apply to every endpoint in both the api and ingestion services.

#### 2.10.1 Versioning

All HTTP endpoints across both services are mounted under a `/v1` prefix, **except** webhook receivers (URLs already registered with external systems like Monobank cannot be changed without re-registration). The webhook stays at `/monobank/webhook/{webhook_secret}` (per §2.2) for the lifetime of the v1 API.

- **Acceptance Criteria:**
  - [x] Every route in `services/api` is reachable under `/v1/...`.
  - [x] Every route in `services/ingestion` is reachable under `/v1/...` except the Monobank webhook.
  - [x] OpenAPI documentation at `/docs` reflects the versioned URLs.
  - [x] Versioning is implemented at the FastAPI `APIRouter` prefix level: both `services/api/main.py` and `services/ingestion/main.py` mount their routers via `app.include_router(router, prefix="/v1")`, not by hand-editing every route declaration. The webhook router in `services/ingestion` is the one router mounted without the `/v1` prefix; every other `include_router` call in either service uses it.
  - [x] The Postman collection (`infra/grosh.postman_collection.json`) uses `/v1` prefixes for all non-webhook requests.

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

- `type`: stable URL identifying the error class (placeholder URL is fine for now — frontend uses `code` for branching).
- `title`: short human-readable summary (English, frontend may localize via `code`).
- `status`: HTTP status code, duplicated in the body.
- `code`: stable machine-readable code from `grosh_shared.errors.ErrorCode` StrEnum.
- `detail`: human-readable explanation, never contains tokens or secrets.
- `instance`: the request path that produced the error.
- `validation_errors`: `null` for non-422 errors; on 422, list of `{loc: list[str|int], msg: str, type: str}` (Pydantic v2's native `.errors()` shape, with `loc` tuple→list converted for JSON compatibility).

**`ErrorCode` enumeration (complete, exhaustive):**

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
| `RATE_LIMITED`                | 429         | Per-user reprocess rate-limit exceeded         |
| `BACKFILL_WINDOW_TOO_LARGE`   | 422         | Backfill window exceeds Monobank's 31-day cap |
| `MONOBANK_TOKEN_INVALID`      | 422         | Token rejected by Monobank during link        |
| `MONOBANK_API_UNAVAILABLE`    | 502         | Monobank API timed out or returned 5xx        |
| `INTEGRATION_ALREADY_LINKED`  | 409         | User already has an active integration for a different Monobank client_id |
| `JOB_STATUS_UNAVAILABLE`      | 503         | K8s API unreachable while fetching job status |
| `JOB_SUBMISSION_FAILED`       | 502         | K8s API unreachable while creating a new Job  |
| `INTERNAL_ERROR`              | 500         | Unhandled exception (last-resort catch-all)   |

This list is exhaustive. Adding a new code requires a documentation update in this section. Code names are SCREAMING_SNAKE_CASE; never abbreviate.

- **Acceptance Criteria:**
  - [x] `grosh_shared/http/errors.py` defines `ErrorCode(StrEnum)` containing exactly the codes above — no more, no fewer.
  - [x] `grosh_shared` exposes a `ProblemDetail` Pydantic model and a `raise_problem(...)` helper that constructs and raises an `HTTPException` whose body conforms to the envelope.
  - [x] Both `services/api` and `services/ingestion` register error handlers for: `HTTPException`, Pydantic `RequestValidationError`, and a catch-all 500 handler. Each emits the envelope.
  - [x] On 422 validation errors, `validation_errors` contains the Pydantic v2 `.errors()` output serialized as `list[{loc: list[str|int], msg: str, type: str}]`. On every other error, `validation_errors` is `null`.
  - [x] No domain error returns raw `{"detail": "..."}` — every raised error has a stable `code` from the enum above.
  - [x] Frontend never sees an error response without a `code` field.
  - [x] Logged error context never includes credentials, refresh tokens, or Monobank tokens. An integration test asserts that calling `/v1/monobank/link` with a malformed token returns an error envelope whose `detail` does not contain the token substring.
  - [x] Pydantic validators on sensitive fields (`monobank_token`, password) emit generic error messages — no user input is echoed in the validation error.

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

Both "job does not exist" and "job exists but does not match the URL's scope" return **404 with `code: JOB_NOT_FOUND`**. K8s Jobs created by trigger endpoints carry ownership labels (`grosh.app/user-id` and/or `grosh.app/account-id`); the status endpoint verifies these against the URL's scope before responding.

Job-status endpoints are not cached — every request makes a fresh K8s API call. At our scale this is acceptable; if it ever becomes a bottleneck, a short-lived (1-2s) in-memory cache keyed by `job_id` is a future improvement.

#### 2.10.4 OpenAPI Completeness

Every endpoint in both services exposes a fully-resolved response schema at `/docs`.

- **Acceptance Criteria:**
  - [x] No endpoint shows `anyOf: [{}]`, `additionalProperties: true`-only, or empty `$ref` in the OpenAPI schema.
  - [x] A CI check parses the live `/openapi.json` from both services and asserts every operation has a non-empty `responses[*].content[*].schema`.

### 2.11 Row-Level Security Invariants

User-scoped tables enforce RLS on both reads AND writes. Every RLS policy on a user-scoped table carries both `USING` and `WITH CHECK` clauses. `USING` filters reads and limits which rows an UPDATE/DELETE can target; `WITH CHECK` rejects the new row image on INSERT or UPDATE.

- **Acceptance Criteria:**
  - [x] Every user-scoped table (`accounts`, `bank_integrations`, `categories`, `transactions`, `user_settings`, `users`) has RLS enabled and at least one policy with `WITH CHECK (user_id = app.current_user_id())`.
  - [x] The `categories` policy retains its `OR user_id IS NULL` system-category branch in both `USING` and `WITH CHECK`. The `WITH CHECK` form requires admin role: `WITH CHECK (user_id = app.current_user_id() OR (user_id IS NULL AND app.current_user_role() = 'admin'))`. System categories can only be inserted by admin role; reads of `user_id IS NULL` rows are preserved for non-admins.
  - [x] **CI invariant test.** A pytest integration test queries `pg_class.relrowsecurity` and `pg_policies` after migrations run on a fresh DB, enumerates every table in `public` that has a `user_id` column, and asserts each one has `relrowsecurity = true` AND at least one policy. An explicit allowlist of exempt tables exists (currently empty). The test produces a readable failure message listing offending tables.
  - [x] Integration test: with `app.current_user_id` set to user A, an attempt to INSERT or UPDATE a row with `user_id = <user_B_uuid>` raises a Postgres RLS error (sqlstate `42501`).
  - [x] The enrichment service (which runs under `grosh_consumer` with `BYPASSRLS`) continues to write `transactions` rows without RLS interference. Verified by the existing carve-out test in `tests/e2e/integration/test_single_writer_carveout.py`.

---

## 3. Scope and Boundaries

### In-Scope

- Monobank account linking (self-service via Settings page).
- Monobank webhook receiver (GET verification + POST transaction ingestion).
- Monobank historical backfill with manual trigger and progress indication.
- Two-service consumer pipeline: normalization service (owns every producer of `normalized_transactions` — steady-state normalization, staging drain, reprocess job) and enrichment service (pure consumer of `normalized_transactions` running transfer detection → currency conversion → classification → persistence).
- Per-source transfer detection strategy registry. Monobank uses the 7-step algorithm (universal candidate fetch + IBAN/description evidence ranking + count-and-decide). Other banks plug in their own strategies without modifying generic code.
- Transfer match anomaly recording and auto-resolution.
- Manual cash account creation and manual transaction entry (through Redpanda).
- Tri-currency storage (UAH + USD + EUR equivalents) on every transaction, denormalized at write time.
- Currency rate ingestion from Monobank and NBU with configurable polling cadence.
- Rate fallback chain (monobank → nbu) with stale rate detection via `last_polled_at`.
- Admin-triggered historical rate backfill from NBU via K8s Job.
- Rate source traceability in transaction metadata (per-step path with tier, side, operation).
- Compute-on-read aggregation API (any bucket, any currency, any date range, timezone-aware).
- REST API endpoints for transactions, aggregates, accounts, rates, settings, and admin users — all under `/v1/` prefix, all returning typed response models.
- Monobank integration lifecycle CRUD: list / link (idempotent on `monobank_client_id`) / delete.
- K8s Job status endpoints (per-account backfill, rates backfill, per-user reprocess, admin bulk reprocess) returning native K8s status (no application-level progress in v1).
- Transaction reprocessing (per-user self-service + admin bulk via K8s Job) with race-free lock ownership (API inserts the lock atomically before submitting the Job; pod asserts the lock at startup) and per-user 1/hour rate-limit (admin force-override available).
- Cross-cutting API contract: `/v1` versioning, RFC 7807 error envelope with stable `ErrorCode` enum, OpenAPI completeness CI check.
- `users.last_active_at` column (set on access-token issuance) and admin user list endpoint.
- Per-service database roles with write-privilege separation.
- Access token revocation for instant logout.
- Encrypted Monobank token storage.
- RLS `WITH CHECK` on all user-scoped tables; CI invariant test asserting every `user_id`-carrying table has RLS enabled.
- `pg_cron` daily cleanup of `reprocessing_backups` (30-day retention).
- WARN-level log when oldest staged row age exceeds 5 minutes.

### Out-of-Scope

- Transaction feed UI and rolling monthly chart — separate spec (004).
- Transaction classification (rule engine, MCC fallback, ML classifier) — Phase 2.
- Feedback loop UI for category labeling — Phase 2.
- Per-account transaction view — future enhancement.
- Scheduled events and forecasting — Phase 3.
- Net worth dashboard and balance history — Phase 3.
- Family sharing and RLS enforcement — Phase 4.
- Debt tracking — deferred.
- Non-Monobank bank integrations (PUMB, Revolut) — Phase 5.
- Notification service — Phase 5.
- `TransactionEnvelope.payload` discriminated union by source — defer to first non-Monobank adapter slice.
- Per-source DLQ topics on `raw_transactions` — defer to first non-Monobank adapter slice.
- Grafana/Prometheus wiring for the staging-drain age metric — observability phase; today only the log line exists.
