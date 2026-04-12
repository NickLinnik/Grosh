# Functional Specification: Transaction Ingestion Pipeline

- **Roadmap Item:** Transaction Ingestion Pipeline (Phase 1)
- **Status:** Draft
- **Author:** Nick

---

## 1. Overview and Rationale (The "Why")

The spreadsheet workflow requires manual entry of every transaction — easy to forget, painful to backfill, and impossible to forecast from. The ingestion pipeline replaces this by automatically pulling transactions from Monobank in real time (webhook) and on demand (historical backfill), routing them through a unified streaming pipeline, and storing them in a queryable time-series database.

Users also need to record cash transactions that don't flow through any bank. All transaction sources — automated and manual — converge into the same Redpanda-based pipeline so that downstream consumers and future features have a single, consistent data source.

A key challenge is the user's FOP (sole proprietor) account structure: salary arrives on a USD FOP account, moves to a UAH FOP account (taxes paid there), then gets transferred in small batches to a UAH credit card for spending. Without transfer detection, these internal movements pollute income/expense numbers with false signals. The pipeline must distinguish real income/expense from internal transfers from the start.

**Success criteria:**

- All Monobank transactions arrive automatically via webhook — zero manual entry for bank transactions.
- Historical transactions importable via backfill on first setup; re-running backfill produces no duplicates.
- Internal transfers between user's own accounts are correctly detected and tagged.
- Cash transactions recordable manually through the same pipeline.
- REST API endpoints expose paginated transactions, monthly aggregates (pre-computed, per currency), and account listings.
- Monthly rollups with cross-currency totals (UAH, USD, EUR) are materialized as TimescaleDB continuous aggregates, powered by per-bank exchange rates stored in an SCD Type 2 table.
- Exchange rates are ingested from each bank's currency endpoint and NBU as fallback.

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

### 2.2 Monobank Webhook Receiver

FastAPI endpoint receives real-time transaction pushes from Monobank.

- **Acceptance Criteria:**
  - [ ] `GET /webhook/monobank` responds with `200 OK` (Monobank's verification request).
  - [ ] `POST /webhook/monobank` accepts Monobank's `StatementItem` payload.
  - [ ] The endpoint validates that the account ID in the payload matches a registered account in Grosh.
  - [ ] Valid transactions are published to the Redpanda `raw_transactions` topic, partitioned by `user_id`.
  - [ ] The endpoint responds within Monobank's timeout — publishing to Redpanda is fast; no synchronous processing.
  - [ ] [NEEDS CLARIFICATION: Monobank webhook payload authentication — research whether Monobank provides any signature or verification mechanism beyond the initial GET handshake.]

### 2.3 Historical Backfill

Users can import historical transactions from Monobank.

- **Acceptance Criteria:**
  - [ ] A "Backfill" button is available on the Settings page per linked Monobank account.
  - [ ] Triggering backfill starts a background job that paginates through Monobank's statement API (max 31 days per request, 1 request per 60 seconds rate limit per account).
  - [ ] Each batch of transactions is published to the same `raw_transactions` Redpanda topic.
  - [ ] The user sees a progress indicator (or at minimum a status: "Backfilling..." / "Complete").
  - [ ] Backfill is safe to re-run — the consumer deduplicates by Monobank transaction ID.
  - [ ] Backfill covers the maximum available history (up to 31 days per request, paginating backward).

### 2.4 Transaction Consumer

A Redpanda consumer subscribes to `raw_transactions`, processes, and stores transactions.

- **Acceptance Criteria:**
  - [ ] Consumer deduplicates by transaction source ID (Monobank ID or manual entry ID) — duplicate publishes are silently dropped.
  - [ ] Consumer writes transactions to the TimescaleDB `transactions` hypertable with all fields: user_id, account_id, time, amount (account currency), operation_amount (original currency), currency_code, description, mcc, cashback_amount, balance, hold status.
  - [ ] Both original currency amount and UAH equivalent are stored on every transaction.
  - [ ] Consumer detects internal transfers: when a transaction's counterparty IBAN matches another account owned by the same user, it is tagged as `transaction_type = 'transfer'`.
  - [ ] Transactions not matching an internal account are classified as `transaction_type = 'income'` (positive amount) or `transaction_type = 'expense'` (negative amount).
  - [ ] Transfer detection matches by counterparty IBAN, not by amount (tolerates FX conversion differences).
  - [ ] Consumer processes messages from all sources (webhook, backfill, manual) identically.

### 2.5 Manual Entry

Users can log cash transactions not captured by any bank.

- **Acceptance Criteria:**
  - [ ] User can create a manual account of type `cash` from the Settings page.
  - [ ] User can create a manual transaction with: amount, date, description, category, and account (cash account).
  - [ ] Manual transactions are published to the `raw_transactions` Redpanda topic with `source = 'manual'`.
  - [ ] Manual transactions appear alongside Monobank transactions in all views.
  - [ ] Manual transactions are included in chart aggregates.

### 2.6 REST API Endpoints

The pipeline exposes data to frontend consumers via REST.

- **Acceptance Criteria:**
  - [ ] `GET /transactions` — paginated list of transactions, filterable by transaction type (income/expense/transfer), account, and date range.
  - [ ] `GET /transactions/monthly-aggregate` — pre-computed monthly income, expense, and delta in all three display currencies (UAH, USD, EUR). Powered by TimescaleDB continuous aggregates over denormalized per-currency amounts.
  - [ ] `GET /accounts` — list of the authenticated user's accounts (Monobank and manual).
  - [ ] All endpoints are scoped to the authenticated user via JWT + RLS.

### 2.7 Continuous Aggregates

Monthly rollups are pre-computed in TimescaleDB for performant chart rendering.

- **Acceptance Criteria:**
  - [ ] A continuous aggregate materializes monthly income, expense, and delta totals per user in UAH, USD, and EUR simultaneously.
  - [ ] Amounts are denormalized at write time using per-bank exchange rates from the `currency_rates` SCD2 table.
  - [ ] Internal transfers are excluded from income and expense totals in the aggregate.
  - [ ] The aggregate supports querying a rolling 12-month window efficiently.
  - [ ] Exchange rates are ingested from each bank's currency endpoint (Monobank `/bank/currency`) with NBU daily rates as fallback.
  - [ ] The aggregate refreshes incrementally as new transactions are inserted.

---

## 3. Scope and Boundaries

### In-Scope

- Monobank account linking (self-service via Settings page)
- Monobank webhook receiver (GET verification + POST transaction ingestion)
- Monobank historical backfill with manual trigger and progress indication
- Redpanda-based transaction consumer with deduplication and transfer detection
- Manual cash account creation and manual transaction entry (through Redpanda)
- Dual-currency storage (original currency + UAH equivalent) on every transaction
- TimescaleDB continuous aggregates for monthly rollups by currency
- REST API endpoints for transactions, monthly aggregates, and accounts
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
