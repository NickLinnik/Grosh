# Technical Specification: Transaction Ingestion Pipeline

- **Functional Specification:** `context/spec/003-transaction-ingestion-pipeline/functional-spec.md`
- **Status:** Draft
- **Author(s):** Nick

---

## 1. High-Level Technical Approach

The pipeline spans three backend services and the database layer:

1. **API service** — gains a Redpanda producer (initialized in lifespan), new routers for webhook, accounts, manual entry, and transaction queries. Monobank account linking calls Monobank's API, stores encrypted tokens, and registers per-integration webhook URLs. Backfill is triggered via a Kubernetes Job.
2. **Consumer service** — built from scratch. Subscribes to `raw_transactions` topic, deduplicates via deterministic hash IDs (`ON CONFLICT DO NOTHING`), detects internal transfers by counterparty IBAN lookup, classifies transactions as income/expense/transfer, and writes to TimescaleDB.
3. **Database** — new migration enables TimescaleDB + pgcrypto extensions, creates `bank_integrations`, `accounts`, `categories`, and `transactions` (hypertable) tables with RLS policies, plus continuous aggregates for monthly rollups by currency.

No frontend changes in this spec — REST API endpoints are the delivery boundary. The Redpanda wire format (`RawTransactionEvent`) and Monobank API client live in `grosh-shared` for cross-service reuse.

---

## 2. Proposed Solution & Implementation Plan

### 2.1 Architecture Changes

**Redpanda producer** added to the API service, initialized in the FastAPI lifespan alongside the asyncpg pool. `confluent_kafka.Producer` with `bootstrap.servers = redpanda:9092`. Fire-and-forget produces with delivery callbacks for error logging. Flushed on shutdown.

**Backfill worker** runs as a Kubernetes Job. The API service triggers it via the `kubernetes` Python client. The Job pod uses the consumer service image with a backfill entrypoint. It subscribes to the `backfill_requests` Redpanda topic, paginates Monobank's statement API (1 req/60s rate limit), and publishes each transaction batch to `raw_transactions`.

**Consumer service** runs two consumer loops in the same process:
- **Transaction consumer** (`group.id = transaction-pipeline`) — subscribes to `raw_transactions`, deduplicates, detects transfers, writes to DB.
- The backfill K8s Job runs separately — it reads `backfill_requests` and produces to `raw_transactions`.

**Webhook endpoint** is unauthenticated (Monobank calls it). URL pattern: `/webhook/monobank/{webhook_secret}` where `webhook_secret` is an opaque random token stored on `bank_integrations`. Validated by looking up the secret in DB.

### 2.2 Data Model / Database Changes

New migration: `0004_transaction_pipeline.py`

**Extensions to enable:**

| Extension     | Purpose                              |
|---------------|--------------------------------------|
| `timescaledb` | Hypertables, continuous aggregates   |
| `pgcrypto`    | `pgp_sym_encrypt` for Monobank token |

**New ENUM types:**

| Type                | Values                        |
|---------------------|-------------------------------|
| `bank_source`       | `monobank`                    |
| `account_type`      | `black`, `white`, `platinum`, `fop`, `cash` |
| `transaction_type`  | `income`, `expense`, `transfer` |
| `transaction_source`| `monobank`, `manual`          |

**New tables:**

| Table               | Key Columns                                                                                                     | Notes                                          |
|---------------------|-----------------------------------------------------------------------------------------------------------------|------------------------------------------------|
| `bank_integrations` | `id UUID PK`, `user_id FK→users`, `bank bank_source`, `encrypted_token BYTEA`, `webhook_secret TEXT UNIQUE`, `webhook_url TEXT`, `status TEXT`, `created_at`, `updated_at` | RLS on `user_id`. Token encrypted via `pgp_sym_encrypt`. Webhook secret is a 32-byte hex random string. |
| `accounts`          | `id UUID PK`, `user_id FK→users`, `integration_id FK→bank_integrations NULL`, `source bank_source NULL`, `type account_type`, `currency_code TEXT`, `masked_pan TEXT`, `iban TEXT`, `monobank_id TEXT`, `cashback_type TEXT`, `is_active BOOLEAN`, `created_at`, `updated_at` | RLS on `user_id`. `integration_id` NULL for manual accounts. `monobank_id` is Monobank's internal account identifier. |
| `categories`        | `id UUID PK`, `user_id FK→users NULL`, `name TEXT`, `parent_id FK→categories NULL`, `created_at`               | RLS: `user_id = current_setting(...) OR user_id IS NULL` (system defaults visible to all). |
| `transactions`      | `id UUID PK` (deterministic hash), `source_id TEXT`, `user_id FK→users`, `account_id FK→accounts`, `time TIMESTAMPTZ`, `amount_cents BIGINT`, `operation_amount_cents BIGINT`, `currency_code TEXT`, `description TEXT`, `mcc INT`, `cashback_amount_cents BIGINT`, `balance_cents BIGINT`, `hold BOOLEAN`, `transaction_type transaction_type`, `counterparty_iban TEXT`, `source transaction_source`, `created_at` | **Hypertable** on `time`. RLS on `user_id`. `id` = `UUID5(source + ":" + source_id)`. `source_id` = Monobank's original transaction ID or generated UUID for manual entries. |

**Transaction ID strategy:** The primary key `id` is a deterministic UUID computed as `UUID5(NAMESPACE, source + ":" + source_id)` where `source` is "monobank" or "manual" and `source_id` is the external system's transaction identifier. This is computed by the producer (API side) before publishing to Redpanda. The consumer uses `INSERT ... ON CONFLICT (id) DO NOTHING` for idempotent deduplication.

**Indexes:**

| Index                                              | Purpose                            |
|----------------------------------------------------|------------------------------------|
| `transactions(user_id, time DESC)`                 | Feed pagination                    |
| `transactions(id)` UNIQUE (PK)                     | Deduplication via ON CONFLICT      |
| `transactions(user_id, account_id, time DESC)`     | Per-account queries (future)       |
| `accounts(user_id)`                                | Account listing                    |
| `accounts(iban)` WHERE `iban IS NOT NULL`           | Transfer detection lookup          |
| `bank_integrations(webhook_secret)` UNIQUE         | Webhook URL validation             |

**Continuous aggregates:**

```sql
CREATE MATERIALIZED VIEW monthly_aggregates
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 month', time) AS month,
    user_id,
    currency_code,
    SUM(amount_cents) FILTER (WHERE transaction_type = 'income') AS total_income_cents,
    SUM(ABS(amount_cents)) FILTER (WHERE transaction_type = 'expense') AS total_expense_cents,
    SUM(amount_cents) FILTER (WHERE transaction_type IN ('income', 'expense')) AS delta_cents
FROM transactions
WHERE transaction_type != 'transfer'
GROUP BY month, user_id, currency_code
WITH NO DATA;
```

Refresh policy: continuous, real-time aggregation enabled (combines materialized data with recent un-materialized rows).

### 2.3 API Contracts

**accounts.py router:**

| Method | Path                          | Auth     | Request Body                              | Response                               | Notes                                         |
|--------|-------------------------------|----------|-------------------------------------------|----------------------------------------|-----------------------------------------------|
| POST   | `/accounts/link-monobank`     | JWT      | `{ token: str }`                          | `{ integration_id, accounts: [...] }`  | Calls Monobank `/personal/client-info`, creates integration + accounts, registers webhook |
| POST   | `/accounts/{id}/backfill`     | JWT      | (none)                                    | `{ status: "started", job_name: str }` | Triggers K8s Job. Validates account belongs to user. |
| GET    | `/accounts`                   | JWT      | (none)                                    | `[{ id, type, currency_code, ... }]`   | User's accounts (RLS-scoped)                  |
| POST   | `/accounts/manual`            | JWT      | `{ type: "cash", currency_code, name }`   | `{ id, type, currency_code, ... }`     | Creates a manual cash account                 |

**webhook.py router:**

| Method | Path                                  | Auth  | Request Body              | Response | Notes                                    |
|--------|---------------------------------------|-------|---------------------------|----------|------------------------------------------|
| GET    | `/webhook/monobank/{webhook_secret}`  | None  | (none)                    | 200 OK   | Monobank verification handshake          |
| POST   | `/webhook/monobank/{webhook_secret}`  | None  | Monobank `StatementItem`  | 200 OK   | Validates secret, publishes to Redpanda  |

**transactions.py router:**

| Method | Path                                  | Auth | Query Params                                      | Response                                      |
|--------|---------------------------------------|------|---------------------------------------------------|-----------------------------------------------|
| GET    | `/transactions`                       | JWT  | `type`, `account_id`, `from`, `to`, `limit`, `offset` | Paginated list of transactions                |
| GET    | `/transactions/monthly-aggregate`     | JWT  | `currency` (UAH\|USD)                             | `[{ month, income_cents, expense_cents, delta_cents }]` |
| POST   | `/transactions/manual`                | JWT  | `{ account_id, amount_cents, currency_code, description, time, category_id? }` | Created transaction               |

### 2.4 Redpanda Topics

| Topic               | Partitions | Key        | Purpose                                              |
|----------------------|------------|------------|------------------------------------------------------|
| `raw_transactions`   | 3          | `user_id`  | All incoming transactions (webhook, backfill, manual) |
| `backfill_requests`  | 1          | `user_id`  | Backfill job requests from API to K8s Job worker      |

### 2.5 Shared Models (grosh-shared)

**New file: `shared/src/grosh_shared/events.py`**

| Model                  | Key Fields                                                                                                       | Purpose                     |
|------------------------|------------------------------------------------------------------------------------------------------------------|-----------------------------|
| `RawTransactionEvent`  | `id` (deterministic UUID), `source`, `source_id`, `user_id`, `account_id`, `time`, `amount_cents`, `operation_amount_cents`, `currency_code`, `description`, `mcc`, `cashback_amount_cents`, `balance_cents`, `hold`, `counterparty_iban` | Wire format on `raw_transactions` topic |
| `BackfillRequestEvent` | `integration_id`, `user_id`, `account_monobank_id`, `from_timestamp`, `to_timestamp`                            | Wire format on `backfill_requests` topic |

**New file: `shared/src/grosh_shared/id_utils.py`**

Deterministic UUID generation: `generate_transaction_id(source: str, source_id: str) -> UUID` using `uuid5(NAMESPACE, f"{source}:{source_id}")`.

**New file: `shared/src/grosh_shared/monobank_client.py`**

HTTP client for Monobank API using `httpx`. Methods: `get_client_info(token)`, `set_webhook(token, url)`, `get_statements(token, account_id, from_ts, to_ts)`. New dependency: `httpx` added to `grosh-shared` pyproject.toml.

**Update: `shared/src/grosh_shared/models.py`**

Update existing `Transaction` model to match the new DB schema (add `transaction_type`, `operation_amount_cents`, `source`, `counterparty_iban`, `balance_cents`, `hold`). Add `Account`, `BankIntegration` domain models.

### 2.6 Consumer Pipeline Logic

Transaction consumer processing flow:

1. Poll message from `raw_transactions`
2. Deserialize to `RawTransactionEvent`
3. `INSERT INTO transactions (...) VALUES (...) ON CONFLICT (id) DO NOTHING RETURNING id`
4. If inserted (not a duplicate):
   - Transfer detection: query `accounts` table for `iban = counterparty_iban` AND `user_id = event.user_id`
   - Match found → `UPDATE transactions SET transaction_type = 'transfer' WHERE id = ...`
   - No match + `amount_cents > 0` → `income` (already default from INSERT)
   - No match + `amount_cents < 0` → `expense`
5. Commit Kafka offset

Consumer config: `group.id = transaction-pipeline`, `auto.offset.reset = earliest`, `enable.auto.commit = false`. Manual commit after successful DB write.

### 2.7 Backfill K8s Job

**Job template:** `infra/k8s/backfill-job-template.yaml`

- Uses the consumer service Docker image with a `--mode=backfill` entrypoint flag
- Job subscribes to `backfill_requests` topic, processes one message, then exits
- Paginates Monobank statement API: starts from `to_timestamp`, walks backward in 31-day chunks
- Rate-limited: 1 request per 60 seconds per account (sleep between calls)
- Publishes each batch of transactions to `raw_transactions` as `RawTransactionEvent` messages
- `backoffLimit: 3`, `ttlSecondsAfterFinished: 3600`, `activeDeadlineSeconds: 1800`

**API triggers the job** via `kubernetes` Python client: reads the Job template, substitutes metadata (unique name with timestamp), creates the Job in the k3s namespace. New dependency: `kubernetes` package added to `grosh-api` pyproject.toml.

### 2.8 New File Structure Summary

**API service:**

| Path                                                            | Responsibility                                    |
|-----------------------------------------------------------------|---------------------------------------------------|
| `services/api/src/grosh_api/routers/accounts.py`                | Account linking, backfill trigger, manual accounts |
| `services/api/src/grosh_api/routers/webhook.py`                 | Monobank webhook GET/POST                         |
| `services/api/src/grosh_api/routers/transactions.py`            | Transaction list, monthly aggregate, manual entry  |
| `services/api/src/grosh_api/services/account_service.py`        | Monobank API orchestration, account creation       |
| `services/api/src/grosh_api/services/backfill_service.py`       | K8s Job creation and management                    |
| `services/api/src/grosh_api/services/transaction_service.py`    | Transaction queries, manual entry publishing       |
| `services/api/src/grosh_api/repositories/account_repo.py`       | Account + integration DB operations                |
| `services/api/src/grosh_api/repositories/transaction_repo.py`   | Transaction queries, aggregate queries             |
| `services/api/migrations/versions/0004_transaction_pipeline.py` | Extensions, tables, hypertable, aggregates, RLS    |

**Consumer service:**

| Path                                                                      | Responsibility                        |
|---------------------------------------------------------------------------|---------------------------------------|
| `services/consumer/src/grosh_consumer/consumer.py`                        | Main consumer loop, message dispatch  |
| `services/consumer/src/grosh_consumer/handlers/transaction_handler.py`    | Dedup + transfer detection + DB write |
| `services/consumer/src/grosh_consumer/db.py`                              | asyncpg pool setup for consumer       |

**Shared package:**

| Path                                              | Responsibility                                  |
|---------------------------------------------------|-------------------------------------------------|
| `shared/src/grosh_shared/events.py`               | `RawTransactionEvent`, `BackfillRequestEvent`   |
| `shared/src/grosh_shared/id_utils.py`             | Deterministic UUID hash function                |
| `shared/src/grosh_shared/monobank_client.py`      | Monobank API HTTP client (httpx)                |
| `shared/src/grosh_shared/models.py`               | Updated `Transaction` + new `Account`, `BankIntegration` domain models |

**Infrastructure:**

| Path                                              | Responsibility                                  |
|---------------------------------------------------|-------------------------------------------------|
| `infra/k8s/backfill-job-template.yaml`            | K8s Job manifest template for backfill worker   |

---

## 3. Impact and Risk Analysis

### System Dependencies

- **API service** depends on: Redpanda (producer), TimescaleDB (reads), Monobank API (linking/webhook registration), K8s API (backfill job trigger)
- **Consumer service** depends on: Redpanda (consumer), TimescaleDB (writes)
- **Backfill Job** depends on: Monobank API (statement reads), Redpanda (producer), shared package

### Potential Risks & Mitigations

| Risk                                                 | Mitigation                                                                                          |
|------------------------------------------------------|-----------------------------------------------------------------------------------------------------|
| Monobank webhook replay / duplicate delivery         | Deterministic hash ID + `ON CONFLICT DO NOTHING` makes consumer fully idempotent                    |
| Monobank API downtime during backfill                | K8s Job `backoffLimit: 3` retries. Backfill is idempotent — safe to re-run.                         |
| Webhook endpoint abuse (public, unauthenticated)     | Opaque webhook secret in URL (unguessable). Validate account exists in DB. Rate-limit endpoint.     |
| Redpanda unavailable when webhook fires              | Producer delivery failure logged. Monobank retries webhook delivery (built-in). No data loss.       |
| Transfer detection misses (IBAN not yet registered)  | When a new account is linked, re-scan recent transactions for transfer matches.                     |
| Consumer crashes mid-batch                           | Manual offset commit after DB write. At-least-once + idempotent dedup. No data loss.                |
| [NEEDS CLARIFICATION] Monobank webhook auth          | Research whether Monobank provides any signature or verification beyond the GET handshake. Currently relying on opaque URL + account ID validation. |

---

## 4. Testing Strategy

| Layer                | Approach                                                                                                           |
|----------------------|--------------------------------------------------------------------------------------------------------------------|
| **Unit tests**       | Service layer: mock repos, Monobank client, and K8s client. Test transfer detection logic, deterministic ID hashing, event serialization/deserialization. Consumer handler: mock DB, test dedup behavior and transaction type classification. |
| **Integration tests**| API: real DB (existing rollback-transaction pattern from conftest), mocked Redpanda producer. Verify endpoints produce correct events and return correct query results. Consumer: real DB, feed pre-built events. Verify transactions land with correct types and dedup works. |
| **Contract tests**   | Verify `RawTransactionEvent` round-trip: serialize in API → deserialize in consumer. Ensures shared model stays consistent across services. |
| **Backfill tests**   | Unit test the pagination logic (mock Monobank API responses). Integration test with a local K8s environment is deferred to Phase 2 Go Live. |
