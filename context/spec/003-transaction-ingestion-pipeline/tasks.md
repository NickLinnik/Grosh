# Tasks: Transaction Ingestion Pipeline

---

## Slice 1: Database foundation — extensions, tables, hypertable, aggregates, currency rates

- [x] Create migration `0004_transaction_pipeline.py` — enable `timescaledb` and `pgcrypto` extensions, create enums (`bank_source`, `account_type`, `transaction_type`, `transaction_source`, `transaction_origin`), create `bank_integrations`, `accounts`, `categories`, `transactions` tables with all columns per tech spec (including `amount_uah_cents`, `amount_usd_cents`, `amount_eur_cents`, `metadata JSONB`, `origin`, `related_transaction_id`). Convert `transactions` to hypertable. Create `currency_rates` SCD2 table. Add all indexes. Enable RLS + policies on all user-scoped tables. **[Agent: postgres-database]**
- [x] Create migration `0005_monthly_aggregates.py` — continuous aggregate over denormalized per-currency amounts (income/expense/delta for UAH, USD, EUR). Real-time aggregation + refresh policy. **[Agent: postgres-database]**
- [x] Apply migrations (`upgrade head`). Verify all tables, indexes, RLS policies, hypertable, and continuous aggregate exist. Insert test rates + transactions, confirm aggregate correctness. **[Agent: postgres-database]**

---

## Slice 2: Shared models + Redpanda wire format + ID utils

- [x] Create `shared/src/grosh_shared/events.py` with `RawTransactionEvent` and `BackfillRequestEvent` Pydantic models. **[Agent: python-backend]**
- [x] Create `shared/src/grosh_shared/id_utils.py` with `generate_transaction_id(source, source_id) -> UUID` using UUID5. **[Agent: python-backend]**
- [x] Update `shared/src/grosh_shared/models.py` — update `Transaction` model, add `Account`, `BankIntegration` domain models, `TransactionOrigin` enum. **[Agent: python-backend]**
- [x] Create `services/ingestion/src/grosh_ingestion/clients/monobank.py` — httpx-based async client with `get_client_info`, `set_webhook`, `get_statements` methods. **[Agent: python-backend]**
- [x] Create `shared/src/grosh_shared/auth.py` — JWT decode/validate utility extracted from the API service, reusable by both API and ingestion service. **[Agent: python-backend]**
- [x] Verify — unit tests for event serialization round-trip, ID determinism (same input → same UUID), and Monobank client (mocked httpx responses). **[Agent: python-backend]**

---

## Slice 3: Ingestion service scaffold + Redpanda producer + webhook + currency rate cron

- [ ] Scaffold `services/ingestion/` — `pyproject.toml`, `Dockerfile`, `src/grosh_ingestion/main.py` (app factory with lifespan: asyncpg pool + confluent_kafka Producer), `deps.py` (DI composition root with JWT validation dependency). Follow existing API service patterns. **[Agent: python-backend]**
- [ ] Add ingestion service to `infra/docker-compose.yml` and `infra/docker-compose.dev.yml`. **[Agent: k8s-infra]**
- [ ] Create `services/ingestion/src/grosh_ingestion/adapters/monobank_adapter.py` — normalize Monobank payload to `RawTransactionEvent` (abs amounts, infer transaction_type from sign). **[Agent: python-backend]**
- [ ] Create `services/ingestion/src/grosh_ingestion/routers/webhook.py` — GET (verification) and POST (parse payload, lookup webhook_secret in `bank_integrations`, normalize via adapter, publish to Redpanda). **[Agent: python-backend]**
- [ ] Create currency rate ingestion cron job — poll Monobank `/bank/currency` endpoint on a schedule, compare with current rate, close previous SCD2 row and insert new one if changed. Also poll NBU daily rates as fallback source. **[Agent: python-backend]**
- [ ] Verify — start the stack with `make dev`, confirm ingestion service is healthy. `curl` the GET webhook endpoint (200 OK). `curl` POST with a mock Monobank payload, confirm message arrives on `raw_transactions` topic via `rpk topic consume`. Confirm currency rates are being populated. **[Agent: python-backend]**

---

## Slice 4: Transaction consumer — dedup + transfer detection + currency conversion + DB write

- [ ] Create `services/consumer/src/grosh_consumer/db.py` — asyncpg pool setup. **[Agent: python-backend]**
- [ ] Create `services/consumer/src/grosh_consumer/handlers/transaction_handler.py` — dedup via `ON CONFLICT DO NOTHING`, transfer detection via counterparty IBAN lookup (overrides transaction_type to 'transfer' if match), per-bank exchange rate lookup from `currency_rates` SCD2 table, compute denormalized `amount_uah_cents`/`amount_usd_cents`/`amount_eur_cents`. **[Agent: python-backend]**
- [ ] Create `services/consumer/src/grosh_consumer/consumer.py` — main consumer loop: poll, deserialize, dispatch to handler, manual offset commit. Replace placeholder `main.py`. **[Agent: python-backend]**
- [ ] Verify — publish a test event to `raw_transactions` via `rpk topic produce`, confirm it appears in the `transactions` table with correct denormalized amounts. Publish the same event again, confirm no duplicate. Insert two accounts with known IBANs, publish a transfer-like event, confirm `transaction_type = 'transfer'`. **[Agent: python-backend]**

---

## Slice 5: Account linking + manual entry (ingestion service)

- [ ] Create `services/ingestion/src/grosh_ingestion/repositories/account_repo.py` — write operations for `bank_integrations` and `accounts` tables, including `pgp_sym_encrypt` for token storage. **[Agent: python-backend]**
- [ ] Create `services/ingestion/src/grosh_ingestion/services/account_service.py` — orchestrates: call Monobank `get_client_info`, create integration (generate webhook_secret), create accounts, call `set_webhook`. **[Agent: python-backend]**
- [ ] Create `services/ingestion/src/grosh_ingestion/routers/accounts.py` — `POST /accounts/link-monobank`, `POST /accounts/manual`. Register router in `main.py`. **[Agent: python-backend]**
- [ ] Create `services/ingestion/src/grosh_ingestion/services/transaction_service.py` — manual entry: generate deterministic ID, build `RawTransactionEvent`, publish to Redpanda. **[Agent: python-backend]**
- [ ] Create `services/ingestion/src/grosh_ingestion/routers/transactions.py` — `POST /transactions/manual`. **[Agent: python-backend]**
- [ ] Verify — call `POST /accounts/link-monobank` with a real or mocked Monobank token, confirm integration + accounts created in DB, token is encrypted, webhook_secret is populated. Create a manual cash account. Post a manual transaction, confirm it flows through Redpanda → consumer → DB. **[Agent: python-backend]**

---

## Slice 6: Transaction query endpoints (main API)

- [ ] Create `services/api/src/grosh_api/repositories/account_repo.py` — read-only account listing. **[Agent: python-backend]**
- [ ] Create `services/api/src/grosh_api/repositories/transaction_repo.py` — paginated transaction query with filters (type, account, date range), monthly aggregate query against the continuous aggregate view. **[Agent: python-backend]**
- [ ] Create `services/api/src/grosh_api/routers/accounts.py` — `GET /accounts`. Register router in `main.py`. **[Agent: python-backend]**
- [ ] Create `services/api/src/grosh_api/routers/transactions.py` — `GET /transactions` and `GET /transactions/monthly-aggregate`. Register router in `main.py`. **[Agent: python-backend]**
- [ ] Verify — seed several transactions (income, expense, transfer) across 2+ months via the ingestion pipeline. Call `GET /transactions` with filters on the main API, confirm correct results. Call `GET /transactions/monthly-aggregate`, confirm all three currency views are correct and transfers excluded. Call `GET /accounts`, confirm accounts are listed. **[Agent: python-backend]**

---

## Slice 7: Backfill via K8s Job (ingestion service)

- [ ] Create `infra/k8s/backfill-job-template.yaml` — K8s Job manifest with ingestion service image, `--mode=backfill` entrypoint, resource limits, backoffLimit, ttl. **[Agent: k8s-infra]**
- [ ] Add backfill entrypoint to ingestion service — `--mode=backfill` flag reads from `backfill_requests` topic, paginates Monobank API using `grosh_ingestion.clients.monobank`, normalizes via adapter, publishes to `raw_transactions`, exits on completion. **[Agent: python-backend]**
- [ ] Create `services/ingestion/src/grosh_ingestion/services/backfill_service.py` — uses `kubernetes` Python client to create Job from template. **[Agent: python-backend]**
- [ ] Add `POST /accounts/{id}/backfill` to ingestion service accounts router. **[Agent: python-backend]**
- [ ] Set up RBAC — ServiceAccount + Role + RoleBinding for ingestion pod to create Jobs. **[Agent: k8s-infra]**
- [ ] Verify — trigger backfill via `curl` to the ingestion service, confirm K8s Job is created (`kubectl get jobs`), confirm historical transactions flow into DB. **[Agent: k8s-infra]**
