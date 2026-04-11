# Tasks: Transaction Ingestion Pipeline

---

## Slice 1: Database foundation — extensions, tables, hypertable

- [ ] Create migration `0004_transaction_pipeline.py` — enable `timescaledb` and `pgcrypto` extensions, create `bank_source`, `account_type`, `transaction_type`, `transaction_source` enums, create `bank_integrations`, `accounts`, `categories`, `transactions` tables with all columns per tech spec. Convert `transactions` to hypertable on `time`. Add all indexes. Enable RLS + policies on all new tables. **[Agent: postgres-database]**
- [ ] Verify — run `alembic upgrade head`, confirm all tables/indexes/policies exist via `psql` queries. Confirm app starts without errors. **[Agent: postgres-database]**

---

## Slice 2: Continuous aggregates for monthly rollups

- [ ] Create migration `0005_monthly_aggregates.py` — create `monthly_aggregates` continuous aggregate with real-time aggregation policy and refresh policy per tech spec. **[Agent: postgres-database]**
- [ ] Verify — insert test transactions via `psql`, query `monthly_aggregates` view, confirm income/expense/delta are correct and transfers are excluded. **[Agent: postgres-database]**

---

## Slice 3: Shared models + Redpanda wire format + ID utils

- [ ] Create `shared/src/grosh_shared/events.py` with `RawTransactionEvent` and `BackfillRequestEvent` Pydantic models. **[Agent: python-backend]**
- [ ] Create `shared/src/grosh_shared/id_utils.py` with `generate_transaction_id(source, source_id) -> UUID` using UUID5. **[Agent: python-backend]**
- [ ] Update `shared/src/grosh_shared/models.py` — update `Transaction` model, add `Account` and `BankIntegration` domain models. **[Agent: python-backend]**
- [ ] Create `shared/src/grosh_shared/monobank_client.py` — httpx-based async client with `get_client_info`, `set_webhook`, `get_statements` methods. Add `httpx` to shared pyproject.toml. **[Agent: python-backend]**
- [ ] Verify — unit tests for event serialization round-trip, ID determinism (same input → same UUID), and Monobank client (mocked httpx responses). **[Agent: python-backend]**

---

## Slice 4: Redpanda producer in API + webhook endpoint

- [ ] Initialize `confluent_kafka.Producer` in API lifespan. Add Redpanda topic creation (auto or startup script). **[Agent: python-backend]**
- [ ] Create `services/api/src/grosh_api/routers/webhook.py` — GET (verification) and POST (parse payload, lookup webhook_secret in `bank_integrations`, publish `RawTransactionEvent` to Redpanda). **[Agent: python-backend]**
- [ ] Verify — `curl` the GET endpoint (200 OK). `curl` POST with a mock Monobank payload, confirm message arrives on `raw_transactions` topic via `rpk topic consume`. **[Agent: python-backend]**

---

## Slice 5: Transaction consumer — dedup + transfer detection + DB write

- [ ] Create `services/consumer/src/grosh_consumer/db.py` — asyncpg pool setup. **[Agent: python-backend]**
- [ ] Create `services/consumer/src/grosh_consumer/handlers/transaction_handler.py` — dedup via `ON CONFLICT DO NOTHING`, transfer detection via counterparty IBAN lookup, transaction type classification. **[Agent: python-backend]**
- [ ] Create `services/consumer/src/grosh_consumer/consumer.py` — main consumer loop: poll, deserialize, dispatch to handler, manual offset commit. Replace placeholder `main.py`. **[Agent: python-backend]**
- [ ] Verify — publish a test event to `raw_transactions` via `rpk topic produce`, confirm it appears in the `transactions` table. Publish the same event again, confirm no duplicate. Insert two accounts with known IBANs, publish a transfer-like event, confirm `transaction_type = 'transfer'`. **[Agent: python-backend]**

---

## Slice 6: Account linking — Monobank integration

- [ ] Create `services/api/src/grosh_api/repositories/account_repo.py` — CRUD for `bank_integrations` and `accounts` tables, including `pgp_sym_encrypt` for token storage. **[Agent: python-backend]**
- [ ] Create `services/api/src/grosh_api/services/account_service.py` — orchestrates: call Monobank `get_client_info`, create integration (generate webhook_secret), create accounts, call `set_webhook`. **[Agent: python-backend]**
- [ ] Create `services/api/src/grosh_api/routers/accounts.py` — `POST /accounts/link-monobank`, `GET /accounts`, `POST /accounts/manual`. Register router in `main.py`. **[Agent: python-backend]**
- [ ] Verify — call `POST /accounts/link-monobank` with a real or mocked Monobank token, confirm integration + accounts created in DB, token is encrypted, webhook_secret is populated. Call `GET /accounts`, confirm the accounts are returned. Create a manual cash account, confirm it appears. **[Agent: python-backend]**

---

## Slice 7: Manual transaction entry

- [ ] Create `services/api/src/grosh_api/services/transaction_service.py` — manual entry: generate deterministic ID, build `RawTransactionEvent`, publish to Redpanda. **[Agent: python-backend]**
- [ ] Add `POST /transactions/manual` to `services/api/src/grosh_api/routers/transactions.py`. **[Agent: python-backend]**
- [ ] Verify — create a cash account, post a manual transaction via `curl`, confirm it flows through Redpanda → consumer → DB. **[Agent: python-backend]**

---

## Slice 8: Transaction query endpoints

- [ ] Create `services/api/src/grosh_api/repositories/transaction_repo.py` — paginated transaction query with filters (type, account, date range), monthly aggregate query against the continuous aggregate view. **[Agent: python-backend]**
- [ ] Add `GET /transactions` and `GET /transactions/monthly-aggregate` to `transactions.py` router. **[Agent: python-backend]**
- [ ] Verify — seed several transactions (income, expense, transfer) across 2+ months via the pipeline. Call `GET /transactions` with filters, confirm correct results. Call `GET /transactions/monthly-aggregate?currency=UAH`, confirm aggregated totals exclude transfers. **[Agent: python-backend]**

---

## Slice 9: Backfill via K8s Job

- [ ] Create `infra/k8s/backfill-job-template.yaml` — K8s Job manifest with consumer image, `--mode=backfill` entrypoint, resource limits, backoffLimit, ttl. **[Agent: k8s-infra]**
- [ ] Add backfill entrypoint to consumer service — `--mode=backfill` flag reads from `backfill_requests` topic, paginates Monobank API, publishes to `raw_transactions`, exits on completion. **[Agent: python-backend]**
- [ ] Create `services/api/src/grosh_api/services/backfill_service.py` — uses `kubernetes` Python client to create Job from template. Add `kubernetes` to API dependencies. **[Agent: python-backend]**
- [ ] Add `POST /accounts/{id}/backfill` to accounts router. **[Agent: python-backend]**
- [ ] Set up RBAC — ServiceAccount + Role + RoleBinding for API pod to create Jobs. **[Agent: k8s-infra]**
- [ ] Verify — trigger backfill via `curl`, confirm K8s Job is created (`kubectl get jobs`), confirm historical transactions flow into DB. **[Agent: k8s-infra]**
