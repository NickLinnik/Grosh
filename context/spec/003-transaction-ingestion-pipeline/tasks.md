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
- [x] Create Monobank HTTP client — httpx-based async client with `get_client_info`, `set_webhook`, `get_statements` methods. Now at `banks/monobank/client.py`. **[Agent: python-backend]**
- [x] Create `shared/src/grosh_shared/auth.py` — JWT decode/validate utility extracted from the API service, reusable by both API and ingestion service. **[Agent: python-backend]**
- [x] Verify — unit tests for event serialization round-trip, ID determinism (same input → same UUID), and Monobank client (mocked httpx responses). **[Agent: python-backend]**

---

## Slice 3: Ingestion service scaffold + Redpanda producer + webhook + currency rate cron

- [x] Scaffold `services/ingestion/` — `pyproject.toml`, `Dockerfile`, `src/grosh_ingestion/main.py` (app factory with lifespan: asyncpg pool + confluent_kafka Producer), `deps.py` (DI composition root with JWT validation dependency). Follow existing API service patterns. **[Agent: python-backend]**
- [x] Add ingestion service to `infra/docker-compose.yml` and `infra/docker-compose.dev.yml`. **[Agent: k8s-infra]**
- [x] Create `banks/monobank/adapter.py` — normalize Monobank payload to `RawTransactionEvent` (abs amounts, infer transaction_type from sign). **[Agent: python-backend]**
- [x] Create `banks/monobank/webhook.py` — GET (verification) and POST (parse payload, lookup webhook_secret in `bank_integrations`, normalize via adapter, publish to Redpanda). **[Agent: python-backend]**
- [x] Create `repositories/account_repo.py` — covers both `bank_integrations` and `accounts` tables (single aggregate root). Read operations: lookup integration by webhook_secret, lookup account by external_id + integration_id. Also create `user_repo.py` with `is_active` check. Refactor `deps.py` and `banks/monobank/webhook.py` to use repos instead of raw SQL. **[Agent: python-backend]**
- [x] Create currency rate ingestion cron job — poll Monobank `/bank/currency` endpoint on a schedule, compare with current rate, close previous SCD2 row and insert new one if changed. Also poll NBU daily rates as fallback source. **[Agent: python-backend]**
- [x] Verify — start the stack with `make dev`, confirm ingestion service is healthy. `curl` the GET webhook endpoint (200 OK). `curl` POST with a mock Monobank payload, confirm message arrives on `raw_transactions` topic via `rpk topic consume`. Confirm currency rates are being populated. **[Agent: python-backend]**
- [x] Review fixes — `TransactionType.check` for zero-amount transactions, parameterized RLS set_config, extracted `CurrencyRateService` for testability, env-configurable `RATE_POLL_INTERVAL_SECONDS`, webhook error handling (ValidationError + ValueError), Kafka delivery callback, typed repo returns (`IntegrationRef`/`AccountRef`), NUMERIC(18,8) for rates, adapter + webhook unit tests (14 new tests). **[Agent: python-backend]**

---

## Slice 4: Currency rate fallback chain + stale rate detection

- [ ] Create migration `0006_rate_source_config.py` — add `last_polled_at TIMESTAMPTZ NOT NULL DEFAULT now()` to `currency_rates`, backfill existing rows (`SET last_polled_at = valid_from`), create `rate_source_config` table with seed data (monobank→nbu 7200s, nbu→NULL 90000s). **[Agent: postgres-database]**
- [ ] Update `repositories/currency_rate_repo.py` — modify `upsert()` to update `last_polled_at = now()` when rate is unchanged (no new row), set `last_polled_at = now()` on new inserts. **[Agent: python-backend]**
- [ ] Add historical rate fetching to `banks/nbu/` — new `NbuHistoricalRate` model in `client.py` (different schema from daily: includes `units`, `rate_per_unit`, `enname`, `group`, `calcdate`). New `fetch_nbu_historical_rates(from_date, to_date, valcode)` in `client.py` using `https://bank.gov.ua/NBU_Exchange/exchange_site?start=YYYYMMDD&end=YYYYMMDD&valcode=CC&json`. New `fetch_historical_rates(from_date, to_date)` in `rates_provider.py` that loops all currencies (~45 requests) and normalizes to `list[NormalizedRate]`. Use `rate_per_unit` (not `rate`) for conversion. **[Agent: python-backend]**
- [ ] Verify — apply migration, run rate loop twice, confirm `last_polled_at` updates on unchanged rates (no new rows). Call `fetch_historical_rates(date(2025, 1, 1), date(2025, 1, 31))`, confirm rates returned for the full range. **[Agent: python-backend]**

---

## Slice 5: Transaction consumer — dedup + transfer detection + currency conversion + DB write

- [ ] Create `services/consumer/src/grosh_consumer/db.py` — asyncpg pool setup. **[Agent: python-backend]**
- [ ] Create `services/consumer/src/grosh_consumer/handlers/transaction_handler.py` — dedup via `ON CONFLICT DO NOTHING`, transfer detection via counterparty IBAN lookup (overrides transaction_type to 'transfer' if match), hold→settlement linking (if `hold = false` and a hold row with same base ID exists, generate `:settled` ID and set `related_transaction_id`; otherwise insert with base ID), rate lookup with fallback chain (check `last_polled_at` staleness against `rate_source_config.max_staleness_seconds`, fall back to `fallback_source`), compute denormalized `amount_uah_cents`/`amount_usd_cents`/`amount_eur_cents`, write full rate traceability per currency to transaction `metadata` (`{"rate_uah": {"source": "monobank", "rate_id": 42, "value": "43.4997"}, ...}`). **[Agent: python-backend]**
- [ ] Create `services/consumer/src/grosh_consumer/consumer.py` — main consumer loop: poll, deserialize, dispatch to handler, manual offset commit. Replace placeholder `main.py`. **[Agent: python-backend]**
- [ ] Verify — publish a test event to `raw_transactions` via `rpk topic produce`, confirm it appears in the `transactions` table with correct denormalized amounts and rate source metadata. Publish the same event again, confirm no duplicate. Insert two accounts with known IBANs, publish a transfer-like event, confirm `transaction_type = 'transfer'`. Test fallback: insert a stale monobank rate, confirm consumer uses NBU instead. Test hold→settlement: publish a hold event then a settlement with same source_id, confirm two rows with different IDs, settlement's `related_transaction_id` points to hold. **[Agent: python-backend]**

---

## Slice 6: Account linking + manual entry (ingestion service)

- [ ] Add write operations to `repositories/account_repo.py` — create integration (`pgp_sym_encrypt` for token), create accounts. **[Agent: python-backend]**
- [ ] Create `services/ingestion/src/grosh_ingestion/services/account_service.py` — orchestrates: call Monobank `get_client_info`, create integration (generate webhook_secret), create accounts, call `set_webhook`. **[Agent: python-backend]**
- [ ] Create `services/ingestion/src/grosh_ingestion/routers/accounts.py` — `POST /accounts/link-monobank`, `POST /accounts/manual`. Register router in `main.py`. **[Agent: python-backend]**
- [ ] Create `services/ingestion/src/grosh_ingestion/services/transaction_service.py` — manual entry: generate deterministic ID, build `RawTransactionEvent`, publish to Redpanda. **[Agent: python-backend]**
- [ ] Create `services/ingestion/src/grosh_ingestion/routers/transactions.py` — `POST /transactions/manual`. **[Agent: python-backend]**
- [ ] Verify — call `POST /accounts/link-monobank` with a real or mocked Monobank token, confirm integration + accounts created in DB, token is encrypted, webhook_secret is populated. Create a manual cash account. Post a manual transaction, confirm it flows through Redpanda → consumer → DB. **[Agent: python-backend]**

---

## Slice 7: Transaction query endpoints (main API)

- [ ] Create `services/api/src/grosh_api/repositories/account_repo.py` — read-only account listing. **[Agent: python-backend]**
- [ ] Create `services/api/src/grosh_api/repositories/transaction_repo.py` — paginated transaction query with filters (type, account, date range), monthly aggregate query against the continuous aggregate view. **[Agent: python-backend]**
- [ ] Create `services/api/src/grosh_api/routers/accounts.py` — `GET /accounts`. Register router in `main.py`. **[Agent: python-backend]**
- [ ] Create `services/api/src/grosh_api/routers/transactions.py` — `GET /transactions` and `GET /transactions/monthly-aggregate`. Register router in `main.py`. **[Agent: python-backend]**
- [ ] Verify — seed several transactions (income, expense, transfer) across 2+ months via the ingestion pipeline. Call `GET /transactions` with filters on the main API, confirm correct results. Call `GET /transactions/monthly-aggregate`, confirm all three currency views are correct and transfers excluded. Call `GET /accounts`, confirm accounts are listed. **[Agent: python-backend]**

---

## Slice 8: Backfill via K8s Jobs (transactions + rates)

- [ ] Create `infra/k8s/backfill-job-template.yaml` — K8s Job manifest with ingestion service image, `--mode=backfill` entrypoint, resource limits, backoffLimit, ttl. **[Agent: k8s-infra]**
- [ ] Create `infra/k8s/rate-backfill-job-template.yaml` — K8s Job manifest for historical rate backfill, `--mode=backfill-rates` entrypoint. **[Agent: k8s-infra]**
- [ ] Add transaction backfill entrypoint — `--mode=backfill` reads from `backfill_requests` topic, paginates Monobank API using `banks/monobank/client.py`, normalizes via adapter, publishes to `raw_transactions`, exits on completion. **[Agent: python-backend]**
- [ ] Add rate backfill entrypoint — `--mode=backfill-rates` accepts `source`, `from_date`, `to_date` args, calls `fetch_historical_rates()` for the given source, upserts via `CurrencyRateRepo`. **[Agent: python-backend]**
- [ ] Create `services/ingestion/src/grosh_ingestion/services/backfill_service.py` — uses `kubernetes` Python client to create Jobs from templates. Supports both transaction and rate backfill. **[Agent: python-backend]**
- [ ] Add `POST /accounts/{id}/backfill` to ingestion service accounts router (transaction backfill). **[Agent: python-backend]**
- [ ] Add `POST /admin/backfill-rates` to ingestion service admin router — triggers rate backfill K8s Job with `{ source, from_date, to_date }`. Requires admin role. **[Agent: python-backend]**
- [ ] Set up RBAC — ServiceAccount + Role + RoleBinding for ingestion pod to create Jobs. **[Agent: k8s-infra]**
- [ ] Verify — trigger transaction backfill, confirm K8s Job created and historical transactions flow into DB. Trigger rate backfill for NBU 2024-01-01 to 2025-12-31, confirm ~33k rate rows inserted. **[Agent: k8s-infra]**
