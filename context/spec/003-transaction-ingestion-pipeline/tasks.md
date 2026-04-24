# Tasks: Transaction Ingestion Pipeline

---

## Slice 1: Database foundation — extensions, tables, hypertable, aggregates, currency rates

- [x] Create migration `0004_transaction_pipeline.py` — enable `timescaledb` and `pgcrypto` extensions, create enums (`bank_source`, `account_type`, `transaction_type`, `transaction_source`, `transaction_origin`), create `bank_integrations`, `accounts`, `categories`, `transactions` tables with all columns per tech spec (including `amount_uah_cents`, `amount_usd_cents`, `amount_eur_cents`, `metadata JSONB`, `origin`, `related_transaction_id`). Convert `transactions` to hypertable. Create `currency_rates` SCD2 table. Add all indexes. Enable RLS + policies on all user-scoped tables. **[Agent: postgres-database]**
- [x] Create migration `0005_monthly_aggregates.py` — continuous aggregate over denormalized per-currency amounts (income/expense/delta for UAH, USD, EUR) plus per-currency NULL conversion counts (`null_uah_count`, `null_usd_count`, `null_eur_count`) for data quality visibility. Real-time aggregation + refresh policy. **[Agent: postgres-database]**
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
- [x] Create currency rate ingestion — per-source background loops via `RateProviderConfig` (source, fetch, interval_seconds, kind). Monobank (`RateKind.POLLED`, 300s) and NBU (`RateKind.HISTORICAL`, daily). Polled sources use `upsert_polled` (SCD2 with `last_polled_at` + `update_cadence_seconds`); historical sources use `upsert_historical` (slot-in SCD2, no polling metadata). `NormalizedRate` carries provider-authoritative `at_time` (Monobank Unix timestamp, NBU parsed `DD.MM.YYYY`). **[Agent: python-backend]**
- [x] Verify — start the stack with `make dev`, confirm ingestion service is healthy. `curl` the GET webhook endpoint (200 OK). `curl` POST with a mock Monobank payload, confirm message arrives on `raw_transactions` topic via `rpk topic consume`. Confirm currency rates are being populated. **[Agent: python-backend]**
- [x] Review fixes — `TransactionType.check` for zero-amount transactions, parameterized RLS set_config, extracted `CurrencyRateService` with dispatch by `RateKind`, webhook error handling (ValidationError + ValueError), Kafka delivery callback, typed repo returns (`IntegrationRef`/`AccountRef`), NUMERIC(18,8) for rates, adapter + webhook unit tests (14 new tests). **[Agent: python-backend]**

---

## Slice 4: Currency rate fallback chain + two-tier rate resolution

- [x] Create migration `0006_rate_source_config.py` — add `last_polled_at TIMESTAMPTZ` and `update_cadence_seconds INTEGER` to `currency_rates`, create `rate_source_config` table (source PK, fallback_source FK, base_currencies TEXT[]) with seed data (nbu→NULL `{UAH}`, monobank→nbu `{UAH}`). **[Agent: postgres-database]**
- [x] Split `repositories/currency_rate_repo.py` into `upsert_polled` (SCD2 anchored at `at_time`, bumps `last_polled_at`, carries `update_cadence_seconds`, `FOR UPDATE` locking) and `upsert_historical` (slot-in SCD2 with explicit `at_time` as `valid_from`, advisory lock, no polling metadata). **[Agent: python-backend]**
- [x] Add historical rate fetching to `banks/nbu/` — new `NbuHistoricalRate` model in `client.py` (different schema from daily: includes `units`, `rate_per_unit`, `enname`, `group`, `calcdate`). New `fetch_nbu_historical_rates(from_date, to_date, valcode)` in `client.py` using `https://bank.gov.ua/NBU_Exchange/exchange_site?start=YYYYMMDD&end=YYYYMMDD&valcode=CC&json`. New `fetch_historical_rates(from_date, to_date)` in `rates_provider.py` that loops all currencies (~45 requests) and normalizes to `list[NormalizedRate]`. Use `rate_per_unit` (not `rate`) for conversion. **[Agent: python-backend]**
- [x] Verify — apply migration, run rate loop twice, confirm `last_polled_at` updates on unchanged rates (no new rows). Call `fetch_historical_rates(date(2025, 1, 1), date(2025, 1, 31))`, confirm rates returned for the full range. **[Agent: python-backend]**
- [x] Implement unit and integration tests per `ingestion-currency-rate-test-suite.md` — provider unit tests (Monobank, NBU live, NBU historical), service dispatch tests, rate loop tests, repo integration tests (`upsert_polled`, `upsert_historical`), end-to-end service tests. Target ≥90% line / ≥85% branch coverage. **[Agent: python-backend]**

---

## Slice 5: Transaction consumer — dedup + transfer detection + currency conversion + DB write

- [x] Create `services/consumer/src/grosh_consumer/db.py` — asyncpg pool setup. **[Agent: python-backend]**
- [x] Create `services/consumer/src/grosh_consumer/handlers/transaction_handler.py` — dedup via `ON CONFLICT DO NOTHING`, transfer detection via counterparty IBAN lookup (overrides transaction_type to 'transfer' if match), hold→settlement linking (if `hold = false` and a hold row with same base ID exists, generate `:settled` ID and set `related_transaction_id`; otherwise insert with base ID), two-tier rate lookup with fallback chain (FRESH via `last_polled_at + K * update_cadence_seconds >= T`, CLOSEST by proximity within 7 days), compute denormalized `amount_uah_cents`/`amount_usd_cents`/`amount_eur_cents`, write path-based rate traceability per currency to transaction `metadata` (`{"rate_eur": {"path": [{"from": "USD", "to": "EUR", ...}], "effective_rate": "0.9030"}}`). **[Agent: python-backend]**
- [x] Create `services/consumer/src/grosh_consumer/consumer.py` — main consumer loop: poll, deserialize, dispatch to handler, manual offset commit. Replace placeholder `main.py`. **[Agent: python-backend]**
- [x] Verify — publish a test event to `raw_transactions` via `rpk topic produce`, confirm it appears in the `transactions` table with correct denormalized amounts and rate source metadata. Publish the same event again, confirm no duplicate. Insert two accounts with known IBANs, publish a transfer-like event, confirm `transaction_type = 'transfer'`. Test fallback: insert a stale monobank rate, confirm consumer uses NBU instead. Test hold→settlement: publish a hold event then a settlement with same source_id, confirm two rows with different IDs, settlement's `related_transaction_id` points to hold. **[Agent: python-backend]**
- [x] Implement unit tests per `consumer-currency-conversion-test-suite.md` sections 1-3 — shared fixtures, `InMemoryRateRepo`, pure function tests (`_pick_rate`, `_ordered_pivots`, `_ensure_tz`, `RateTier`/`RateSide`, `_RateStep.apply`, `_RatePath`), service resolution tests with in-memory repo (tier ordering, source priority, multi-hop, rate-side, no-path, metadata shape). **[Agent: python-backend]**
- [x] Implement integration tests per `consumer-currency-conversion-test-suite.md` sections 4-5 — real DB with rolled-back transactions. Repo tests (`find_fresh_rate`, `find_closest_rate`, `load_source_chain` including depth cap, cyclic chain, base_currencies). End-to-end service tests (passthrough, direct/reverse/2-hop, tier coherence, ROUND_HALF_UP, large amounts, naive timezone, `RateSourceChainError` propagation). Target ≥95% line / ≥90% branch coverage. **[Agent: python-backend]**

---

## Slice 6: Account linking + manual entry (ingestion service)

- [x] Add generic write operations — `repositories/integration_repo.py` (`create_integration()` takes config dict, no bank-specific columns) and `repositories/account_repo.py` (`create_account()` with source + integration_id params, `get_user_default_rate_source()`). **[Agent: python-backend]**
- [x] Create `sources/monobank/linking_service.py` — `MonobankLinkingService`: calls Monobank `get_client_info`, generates `webhook_secret`, encrypts token via `pgp_sym_encrypt` (direct SQL call), stores encrypted bytes as hex in `config.encrypted_token`, creates integration via `IntegrationRepo`, creates accounts via `AccountRepo`, calls `set_webhook`. **[Agent: python-backend]**
- [x] Create `sources/monobank/repo.py` — `MonobankRepo`: `get_active_integration_by_webhook_secret()` (queries `config->>'webhook_secret'`), `get_account_by_external_id()`. **[Agent: python-backend]**
- [x] Create `sources/monobank/router.py` — `POST /monobank/link` (JWT), `GET /monobank/webhook/{secret}`, `POST /monobank/webhook/{secret}`. Register router in `main.py`. **[Agent: python-backend]**
- [x] Create `sources/manual/service.py` — `ManualService`: account creation, manual transaction entry (generate deterministic ID, build `RawTransactionEvent`, publish to Redpanda). **[Agent: python-backend]**
- [x] Create `sources/manual/router.py` — `POST /manual/accounts`, `POST /manual/transactions`. Register router in `main.py`. **[Agent: python-backend]**
- [x] Create migration `0007_user_settings.py` — `user_settings` table with `user_id UUID PK FK→users`, `default_rate_source TEXT FK→rate_source_config NULL` (nullable, no default), `updated_at TIMESTAMPTZ DEFAULT now()`. Enable RLS on `user_id`. Create `AFTER INSERT ON users` trigger to auto-create a row with NULL `default_rate_source` for each new user. **[Agent: postgres-database]**
- [x] Add `rate_source: str | None = None` to `RawTransactionEvent`. Bank `transaction_adapter.py` sets it to the bank source (e.g. `"monobank"`). `ManualService` resolves it from the request or `user_settings.default_rate_source`. **[Agent: python-backend]**
- [x] Update consumer `CurrencyConversionService.convert()` — use `event.rate_source or event.source` for `load_source_chain()` (backwards compatibility with events already on the topic). **[Agent: python-backend]**
- [x] Verify — call `POST /monobank/link` with a real or mocked Monobank token, confirm integration + accounts created in DB, token is encrypted, `config->>'webhook_secret'` is populated. Create a manual cash account via `POST /manual/accounts`. Post a manual transaction with explicit `rate_source`, confirm correct chain used. Post without `rate_source`, confirm `default_rate_source` from `user_settings` is used. Confirm bank webhook transactions still work (adapter sets `rate_source`). **[Agent: python-backend]**

---

## Slice 7: Transaction query endpoints (main API)

- [x] Create `services/api/src/grosh_api/repositories/account_repo.py` — read-only account listing. **[Agent: python-backend]**
- [x] Create `services/api/src/grosh_api/repositories/transaction_repo.py` — paginated transaction query with filters (type, account, date range), monthly aggregate query against the continuous aggregate view. **[Agent: python-backend]**
- [x] Create `services/api/src/grosh_api/routers/accounts.py` — `GET /accounts`. Register router in `main.py`. **[Agent: python-backend]**
- [x] Create `services/api/src/grosh_api/routers/transactions.py` — `GET /transactions` and `GET /transactions/monthly-aggregate`. Register router in `main.py`. **[Agent: python-backend]**
- [x] Verify — seed several transactions (income, expense, transfer) across 2+ months via the ingestion pipeline. Call `GET /transactions` with filters on the main API, confirm correct results. Call `GET /transactions/monthly-aggregate`, confirm all three currency views are correct and transfers excluded. Call `GET /accounts`, confirm accounts are listed. Clean up seeded test data from the DB after verification. **[Agent: python-backend]**

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

---

## Slice 9: Database role separation — enforce RLS via dedicated app role

- [ ] Create migration `0008_app_role.py` — create `grosh_app` role with `LOGIN`, grant `CONNECT`, `USAGE ON SCHEMA public`, `SELECT/INSERT/UPDATE/DELETE ON ALL TABLES`, `USAGE/SELECT ON ALL SEQUENCES`, `ALTER DEFAULT PRIVILEGES` for future objects created by `grosh_admin`. Password sourced from `GROSH_APP_DB_PASSWORD` env var. **[Agent: postgres-database]**
- [ ] Add `GROSH_APP_DB_PASSWORD` to `infra/.env` and update `DATABASE_URL` to construct two DSNs: `DATABASE_URL` for app services (uses `grosh_app`), `DATABASE_URL_ADMIN` for migrations and consumer (uses `grosh_admin`). **[Agent: k8s-infra]**
- [ ] Update API service — use `DATABASE_URL` (grosh_app role). Verify `get_current_user` dependency still calls `set_config` before queries. **[Agent: python-backend]**
- [ ] Update ingestion service — use `DATABASE_URL` (grosh_app role). Verify `get_current_user_id` dependency still calls `set_config`. Currency rate writes (global tables, no RLS) must still work under `grosh_app`. **[Agent: python-backend]**
- [ ] Update consumer service — use `DATABASE_URL_ADMIN` (grosh_admin role, bypasses RLS). No `set_config` needed. **[Agent: python-backend]**
- [ ] Update Alembic `env.py` — use `DATABASE_URL_ADMIN` for migrations (needs table owner for DDL). **[Agent: python-backend]**
- [ ] Add integration test — connect as `grosh_app`, query `accounts` without `set_config`, assert zero rows returned. Call `set_config('app.current_user_id', '<user-uuid>', true)`, assert only that user's rows returned. Connect as `grosh_admin`, assert all rows visible without `set_config`. **[Agent: python-backend]**
- [ ] Verify — run full test suite. Start the stack with `make dev`, confirm API and ingestion services connect as `grosh_app` (check `pg_stat_activity`), consumer connects as `grosh_admin`. Confirm webhook → consumer → DB pipeline still works end-to-end. **[Agent: python-backend]**
