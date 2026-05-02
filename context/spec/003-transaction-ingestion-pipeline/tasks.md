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
- [x] Create `services/consumer/src/grosh_consumer/handlers/transaction_handler.py` — dedup via `ON CONFLICT DO NOTHING`, transfer detection via counterparty IBAN lookup (overrides transaction_type to 'transfer' if match), two-tier rate lookup with fallback chain (FRESH via `last_polled_at + K * update_cadence_seconds >= T`, CLOSEST by proximity within 7 days), compute denormalized `amount_uah_cents`/`amount_usd_cents`/`amount_eur_cents`, write path-based rate traceability per currency to transaction `metadata` (`{"rate_eur": {"path": [{"from": "USD", "to": "EUR", ...}], "effective_rate": "0.9030"}}`). Hold flag is stored but not used for linking or filtering (Monobank historical API returns unreliable values). **[Agent: python-backend]**
- [x] Create `services/consumer/src/grosh_consumer/consumer.py` — main consumer loop: poll, deserialize, dispatch to handler, manual offset commit. Replace placeholder `main.py`. **[Agent: python-backend]**
- [x] Verify — publish a test event to `raw_transactions` via `rpk topic produce`, confirm it appears in the `transactions` table with correct denormalized amounts and rate source metadata. Publish the same event again, confirm no duplicate. Insert two accounts with known IBANs, publish a transfer-like event, confirm `transaction_type = 'transfer'`. Test fallback: insert a stale monobank rate, confirm consumer uses NBU instead. **[Agent: python-backend]**
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
- [x] **Schema bugfix (post-slice)** — three corrections applied to the `transactions` table and wire format: (1) `operation_currency_code TEXT NULL` added — the merchant/operation currency for cross-currency purchases (e.g. `EUR`); NULL for domestic transactions. (2) `transaction_type` column renamed to `raw_transaction_type` — the immutable sign-based classification from the adapter. (3) New `transaction_type transaction_type NOT NULL` column added — the consumer-enriched classification, copied from `raw_transaction_type` by default and upgraded to `'transfer'` on detected internal transfers. Currency semantics clarified: `currency_code` = account base currency (resolved by consumer from DB); `amount_cents` = amount in the account's base currency. `RawTransactionEvent.currency_code` renamed to `operation_currency_code` on the wire. **[Agent: python-backend]**

---

## Slice 8: Backfill via K8s Jobs (transactions + rates)

- [x] Create `infra/k8s/transactions-backfill-job-template.yaml` — K8s Job manifest with ingestion service image, env var–based parameters, resource limits, backoffLimit, ttl. **[Agent: k8s-infra]**
- [x] Create `infra/k8s/rates-backfill-job-template.yaml` — K8s Job manifest for historical rate backfill. **[Agent: k8s-infra]**
- [x] Extend `models.py` with backfill interfaces — `HistoricalRateProvider` type alias + `fetch_historical` field on `RateProviderConfig`, `TransactionBackfillProvider` protocol. Add repo methods: `UserRepo.get_role()`, `MonobankRepo.decrypt_token()`, `IntegrationRepo.get_bank_source()`. **[Agent: python-backend]**
- [x] Create `sources/monobank/backfill.py` — `MonobankBackfillProvider` implementing `TransactionBackfillProvider`. Encapsulates token decryption (via `MonobankRepo`), account resolution, Monobank client, 31-day chunked pagination with 61s rate-limit pause, adapter normalization, Redpanda publish. No raw SQL. **[Agent: python-backend]**
- [x] Create `registry.py` — `RATE_PROVIDERS` list (with `fetch_historical` on NBU config) and `TRANSACTION_BACKFILL_PROVIDERS` dict. Imported by `main.py` and both backfill scripts. **[Agent: python-backend]**
- [x] Create source-agnostic backfill entrypoints — `jobs/run_transactions_backfill.py` reads params from env vars, resolves bank source via `IntegrationRepo.get_bank_source()`, dispatches to `TRANSACTION_BACKFILL_PROVIDERS`. `jobs/run_rates_backfill.py` looks up `RateProviderConfig` from registry, calls `config.fetch_historical()`. No source names in either file. **[Agent: python-backend]**
- [x] Create `services/backfill_service.py` — generic K8s Job creation via `kubernetes` Python client. `trigger_transactions_backfill` passes all params as env vars (no Redpanda topic). `trigger_rates_backfill` same pattern. **[Agent: python-backend]**
- [x] Add `POST /monobank/accounts/{account_id}/backfill` to Monobank router (transactions backfill trigger). **[Agent: python-backend]**
- [x] Create `routers/admin.py` — `POST /admin/rates-backfill` triggers rates backfill K8s Job. Admin role check via `UserRepo.get_role()`. Source validation via registry lookup. **[Agent: python-backend]**
- [x] Set up RBAC — ServiceAccount + Role + RoleBinding for ingestion pod to create Jobs. **[Agent: k8s-infra]**
- [x] Update `main.py` — import `RATE_PROVIDERS` from `registry`, admin router from `routers.admin`. Remove `BackfillRequestEvent` and `Topic.backfill_requests` from shared package. **[Agent: python-backend]**
- [x] Create `infra/grosh.postman_collection.json` — Postman collection covering all API and ingestion endpoints. Auto-token management via Login test script. **[Agent: python-backend]**
- [x] Create `services/api/src/grosh_api/repositories/rate_repo.py` — read-only rate queries: cursor-paginated list with filters (source, currency_from, currency_to, date range) and point-in-time SCD2 lookup. **[Agent: python-backend]**
- [x] Create `services/api/src/grosh_api/routers/rates.py` — `GET /rates` (paginated, filtered) and `GET /rates/at` (point-in-time rates). Register router in `main.py`. Update Postman collection. **[Agent: python-backend]**
- [x] Create `POST /monobank/relink` — re-registers webhook and updates token on existing integration without recreating accounts. **[Agent: python-backend]**
- [x] Create `WebhookReregistrationProvider` protocol and `WEBHOOK_REREGISTRATION_PROVIDERS` registry. Monobank implements via `MonobankLinkingService.reregister_webhooks()`. **[Agent: python-backend]**
- [x] Create `jobs/reregister_webhooks.py` — iterates all providers in registry. Shell entrypoints: `scripts/reregister-webhooks/dev.sh` (docker compose exec) and `scripts/reregister-webhooks/prod.sh` (kubectl exec). Makefile target: `make dev-reregister-webhooks`. **[Agent: python-backend]**
- [x] Verify — trigger transactions backfill from Monobank router, confirm K8s Job created with env vars. Trigger rates backfill via admin endpoint for NBU. Call `GET /rates/at` to confirm rate counts. Confirm no source names in generic files. **[Agent: python-backend]**

---

## Slice 9: Per-service database roles + account endpoint migration

**Design:** Four roles — `grosh_admin` (owner, migrations only), `grosh_api`, `grosh_ingestion`, `grosh_consumer`. All three app roles get `SELECT ON ALL TABLES`. Write privileges are table-specific: no table has write access from more than one service. Account management endpoints (PUT, DELETE) move from the API service to the ingestion service so `accounts` writes are owned entirely by ingestion.

- [x] Create migration `0008_app_roles.py` — create `grosh_api`, `grosh_ingestion`, `grosh_consumer` roles with `LOGIN`. `grosh_consumer` additionally gets `BYPASSRLS`. Grant `CONNECT`, `USAGE ON SCHEMA public`, `SELECT ON ALL TABLES`, `USAGE/SELECT ON ALL SEQUENCES` to all three. Table-specific writes: `grosh_api` gets INSERT+UPDATE on `users`, `user_settings`; INSERT+DELETE on `refresh_tokens`. `grosh_ingestion` gets INSERT+UPDATE on `accounts`, `bank_integrations`, `currency_rates`. `grosh_consumer` gets INSERT on `transactions`. `ALTER DEFAULT PRIVILEGES FOR ROLE grosh_admin` grants SELECT + sequence usage to all three. Passwords from env vars `GROSH_API_DB_PASSWORD`, `GROSH_INGESTION_DB_PASSWORD`, `GROSH_CONSUMER_DB_PASSWORD`. **[Agent: postgres-database]**
- [x] Update `infra/.env` and `infra/docker-compose.yml` — add three password env vars. Each service gets its own `DATABASE_URL` using its role. Add `DATABASE_URL_ADMIN` for Alembic. **[Agent: k8s-infra]**
- [x] Move account write endpoints to ingestion — move `PUT /accounts/{id}` (rename) and `DELETE /accounts/{id}` (soft-delete) from `services/api/src/grosh_api/routers/accounts.py` to `services/ingestion/src/grosh_ingestion/sources/manual/router.py` as `PUT /manual/accounts/{id}` and `DELETE /manual/accounts/{id}`. Move `update_name()` and `soft_delete()` repo methods from API's `account_repo.py` to ingestion's `account_repo.py`. Remove write methods and endpoints from API service. **[Agent: python-backend]**
- [x] Update API service — use `DATABASE_URL` with `grosh_api` role. Remove `UpdateAccountRequest` schema and write-related imports from accounts router. Verify `get_current_user` dependency still calls `set_config`. **[Agent: python-backend]**
- [x] Update ingestion service — use `DATABASE_URL` with `grosh_ingestion` role. Verify `set_config` is called. Currency rate writes (global tables, no RLS) must still work. **[Agent: python-backend]**
- [x] Update consumer service — use `DATABASE_URL` with `grosh_consumer` role (has `BYPASSRLS`, so RLS policies don't apply). No `set_config` needed. **[Agent: python-backend]**
- [x] Update Alembic `env.py` — use `DATABASE_URL_ADMIN` for migrations. **[Agent: python-backend]**
- [x] Update Postman collection — move account PUT/DELETE requests from API folder to Ingestion folder with updated paths. **[Agent: python-backend]**
- [x] Verify — start the stack with `make dev`. Check `pg_stat_activity`: API connects as `grosh_api`, ingestion as `grosh_ingestion`, consumer as `grosh_consumer`. Test: API cannot INSERT into `transactions` (permission denied). Consumer can read all accounts without `set_config`. Ingestion can write accounts but not transactions. Full webhook → consumer → DB pipeline works. Account rename and soft-delete work via ingestion endpoints. **[Agent: python-backend]**

---

## Slice 10: Access token revocation (instant logout)

**Design:** `revoked_tokens` TimescaleDB hypertable with 1-hour retention policy. On logout, the access token's `jti` is inserted. The `get_current_user` dependency checks revocation before proceeding. TimescaleDB auto-drops expired entries.

- [x] Create migration `0009_revoked_tokens.py` — create `revoked_tokens` table (`jti UUID NOT NULL`, `expires_at TIMESTAMPTZ NOT NULL`), convert to hypertable on `expires_at`, add retention policy (1 hour). Grant INSERT, DELETE to `grosh_api`. **[Agent: postgres-database]**
- [x] Add `RevokedTokenRepo` to API service — `insert(conn, jti, expires_at)` and `is_revoked(conn, jti) -> bool`. **[Agent: python-backend]**
- [x] Update `get_current_user` dependency — after decoding JWT, call `repo.is_revoked(conn, jti)`. If revoked, raise 401. Extract `jti` from the JWT payload (already present as a standard claim). **[Agent: python-backend]**
- [x] Update `AuthService.logout()` — in addition to deleting the refresh token, insert the access token's `jti` + `exp` into `revoked_tokens`. The access token must be passed (from `Authorization` header) so its `jti` can be extracted. **[Agent: python-backend]**
- [x] Update `AuthService.logout_all()` — revoke the current access token (same as above). All refresh tokens are already deleted. **[Agent: python-backend]**
- [x] Update logout router endpoints — pass the access token (from request header) to the service so it can extract `jti`. **[Agent: python-backend]**
- [ ] Verify — login, hit an authenticated endpoint (works), logout, hit the same endpoint again (401). Refresh after logout also fails (refresh token deleted). Wait 15+ minutes after logout, confirm `revoked_tokens` row is cleaned up by retention policy. **[Agent: python-backend]**
