# Currency Rate Ingestion — Test Specification

Implementation instructions for unit and integration tests covering
the ingestion subsystem: `currency_rate_repo.py`,
`currency_rate_service.py`, the rate providers, and the lifespan
plumbing.

Reference `ingestion-currency-rate-guide.md` for behavioral context.
This document is authoritative for test structure and assertions.

## 1. Test infrastructure

### 1.1 File layout

```
services/ingestion/tests/
  conftest.py                             # DB pool, event builders
  unit/
    conftest.py                           # fake fetchers, fixture helpers
    test_monobank_rates_provider.py
    test_nbu_rates_provider.py
    test_nbu_historical_rates_provider.py
    test_service_dispatch.py
    test_rate_loop.py
  integration/
    test_repo_upsert_polled.py
    test_repo_upsert_historical.py
    test_service_end_to_end.py
```

Providers are unit-tested against fake HTTP clients. The repo is
integration-tested against real Postgres. The service is unit-tested
for dispatch logic (with a mock repo) and integration-tested end-to-end
(real repo, real DB).

### 1.2 Shared fixtures (`tests/conftest.py`)

Mirror the existing `grosh-api` pattern:

- `db_pool` — session-scoped `asyncpg.Pool`.
- `conn` — function-scoped connection + rolled-back transaction.

Module-specific:

- `rate_repo` — `CurrencyRateRepo()`.
- `rate_service` — `CurrencyRateService(rate_repo)`.
- `make_rate(source='monobank', currency_from='USD', currency_to='UAH', rate_mid=40, rate_buy=None, rate_sell=None, at_time=T)` — builds `NormalizedRate` with defaults.
- `insert_rate_row(conn, *, source, currency_from, currency_to, rate_mid, rate_buy=None, rate_sell=None, valid_from, valid_to=None, last_polled_at=None, update_cadence_seconds=None)` — direct INSERT for seeding state before a test.
- `select_rows(conn, *, source, currency_from, currency_to, kind=None)` — helper returning all rows for a pair (optionally filtered to polled or historical) ordered by `valid_from ASC`.

### 1.3 Time discipline

Default test time: `T = datetime(2025, 6, 1, 12, 0, tzinfo=UTC)`.

Tests that care about relative times use:

- `T_minus_1m = T - timedelta(minutes=1)`
- `T_minus_1d = T - timedelta(days=1)`
- `T_plus_1d = T + timedelta(days=1)`

All datetimes are timezone-aware. Naive datetimes should never appear
in ingestion code paths — provider parsers produce UTC-aware; tests
assert this.

## 2. Provider unit tests

### 2.1 Monobank provider (`tests/unit/test_monobank_rates_provider.py`)

Use `unittest.mock.patch` to replace `fetch_currency_rates`.

1. **normalizes a standard entry with rate_buy/sell**
   - Fake client returns one entry: `currency_code_a=840, currency_code_b=980, rate_buy=39.0, rate_sell=41.0, rate_cross=None, date=1717243200`.
   - Expect one `NormalizedRate`: `source=monobank, currency_from="USD", currency_to="UAH", rate_buy=Decimal("39.0"), rate_sell=Decimal("41.0"), rate_mid=Decimal("40.0")`.
   - `at_time` is `datetime.fromtimestamp(1717243200, tz=UTC)`, assert `tzinfo=UTC`.

2. **prefers rate_cross over buy/sell midpoint**
   - Fake entry with `rate_buy=39, rate_sell=41, rate_cross=40.5`.
   - `rate_mid == Decimal("40.5")`.

3. **computes midpoint when only buy/sell present**
   - Entry with `rate_buy=39, rate_sell=41, rate_cross=None`.
   - `rate_mid == Decimal("40")`.

4. **drops entry with no rate data**
   - Entry with `rate_buy=None, rate_sell=None, rate_cross=None`.
   - Returned list omits this entry.

5. **drops entry with buy only or sell only**
   - Entry with `rate_buy=39, rate_sell=None, rate_cross=None`.
   - Dropped (can't compute mid).

6. **drops entry with unknown numeric code**
   - Entry with `currency_code_a=999` (not in ISO numeric set).
   - Dropped silently, no exception.

7. **processes multiple entries independently**
   - Three entries: one valid, one with unknown code, one missing rates. Returned list has exactly one `NormalizedRate`.

8. **rate_buy and rate_sell are Decimal when present, None otherwise**
   - Parametrized assertion across scenarios above.

### 2.2 NBU live provider (`tests/unit/test_nbu_rates_provider.py`)

1. **normalizes a single entry**
   - Fake client returns `cc="USD", rate=41.0, exchange_date="01.06.2025"`.
   - One `NormalizedRate`: `currency_from="USD", currency_to="UAH", rate_buy=None, rate_sell=None, rate_mid=Decimal("41.0"), at_time=datetime(2025, 6, 1, tzinfo=UTC)`.

2. **handles multiple currencies in one response**
   - Fake returns USD, EUR, GBP.
   - Three `NormalizedRate`s, each with `currency_to="UAH"`.

3. **parses NBU date format correctly**
   - `exchange_date="15.03.2024"` → `datetime(2024, 3, 15, tzinfo=UTC)`.

4. **rate_buy and rate_sell always None**
   - NBU doesn't publish bid/ask; both fields always None regardless of input.

### 2.3 NBU historical-range provider (`tests/unit/test_nbu_historical_rates_provider.py`)

Mock `fetch_nbu_historical_rates` and `all_alpha_codes`.

1. **iterates currencies, excludes UAH**
   - Mock `all_alpha_codes()` returns `["USD", "EUR", "UAH"]`.
   - Call `fetch_historical_rates(date(2024, 1, 1), date(2024, 1, 2))`.
   - `fetch_nbu_historical_rates` called twice: once for USD, once for EUR. Never for UAH.

2. **normalizes amount/units**
   - Fake entry: `currency_code_l="JPY", amount=100, units=2, start_date="01.01.2024"`.
   - `rate_mid == Decimal("100") / Decimal("2") == Decimal("50")`.

3. **preserves precision for non-integer rates**
   - `amount=37.1234, units=1` → `rate_mid == Decimal("37.1234")`.

4. **returns one NormalizedRate per (currency, date)**
   - Fake returns 3 dates per currency across 2 currencies. Result length 6.

5. **date parsing uses DD.MM.YYYY**
   - `start_date="15.03.2024"` → `at_time=datetime(2024, 3, 15, tzinfo=UTC)`.

## 3. Service unit tests (`tests/unit/test_service_dispatch.py`)

Use `unittest.mock.AsyncMock` for the repo. No real DB.

1. **HISTORICAL kind routes to upsert_historical**
   - Config with `kind=RateKind.HISTORICAL`. Single rate.
   - Assert `repo.upsert_historical` called once; `repo.upsert_polled` not called.

2. **POLLED kind routes to upsert_polled**
   - Config with `kind=RateKind.POLLED, interval_seconds=300`.
   - Assert `repo.upsert_polled` called once with `update_cadence_seconds=300`.
   - `repo.upsert_historical` not called.

3. **batch of N rates → N upsert calls**
   - 5 rates, polled config.
   - `repo.upsert_polled.call_count == 5`.

4. **mixed-kind batches never happen — not a test case, but assert config-driven dispatch**
   - Parametrize over both kinds; verify only the relevant upsert is called.

5. **per-rate at_time passed through**
   - Rate with `at_time=T`. Assert `upsert_*` called with `at_time=T`.

6. **polled: config.interval_seconds passed as update_cadence_seconds**
   - Config `interval_seconds=600`. Assert `upsert_polled` kwarg `update_cadence_seconds=600`.

7. **empty batch is a no-op**
   - `rates=[]`. No upsert calls. Log emitted with count 0 (verify via `caplog`).

8. **repo exception propagates**
   - `upsert_polled.side_effect = RuntimeError("db down")`.
   - `service.ingest_rates(...)` raises `RuntimeError`. Service does not swallow.

## 4. Rate loop unit tests (`tests/unit/test_rate_loop.py`)

The `_rate_loop` function is in `main.py`; import and test directly.
Use `AsyncMock` for `fetch`, `service`, and `asyncio.sleep`.

1. **normal cycle: fetch → ingest → sleep**
   - Mock `fetch` returns 3 rates. Mock `service.ingest_rates` succeeds.
   - Patch `asyncio.sleep` to raise `asyncio.CancelledError` on first call (forces loop exit).
   - Expected order: `fetch`, `ingest_rates(..., config)`, `sleep(config.interval_seconds)`.

2. **fetch raises: logs, skips ingest, still sleeps**
   - `fetch.side_effect = RuntimeError("api down")`.
   - `service.ingest_rates` not called.
   - `caplog` contains a message mentioning the source.
   - `sleep` still called.

3. **ingest raises: logs, still sleeps, fetch still occurs next cycle**
   - First cycle: `fetch` returns rates, `ingest_rates.side_effect = RuntimeError`.
   - Logged, `sleep` called.
   - Set up second cycle: `ingest_rates.side_effect = None`. Force exit after.
   - Verify second-cycle `fetch` and `ingest_rates` both called.

4. **sleep duration matches config**
   - Config `interval_seconds=42`. Assert `asyncio.sleep(42)`.

## 5. Repo integration tests — `upsert_polled`

(`tests/integration/test_repo_upsert_polled.py`)

Use `conn` fixture. No parallelism within a single test.

### 5.1 Empty state

1. **empty sequence: inserts new open row**
   - No existing rows for the pair. Call `upsert_polled`.
   - Query result: one row. `valid_from == at_time`, `valid_to IS NULL`, `last_polled_at == at_time`, `update_cadence_seconds == arg`.

2. **inserted row has correct rates**
   - Verify `rate_buy`, `rate_sell`, `rate_mid` match args.

### 5.2 Existing open row, rates match

3. **rates match: bumps last_polled_at only**
   - Seed open polled row with specific rates and `last_polled_at = T - 1m`.
   - Call `upsert_polled` at `T` with same rates.
   - Row count unchanged.
   - `last_polled_at == T`. `valid_from` unchanged. `valid_to` still NULL.

4. **rates match: refreshes update_cadence_seconds**
   - Seeded row has `update_cadence_seconds=300`.
   - Call with `update_cadence_seconds=600`.
   - Row's `update_cadence_seconds == 600`.

### 5.3 Existing open row, rates differ

5. **rates differ: closes current, opens new**
   - Seed open polled row with `rate_mid=40`, `valid_from = T - 1m`.
   - Call `upsert_polled` at `T` with `rate_mid=41`.
   - Expect two rows: old row closed (`valid_to == T`), new row open (`valid_from == T, valid_to IS NULL`, `rate_mid == 41`).

6. **rates differ: only rate_buy changes (not mid)**
   - Seed `rate_buy=39, rate_sell=41, rate_mid=40`.
   - Call with `rate_buy=39.5, rate_sell=41, rate_mid=40`.
   - Treated as different → close + open.

7. **NULL-vs-set rate change is a rate change**
   - Seed `rate_buy=39`. Call with `rate_buy=None`.
   - Close + open.

### 5.4 Historical rows present

8. **historical rows in sequence are ignored**
   - Seed a historical row (open-ended) at `T - 1d`. No polled rows.
   - Call `upsert_polled` at `T`.
   - Expect: one polled row inserted. Historical row untouched.

9. **polled and historical rows coexist**
   - Seed historical open row at `T - 1d`. Seed polled open row at `T - 1h`.
   - Call `upsert_polled` at `T`, rates unchanged from the polled row.
   - Polled: `last_polled_at` bumped. Historical: unchanged.

### 5.5 Concurrency

10. **concurrent pollers serialize via FOR UPDATE**
    - Two concurrent `upsert_polled` calls on separate connections for the same pair, both with new rates.
    - Exactly two rows created (close + open, once). Not four.
    - Implement via `asyncio.gather` on two connections; the test harness must use two `conn` fixtures or bypass the transaction-fixture to exercise real locking.

## 6. Repo integration tests — `upsert_historical`

(`tests/integration/test_repo_upsert_historical.py`)

### 6.1 Empty state

1. **empty sequence: inserts fresh open row**
   - No existing historical rows.
   - Call `upsert_historical` at `T`.
   - Expect one row: `valid_from == T, valid_to IS NULL, last_polled_at IS NULL, update_cadence_seconds IS NULL`.

### 6.2 Exact valid_from match

2. **same rates at same valid_from: no-op**
   - Seed historical row at `valid_from = T` with specific rates.
   - Call `upsert_historical` at `T` with same rates.
   - Row count unchanged. All fields unchanged (verify by snapshotting before/after).

3. **different rates at same valid_from: updates in place**
   - Seed at `valid_from = T` with `rate_mid=40`. `valid_to` set to something (e.g. `T + 1d`).
   - Call `upsert_historical` at `T` with `rate_mid=41`.
   - Row count unchanged. `rate_mid == 41`. `valid_to` unchanged.

### 6.3 Slot-in middle (covering row exists)

4. **splits open covering row**
   - Seed historical open row `valid_from = T - 10d, valid_to = NULL`.
   - Call `upsert_historical` at `T - 5d` with different rates.
   - Expect two rows: old row now `[T - 10d, T - 5d)`, new row `[T - 5d, NULL)`.

5. **splits closed covering row**
   - Seed historical row `[T - 10d, T - 3d)`.
   - Call at `T - 5d` with different rates.
   - Expect two rows: old row now `[T - 10d, T - 5d)`, new row `[T - 5d, T - 3d)`.

6. **split preserves predecessor and successor rows**
   - Seed three rows forming a continuous history: `[T - 20d, T - 10d)`, `[T - 10d, T - 3d)`, `[T - 3d, NULL)`.
   - Call at `T - 5d` (falls in middle row).
   - Expect 4 rows. Predecessor and successor untouched. Middle row shrinks, new row inserted.

### 6.4 Slot-in at the start (next_historical exists)

7. **earlier than all existing: inserts bounded row**
   - Seed historical row `valid_from = T, valid_to = NULL`.
   - Call at `T - 1d` with different rates.
   - Expect two rows: new row `[T - 1d, T)`, seeded row still `[T, NULL)`.

### 6.5 Bug-guard (all queries missed, but rows exist)

8. **guard fires when invariant is violated**
   - This is not easily reachable under correct logic; simulate by monkey-patching one of the internal queries to return None spuriously, or by constructing a pathological schema state. If neither is practical, mark as "logic-guard covered by code review" and skip.

### 6.6 Polled rows present

9. **polled rows are ignored by upsert_historical**
   - Seed polled open row at `T - 1h`.
   - Call `upsert_historical` at `T - 1d` with new rates.
   - Expect: one new historical row inserted. Polled row untouched.

10. **historical and polled rows can share a valid_from**
    - Seed polled row `valid_from = T, last_polled_at = T`.
    - Call `upsert_historical` at `T` — this is a new historical, not a match on any existing historical.
    - Expect: two rows total. Polled row unchanged. New historical row at `valid_from = T`.

### 6.7 Concurrency

11. **concurrent historical upserts serialize via advisory lock**
    - Two concurrent calls on separate connections for the same pair, different `valid_from` values that would both split the same row.
    - After both complete, sequence invariants hold: no overlapping intervals, no duplicate `valid_from`.

12. **historical and polled upserts for the same pair don't block each other**
    - Concurrent `upsert_polled` and `upsert_historical` on separate connections. Both complete without deadlock. (Verify via a timeout-guarded `asyncio.gather`.)

## 7. End-to-end service integration tests

(`tests/integration/test_service_end_to_end.py`)

Real service, real repo, real DB.

1. **polled config routes to polled upsert**
   - Register polled config for `monobank`.
   - Call `service.ingest_rates(pool, rates, config)`.
   - Query DB: rows have `last_polled_at NOT NULL, update_cadence_seconds = config.interval_seconds`.

2. **historical config routes to historical upsert**
   - Historical config for `nbu`.
   - Resulting rows have `last_polled_at IS NULL, update_cadence_seconds IS NULL`.

3. **two successive polled cycles with unchanged rates**
   - Cycle 1 at `T - 5m`: inserts row.
   - Cycle 2 at `T` with identical rates: bumps `last_polled_at`.
   - Row count: 1. `last_polled_at == T`.

4. **two successive polled cycles with changing rates**
   - Cycle 1 inserts. Cycle 2 changes `rate_mid`. Expect 2 rows: first closed, second open.

5. **historical backfill spanning multiple dates**
   - Simulate NBU historical provider output: rates for USD on 5 consecutive days.
   - Call `service.ingest_rates` once with all 5.
   - Expect 5 historical rows for USD/UAH, with correct `valid_from`/`valid_to` chain (first four closed to the next's `valid_from`, last open).

6. **historical reingestion is idempotent**
   - Run the same 5-day backfill twice.
   - Second run is all no-ops. Row count still 5.

7. **historical reingestion with corrections updates in place**
   - First run: 5 days of rates. Second run: same 5 dates, one with a different `rate_mid`.
   - Expect 5 rows. The corrected day has new rate; neighbors untouched.

8. **mixed provider behavior over time**
   - Register both Monobank (polled) and NBU (historical) configs.
   - Run one ingestion cycle of each.
   - Verify both sequences present for USD/UAH, independent, non-interfering.

## 8. Test hygiene

- Every async test uses `pytest.mark.asyncio`.
- Integration tests never share state — per-test transaction rollback.
- `caplog` for log assertions.
- `Decimal` throughout; never assert on floats.
- Mocks via `unittest.mock.AsyncMock`.
- Parametrize when it genuinely compresses; otherwise keep tests explicit.

## 9. Coverage target

```
pytest --cov=grosh_ingestion --cov-branch
```

Targets: ≥ 90% line, ≥ 85% branch on:

- `services/currency_rate_service.py`
- `repositories/currency_rate_repo.py`
- `banks/monobank/rates_provider.py`
- `banks/nbu/rates_provider.py`

`main.py` and webhook handlers are lower-priority; excluded from the
coverage target. Providers' external HTTP clients
(`banks/*/client.py`) are tested implicitly via provider tests with
mocked fetches.
