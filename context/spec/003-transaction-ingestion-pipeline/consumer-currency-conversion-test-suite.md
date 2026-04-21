# Currency Conversion — Test Specification

Implementation instructions for unit and integration tests covering
`currency_conversion_service.py` and `currency_rate_repo.py`. Each test
maps to one pytest function with explicit Given/When/Then.

Reference `consumer-currency-conversion-guide.md` for behavioral context. This
document is authoritative for test structure and assertions.

## 1. Test infrastructure

### 1.1 File layout

```
services/consumer/tests/
  conftest.py
  unit/
    conftest.py                             # in-memory repo, seed helpers
    test_pick_rate.py
    test_ordered_pivots.py
    test_ensure_tz.py
    test_tiers_and_sides.py
    test_rate_step.py
    test_rate_path.py
    test_path_metadata.py
    test_service_resolution.py
    test_service_rate_side.py
    test_service_metadata.py
  integration/
    test_repo_find_fresh_rate.py
    test_repo_find_closest_rate.py
    test_repo_load_source_chain.py
    test_service_end_to_end.py
```

No `test_row_is_fresh.py` — the FRESH predicate lives in SQL and is
tested via integration tests against real Postgres.

### 1.2 Shared fixtures (`tests/conftest.py`)

DB fixtures follow the existing `grosh-api` pattern:

- `db_pool` — session-scoped `asyncpg.Pool`, `DATABASE_URL` from env, `@timescaledb:` → `@localhost:`.
- `conn` — function-scoped: connection + transaction, yields, rolls back.

Module-specific:

- `rate_repo` — function-scoped, `CurrencyRateRepo()`.
- `service` — function-scoped, `CurrencyConversionService(rate_repo)`.
- `insert_source_config(conn, *, source, fallback_source, base_currencies)` — INSERT helper (no `max_staleness_seconds` parameter).
- `insert_rate(conn, *, source, currency_from, currency_to, rate_mid, rate_buy=None, rate_sell=None, valid_from, valid_to=None, last_polled_at=None, update_cadence_seconds=None)` — INSERT helper.
- `make_event(source='monobank', currency_code='PLN', amount_cents=10000, time=None, **kwargs)` — `RawTransactionEvent` builder.

### 1.3 In-memory repo (`tests/unit/conftest.py`)

`InMemoryRateRepo` **does not inherit from `CurrencyRateRepo`** and
**does not touch `conn`**. `conn` is accepted for interface
compatibility and ignored.

Internal storage: list of rate dicts and dict of source configs.

Methods match production semantics exactly:

- `find_fresh_rate(conn, source, currency_from, currency_to, at_time, poll_tolerance)`: return the row with latest `valid_from` where:
  - `source, currency_from, currency_to` match
  - `last_polled_at IS NOT NULL` AND `update_cadence_seconds IS NOT NULL`
  - `valid_from <= at_time`
  - `valid_to IS NULL OR valid_to > at_time`
  - `last_polled_at + poll_tolerance * update_cadence_seconds >= at_time`
  - None if no match.

- `find_closest_rate(conn, currency_from, currency_to, at_time, sources)`: return the row with minimum proximity to `at_time`:
  - Poll-based row (`last_polled_at IS NOT NULL`): 0 if `valid_from <= at_time <= last_polled_at`; `at_time - last_polled_at` if `at_time > last_polled_at`; `valid_from - at_time` if `at_time < valid_from`.
  - Historical row (`last_polled_at IS NULL`): `|at_time - valid_from|`.
  - Row must be within 7 days by its own metric.
  - Tie-break: chain order (`sources` list index), then row id.
  - Return value carries `proximity_seconds` (integer).

- `load_source_chain(conn, entry_source)`: flattened chain, `RateSourceChainError` on depth > 10 or cycle.

Seed helpers:

- `add_source(source, fallback=None, base_currencies=('UAH',))`.
- `add_rate(source, from_, to_, *, mid, buy=None, sell=None, valid_from, valid_to=None, polled=None, interval=None)`.

### 1.4 Time discipline

Default transaction time: `T = datetime(2025, 6, 1, 12, 0, tzinfo=UTC)`.

- **Poll-based FRESH**: `polled = T - 30s`, `interval = 60`. `K = 2` → grace 120s, covers 30s lag.
- **Poll-based past grace**: `polled = T - 3600s`, `interval = 60`. Cutoff at `T - 3480s`, well before T.
- **Historical within CLOSEST window**: `valid_from = T - 1d`, `polled = None`, `interval = None`.
- **Historical beyond CLOSEST window**: `valid_from = T - 8d`.
- **Malformed**: `polled` set, `interval = None`.

`K = _POLL_INTERVAL_TOLERANCE = 2` is the service-level constant,
passed to `find_fresh_rate` as the `poll_tolerance` parameter.

All datetimes are timezone-aware unless a test exercises the naive branch.

## 2. Unit tests — pure functions

### 2.1 `_pick_rate` (`tests/unit/test_pick_rate.py`)

1. **buy side when divide false and rate_buy present**: `(3.95, BUY)`.
2. **sell side when divide true and rate_sell present**: `(4.05, SELL)`.
3. **falls back to mid when buy NULL and divide false**: `(4.00, MID)`, not sell.
4. **falls back to mid when sell NULL and divide true**: `(4.00, MID)`, not buy.
5. **both sides NULL returns mid for both directions**.
6. **never substitutes opposite side** — parametrize over all `(divide, buy_null, sell_null)` combos.

### 2.2 `_ordered_pivots` (`tests/unit/test_ordered_pivots.py`)

1. **empty chain yields empty list**.
2. **single source, single base**.
3. **single source, multiple bases** preserves array order.
4. **multiple sources, disjoint bases** yields chain-order concatenation.
5. **multiple sources, overlapping bases** — first-seen wins, chain order preserved.
6. **deterministic across calls** — two calls on same input return equal lists.

### 2.3 `_ensure_tz` (`tests/unit/test_ensure_tz.py`)

1. **naive datetime gets UTC attached**, wall-clock unchanged.
2. **aware datetime unchanged**.

### 2.4 `RateTier` and `RateSide` (`tests/unit/test_tiers_and_sides.py`)

1. **`list(RateTier)` is `[FRESH, CLOSEST]`**.
2. **int values**: `FRESH=0`, `CLOSEST=1`.
3. **`max` of mixed tier list** returns `CLOSEST` when any element is `CLOSEST`.
4. **`RateSide` values**: `"buy"`, `"sell"`, `"mid"`.

### 2.5 `_RateStep.apply` (`tests/unit/test_rate_step.py`)

1. **multiply when divide false**: `rate=4.0, apply(100) == 400`.
2. **divide when divide true**: `rate=4.0, apply(100) == 25`.
3. **preserves Decimal precision**.

### 2.6 `_RatePath` (`tests/unit/test_rate_path.py`)

1. **max_tier picks worst**: `[FRESH, CLOSEST, FRESH]` → `CLOSEST`.
2. **max_proximity_seconds picks worst across steps**: `[0, 3600, 0]` → `3600`.
3. **effective_rate compounds multiply steps**: `[2, 3]` → `6`.
4. **effective_rate compounds with divide**: `[2 mul, 4 div]` → `0.5`.
5. **apply equals `amount * effective_rate` for multiply-only paths**.
6. **single-step path behaves as step**.

### 2.7 `_path_metadata` (`tests/unit/test_path_metadata.py`)

1. **1-hop metadata structure**: keys `path`, `effective_rate`, `hops`, `quality`, `max_proximity_seconds`, `sides`. Step keys include `proximity_seconds`.
2. **2-hop metadata structure**: path length 2, hops 2.
3. **sides deduped and sorted**: `[SELL, BUY, MID, BUY]` → `["buy", "mid", "sell"]`.
4. **quality is max tier**: `[FRESH, FRESH, CLOSEST]` → `"closest"`.
5. **max_proximity_seconds matches worst step**: `[0, 3600]` → `3600`.
6. **effective_rate is a string**.
7. **op reflects divide flag**.
8. **proximity_seconds serialized as integer per step**.

## 3. Unit tests — service with in-memory repo

All tests use `InMemoryRateRepo`. Default transaction time `T`. Default
chain: `monobank → nbu → NULL`. `base_currencies={UAH}` on both.

### 3.1 Same-currency passthrough (`tests/unit/test_service_resolution.py`)

1. **target equals event currency**
   - Event `currency_code=UAH`, `amount=12345`.
   - `amounts[UAH] == 12345`. No `rate_uah` in metadata.

### 3.2 1-hop direct resolution

2. **direct FRESH from bank**
   - Monobank PLN/UAH, `polled = T - 30s`, `interval = 60`, `valid_from = T - 1h`, `valid_to = NULL`.
   - Event Monobank PLN 10000.
   - Path uses Monobank, quality fresh, one step, `proximity_seconds = 0`.

3. **CLOSEST from historical fallback when bank lacks rate**
   - No Monobank rate. NBU PLN/UAH historical, `valid_from = T - 1d`, `polled = None`, `interval = None`.
   - Path uses NBU, quality closest (historical rows are never FRESH). `proximity_seconds ≈ 86400`.

4. **reverse-and-divide when only reverse stored**
   - Monobank UAH/PLN FRESH.
   - Step `divide=True`, amount = input / rate.

5. **direct preferred over reverse at same source + tier**
   - Monobank has both PLN/UAH and UAH/PLN FRESH, distinct rates.
   - Direct wins. Verify by `rate_id`.

### 3.3 Tier ordering

6. **CLOSEST at fallback beats nothing at bank**
   - Monobank PLN/UAH past grace (`polled = T - 3600s`) — not FRESH, CLOSEST-eligible at ~1h proximity.
   - NBU PLN/UAH historical (`valid_from = T - 1d`) — never FRESH, CLOSEST-eligible at 1d proximity.
   - Path uses Monobank (1h < 1d), quality closest. `proximity_seconds ≈ 3600`.

7. **CLOSEST only when no FRESH anywhere**
   - Monobank rate past grace but within 7d proximity.
   - NBU historical `valid_from = T - 3d`.
   - Path resolves at CLOSEST.

8. **per-tier exhaustion short-circuits**
   - Mock repo with `AsyncMock`, seed a FRESH hit on first call.
   - Assert `find_closest_rate` is never called.

9. **CLOSEST picks by proximity across chain**
   - Monobank poll-based `polled = T - 5d` (outside grace).
   - NBU historical `valid_from = T - 2d`.
   - Path uses NBU (2d < 5d). Quality closest. `proximity_seconds ≈ 172800`.

### 3.4 Source priority within FRESH

Note: the default chain `monobank → nbu` has only one poll-based source.
Tests requiring "bank + fallback both FRESH" use an extended chain
`monobank → mono2 → nbu` where `mono2` is a hypothetical second
poll-based source.

10. **bank beats fallback at FRESH**
    - Chain: `monobank → mono2`. Both FRESH on PLN/UAH with distinct rates.
    - Monobank wins.

11. **source priority beats direction within FRESH**
    - Chain: `monobank → mono2`.
    - Monobank only has UAH/PLN FRESH (reverse).
    - mono2 has PLN/UAH FRESH (direct).
    - Monobank reverse wins.

### 3.5 Multi-hop resolution

12. **2-hop via pivot when no 1-hop**
    - No PLN/USD direct/reverse.
    - Monobank PLN/UAH + UAH/USD both FRESH.
    - Event Monobank PLN → USD.
    - Path 2 steps, pivot UAH, both Monobank, both FRESH. Both steps `proximity_seconds = 0`.

13. **2-hop skips pivot equal to src or tgt**
    - Pivots `[UAH, USD]`. Event UAH → USD. USD skipped.

14. **2-hop both legs must resolve at same tier**
    - Chain: `monobank → mono2`.
    - Monobank PLN/UAH FRESH, Monobank UAH/USD past grace.
    - mono2 PLN/UAH FRESH, mono2 UAH/USD FRESH.
    - Path is monobank+mono2 at FRESH. Each leg resolves independently via
      chain walk: leg 1 finds Monobank (first in chain, FRESH), leg 2 tries
      Monobank (fails FRESH) then mono2 (FRESH). The invariant is same-tier,
      not same-source.

15. **1-hop FRESH preferred over 2-hop FRESH**
    - ECB (extended chain) has direct PLN/USD FRESH.
    - Monobank has PLN/UAH + UAH/USD FRESH.
    - 1-hop ECB wins.

16. **2-hop FRESH preferred over 1-hop CLOSEST**
    - Monobank PLN/USD past grace (CLOSEST-eligible).
    - Monobank PLN/UAH + UAH/USD FRESH.
    - 2-hop FRESH wins.

17. **2-hop uses ordered_pivots order**
    - Chain: Monobank base=[UAH], NBU base=[EUR, USD].
    - Both UAH and EUR pivots resolve at FRESH.
    - UAH chosen (chain-major order).

### 3.6 Rate-side selection (`tests/unit/test_service_rate_side.py`)

18. **1-hop direct uses buy side**
    - Monobank PLN/UAH FRESH, `buy=3.9, mid=4.0, sell=4.1`.
    - Event PLN 10000. `amounts[UAH] = 39000`. `rate_side == "buy"`.

19. **1-hop reverse uses sell side**
    - Monobank UAH/PLN FRESH, `buy=0.24, mid=0.25, sell=0.26`.
    - Event PLN 10000. `amounts[UAH] = round(10000/0.26) = 38462`. `rate_side == "sell"`, `op == "divide"`.

20. **2-hop direct + direct → both buy**
    - Monobank PLN/UAH + UAH/USD FRESH with buy/sell.
    - `sides == ["buy"]`.

21. **2-hop reverse + direct → buy and sell**
    - Monobank UAH/PLN (leg 1 reverse) + UAH/USD (leg 2 direct), both FRESH.
    - `sides == ["buy", "sell"]`.

22. **falls back to mid when row has NULL buy**
    - Monobank PLN/UAH FRESH, `buy=NULL`, `mid=4.0`.
    - Step rate 4.0, `rate_side == "mid"`.

23. **never falls to opposite side**
    - Monobank PLN/UAH FRESH, `buy=NULL`, `sell=4.1`, `mid=4.0`.
    - Direct conversion uses mid (4.0), not sell.

24. **compounded spread in 2-hop**
    - Monobank PLN/UAH `buy=3.9`; UAH/USD `buy=0.025`.
    - Event PLN 10000 → USD. effective_rate = 0.0975. Amount 975.

### 3.7 No path

25. **no path → None amount, no metadata entry**
    - Empty repo. Event Monobank PLN.
    - `amounts[UAH] is None`, `rate_uah` not in metadata, warning logged.

26. **one target resolvable, one not**
    - Monobank PLN/UAH FRESH only.
    - `amounts[UAH]` populated, USD/EUR None.

### 3.8 Metadata (`tests/unit/test_service_metadata.py`)

27. **metadata key naming**: `rate_uah`, `rate_usd`, `rate_eur` lowercase.
28. **metadata quality matches path max tier**.
29. **metadata hops matches step count**.
30. **metadata effective_rate is stringified Decimal**.
31. **metadata omits entry for passthrough currency**.
32. **FRESH step metadata has `proximity_seconds = 0`**.
33. **CLOSEST step metadata has `proximity_seconds > 0`** (matching row's actual distance).
34. **`max_proximity_seconds` equals max over steps**.

## 4. Integration tests — repo layer

All tests use `conn` fixture with rolled-back transaction.

### 4.1 `find_fresh_rate` (`tests/integration/test_repo_find_fresh_rate.py`)

Pass `poll_tolerance = 2` unless otherwise noted.

1. **returns matching row by (source, pair, time)**
   - Insert Monobank PLN/UAH with `polled = T - 30s, interval = 60, valid_from = T - 1h, valid_to = NULL`.
   - Call `find_fresh_rate(..., T, 2)`. Returns the row.

2. **returns None for historical row**
   - Insert NBU PLN/UAH with `polled = NULL, interval = NULL, valid_from = T - 1d, valid_to = NULL`.
   - Call `find_fresh_rate(..., T, 2)`. Returns None.

3. **returns None for poll-based row past grace**
   - `polled = T - 3600s, interval = 60`. Grace = 120s, cutoff at `T - 3480s`. Returns None.

4. **at exactly cutoff is FRESH**
   - `polled = T - 120s, interval = 60`. Cutoff at T. Returns the row.

5. **1s past cutoff is not FRESH**
   - `polled = T - 121s, interval = 60`. Cutoff at `T - 1s`. Returns None.

6. **at_time before valid_from returns None**
   - `valid_from = T + 1h, polled = T - 30s, interval = 60`. Returns None.

7. **at_time before last_polled_at (backfill) is FRESH**
   - `valid_from = T - 30d, polled = T + 100s, interval = 60`. Event at T. Returns the row.

8. **SCD2 cap: row with valid_to before at_time is not FRESH**
   - Row A: `valid_from = T - 3m, polled = T - 2m, interval = 60, valid_to = T - 1m`. Grace cutoff would be `T`.
   - Call `find_fresh_rate(..., T, 2)`. Returns None — A has been superseded.

9. **SCD2 successor exists: returns successor, not predecessor**
   - Row A: `valid_from = T - 3m, polled = T - 2m, interval = 60, valid_to = T - 1m`.
   - Row B: `valid_from = T - 1m, polled = T - 1m, interval = 60, valid_to = NULL`.
   - Call `find_fresh_rate(..., T, 2)`. Returns B (latest `valid_from` wins under `ORDER BY valid_from DESC LIMIT 1`).

10. **malformed row (interval NULL) returns None**
    - Monobank with `polled = T - 30s, interval = NULL`. Returns None (filter: `update_cadence_seconds IS NOT NULL`).

11. **picks latest valid_from when multiple FRESH rows** (SCD2 violation, but pin behavior)
    - Two overlapping FRESH rows. Latest `valid_from` wins.

12. **filters by source correctly**.

13. **returns all expected fields** (rate_buy/sell/mid, valid_from/to, last_polled_at, update_cadence_seconds).

14. **poll_tolerance parameter is respected**
    - `polled = T - 150s, interval = 60`. With `tolerance = 2`, cutoff at `T - 30s` → not FRESH. With `tolerance = 3`, cutoff at `T + 30s` → FRESH.

### 4.2 `find_closest_rate` (`tests/integration/test_repo_find_closest_rate.py`)

1. **poll-based rows: rank by last_polled_at distance when at_time is after**
   - Two Monobank rows, `polled = T - 2d` and `T - 5d`. Returns 2d one. `proximity_seconds ≈ 172800`.

2. **poll-based row: rank by valid_from when at_time is before valid_from**
   - Monobank with `valid_from = T + 2d, polled = T + 5d`. Returns this row with `proximity_seconds ≈ 172800`.

3. **poll-based row: proximity zero when at_time inside [valid_from, last_polled_at]**
   - Monobank with `valid_from = T - 10d, polled = T - 2d`. `at_time = T - 5d`. `proximity_seconds = 0`.

4. **historical rows: rank by valid_from distance**
   - Two NBU rows, `valid_from = T - 10d` and `T - 2d`. Returns 2d one.

5. **historical row: future vs past valid_from ranks by proximity**
   - NBU `valid_from = T - 3d` and NBU `valid_from = T + 1d`. Returns future one.

6. **mixed poll-based and historical: unified ranking**
   - Monobank `polled = T - 3d`. NBU `valid_from = T - 1d`. Returns NBU.

7. **tie-break: equal proximity between sources → chain order wins**
   - Monobank `polled = T - 1d` (proximity 1d). NBU `valid_from = T - 1d` (proximity 1d).
   - `sources = ['monobank', 'nbu']`. Returns Monobank.

8. **tie-break: equal proximity within source → lower id wins**
   - Two Monobank rows both `polled = T - 2d`. Lower id wins.

9. **respects source ANY filter**.

10. **None beyond 7-day window** — rows outside 7d by their own metric.

11. **exactly at 7-day boundary** (inclusive per `<=`).

12. **eligibility uses each row's own metric**
    - Monobank `polled = T - 8d` and NBU `valid_from = T - 8d`. Returns None.

13. **returns proximity_seconds as integer**.

### 4.3 `load_source_chain` (`tests/integration/test_repo_load_source_chain.py`)

1. **single source, no fallback**.
2. **two-source chain**.
3. **three-source chain**.
4. **ordered by depth**.
5. **depth exactly 10 does not raise**.
6. **depth > 10 raises RateSourceChainError**; message contains `"depth cap"`, `"10"`, all 10 source names.
7. **cyclic chain raises RateSourceChainError**
   - Insert a→b, b→c, c→a. CTE walks cycle to depth 10. Tail non-NULL → raise.
   - Message contains `'a'`, `'b'`, `'c'` (containment). Do NOT assert chain length == 3.
8. **unknown entry source returns `[]`, does not raise**.
9. **base_currencies preserves array order**.

## 5. Integration tests — end-to-end (`tests/integration/test_service_end_to_end.py`)

Seeds `rate_source_config` and `currency_rates`, calls
`service.convert(conn, event)`.

Fixed chain: `monobank → nbu → NULL`. Both `base_currencies=['UAH']`
(extend to `['UAH','EUR','USD']` for tests needing other pivots).

Monobank rows seeded with `update_cadence_seconds = 60`. NBU rows
with `update_cadence_seconds = NULL`.

`T = datetime(2025, 6, 1, 12, 0, tzinfo=UTC)`. `K = 2` → grace 120s.

1. **passthrough**
   - Event `currency=UAH, amount=55500`. Seed Monobank UAH/USD and UAH/EUR FRESH.
   - `amounts[UAH] == 55500`. USD/EUR populated. `rate_uah` absent.

2. **Monobank PLN direct to UAH, 2-hop via UAH for USD/EUR**
   - Monobank PLN/UAH FRESH (`buy=10.9, mid=11.0, sell=11.1`).
   - Monobank UAH/USD FRESH (`buy=0.024, mid=0.025, sell=0.026`).
   - Monobank UAH/EUR FRESH (`buy=0.022, mid=0.023, sell=0.024`).
   - Event PLN 10000.
   - `amounts[UAH]=109000, USD=2616, EUR=2398`. All buy, all fresh, all `proximity_seconds=0`.

3. **reverse direction when only reverse stored**
   - Monobank UAH/PLN FRESH (`buy=0.089, mid=0.09, sell=0.091`).
   - `amounts[UAH] = round(10000/0.091) = 109890`. `op="divide"`, `rate_side="sell"`.

4. **historical fallback (NBU) resolves at CLOSEST**
   - No Monobank. NBU PLN/UAH historical (`valid_from=T-1d, mid=11.0`, buy/sell NULL).
   - `amounts[UAH] = 110000`. Source nbu, rate_side mid, quality closest. `proximity_seconds ≈ 86400`. Warning logged.

5. **poll-based row past grace resolves at CLOSEST**
   - Monobank PLN/UAH `polled=T-3600s, interval=60`.
   - No NBU rate.
   - Resolves at CLOSEST. Quality closest. `proximity_seconds ≈ 3600`.

6. **CLOSEST at fallback preferred by proximity**
   - Monobank PLN/UAH past grace (proximity 2d).
   - NBU PLN/UAH historical `valid_from=T-6h` (proximity 6h).
   - Path uses NBU, quality closest.

7. **CLOSEST when no FRESH anywhere**
   - Monobank `polled=T-2d, valid_to=T-2d`. No other rates.
   - Path quality closest. Warning logged.

8. **no path → None amount, no metadata entry**
   - Monobank PLN/UAH FRESH only. Event Monobank PLN.
   - UAH populated, USD/EUR None.

9. **bid/ask vs mid produce different amounts**
   - A: buy/mid/sell set. B: buy/sell NULL, mid only.
   - Different amounts, different sides.

10. **2-hop tier coherence: mixed-tier rejected**
    - Chain `monobank → mono2`.
    - Monobank PLN/UAH FRESH, Monobank UAH/USD past grace.
    - mono2 PLN/UAH FRESH, mono2 UAH/USD FRESH.
    - Path `["monobank", "mono2"]` at FRESH. Each leg resolves independently
      via chain walk — the invariant is same-tier, not same-source.

11. **1-hop CLOSEST wins over 2-hop CLOSEST at same tier**.

12. **empty chain**
    - Event source not in config. All amounts None (except passthrough). No crash.

13. **multiple display currencies computed independently**.

14. **chain traversed in order**
    - Three configs, each with distinct PLN/UAH rates FRESH. Monobank wins.

15. **ROUND_HALF_UP applied to cents**.

16. **very large amount — no overflow** (`10**15`, rate 1.0).

17. **timezone-naive event.time handled via `_ensure_tz`**.

18. **effective_rate reconstructs amount across scenarios**
    - 1-hop multiply, 1-hop divide, 2-hop multiply+multiply, 2-hop multiply+divide.
    - Parse `effective_rate`, multiply, quantize. Assert equals `amounts[target]`.

19. **RateSourceChainError propagates from service.convert**
    - Cyclic `a → b → a`. Event `source=a`. Raises. Service does not swallow.

20. **sides aggregation reflects multi-hop mix**
    - Monobank PLN/UAH FRESH direct; Monobank USD/UAH FRESH (leg 2 reverse).
    - `sides == ["buy", "sell"]`.

21. **SCD2 successor scenario end-to-end**
    - Row A: `valid_from=T-3m, polled=T-2m, interval=60, valid_to=T-1m, mid=10`.
    - Row B: `valid_from=T-1m, polled=T-1m, interval=60, valid_to=NULL, mid=11`.
    - Event at T, currency PLN. Expected: B wins at FRESH; amount uses 11, not 10.

22. **malformed row (interval NULL) falls to CLOSEST**
    - Monobank PLN/UAH `polled=T-30s, interval=NULL`. Valid window covers T.
    - Row filtered out of FRESH by repo SQL. Falls to CLOSEST via proximity. Quality closest.

23. **historical row with spurious interval set still resolves CLOSEST**
    - NBU PLN/UAH `polled=NULL, interval=3600, valid_from=T-1d`.
    - Row ineligible for FRESH (historical). CLOSEST proximity 1d. Quality closest.

## 6. Test hygiene

- Every async test uses `pytest.mark.asyncio` (or module-level `pytestmark = pytest.mark.asyncio`).
- Integration tests never share state — per-test transaction rollback.
- Use `caplog` for log assertions. Never parse stdout.
- `Decimal` throughout; never assert on floats.
- Parametrize when it compresses (test 5.18). Otherwise keep explicit.
- Mock-based tests use `unittest.mock.AsyncMock`.

## 7. Coverage target

```
pytest --cov=grosh_consumer.services.currency_conversion_service \
       --cov=grosh_consumer.repositories.currency_rate_repo \
       --cov-branch
```

Targets: ≥ 95% line, ≥ 90% branch. Uncovered lines explained in PR.
