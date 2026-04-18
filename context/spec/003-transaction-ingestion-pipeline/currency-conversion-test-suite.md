# Currency Conversion — Test Specification

Complete implementation instructions for unit and integration tests covering
`currency_conversion_service.py` and `currency_rate_repo.py`. Each test below
is stated with a Given/When/Then that should map directly to one pytest
function.

## 1. Test infrastructure

### 1.1 File layout

```
grosh-consumer/
  tests/
    conftest.py                               # shared fixtures
    unit/
      test_pick_rate.py
      test_ordered_pivots.py
      test_rate_step.py
      test_rate_path.py
      test_path_metadata.py
      test_ensure_tz.py
      test_tiers_and_sides.py
      test_service_resolution.py              # service with in-memory repo
      test_service_rate_side.py               # service rate-side behavior
      test_service_metadata.py
    integration/
      test_repo_find_rate_at_time.py
      test_repo_find_closest_rate.py
      test_repo_load_source_chain.py
      test_service_end_to_end.py
```

### 1.2 Shared fixtures (tests/conftest.py)

Mirror the existing `grosh-api` conftest pattern for integration tests:

- `db_pool` — session-scoped `asyncpg.Pool`, reads `DATABASE_URL` from env,
  replaces `@timescaledb:` with `@localhost:`, closes on teardown.
- `conn` — function-scoped: acquires a connection from the pool, opens a
  transaction, yields, rolls back on teardown. Every integration test runs
  inside its own rolled-back transaction.

Add for this module:

- `rate_repo` — function-scoped, returns `CurrencyRateRepo()` instance.
- `service` — function-scoped, returns `CurrencyConversionService(rate_repo)`.
- `insert_source_config(conn, *, source, fallback_source, max_staleness_seconds, base_currencies)` — helper that INSERTs one row into `rate_source_config`.
- `insert_rate(conn, *, source, currency_from, currency_to, rate_mid, rate_buy=None, rate_sell=None, valid_from, valid_to=None, last_polled_at=None)` — helper that INSERTs one row into `currency_rates`.
- `make_event(source='monobank', currency_code='PLN', amount_cents=10000, time=None, **kwargs)` — builds a `RawTransactionEvent` with sensible defaults for tests.

### 1.3 In-memory repo for unit tests (tests/unit/conftest.py)

Implement `InMemoryRateRepo` matching the `CurrencyRateRepo` interface.
Store rates as a list of records with fields matching `find_rate_at_time`
semantics (source, from, to, valid_from, valid_to, rate_mid, rate_buy,
rate_sell, last_polled_at). `find_rate_at_time` filters and picks latest
`valid_from`. `find_closest_rate` must match production SQL semantics:
filter `last_polled_at IS NOT NULL`, use past/future windowing (past via
`last_polled_at` distance, future via `valid_from` distance), rank by
the closer of the two distances, all within a 7-day cap. `load_source_chain`
returns a configured list.

Expose seed helpers: `add_source(source, max_staleness_seconds=300, base_currencies=('UAH',), fallback=None)` and `add_rate(source, from_, to_, *, mid, buy=None, sell=None, valid_from, valid_to=None, polled=None)`. `conn` parameter can be `None` — the in-memory repo ignores it.

### 1.4 Time discipline

- Default transaction time in tests: `datetime(2025, 6, 1, 12, 0, tzinfo=UTC)`.
- `last_polled_at` for a "fresh" rate: transaction time minus 60 seconds.
- `last_polled_at` for a "stale" rate: transaction time minus `max_staleness + 3600` seconds.
- `valid_from`/`valid_to` for "covers at_time": `valid_from = at_time - 1h`, `valid_to = NULL`.
- `valid_from` for "closest-only": a time outside any valid window (e.g. 2 days before, with `valid_to = 1 day before`).

All datetimes are timezone-aware unless a test specifically exercises the
naive-datetime branch.

---

## 2. Unit tests — pure functions

### 2.1 `_pick_rate` (tests/unit/test_pick_rate.py)

Each test builds a `RateRow` with literal decimals and calls `_pick_rate`
directly.

1. **buy side selected when divide is false and rate_buy present**
   - Given row with `rate_mid=Decimal("4.00")`, `rate_buy=Decimal("3.95")`, `rate_sell=Decimal("4.05")`
   - When `_pick_rate(row, divide=False)`
   - Then returns `(Decimal("3.95"), RateSide.BUY)`

2. **sell side selected when divide is true and rate_sell present**
   - Same row
   - When `_pick_rate(row, divide=True)`
   - Then returns `(Decimal("4.05"), RateSide.SELL)`

3. **falls back to mid when buy side null and divide false**
   - Given row with `rate_mid=Decimal("4.00")`, `rate_buy=None`, `rate_sell=Decimal("4.05")`
   - When `_pick_rate(row, divide=False)`
   - Then returns `(Decimal("4.00"), RateSide.MID)` — **not** rate_sell

4. **falls back to mid when sell side null and divide true**
   - Given row with `rate_mid=Decimal("4.00")`, `rate_buy=Decimal("3.95")`, `rate_sell=None`
   - When `_pick_rate(row, divide=True)`
   - Then returns `(Decimal("4.00"), RateSide.MID)` — **not** rate_buy

5. **both sides null returns mid both directions**
   - Given row with all rate_buy/rate_sell None, rate_mid set
   - When called with divide=False and divide=True
   - Then both return `(rate_mid, RateSide.MID)`

6. **never substitutes opposite side**
   - This is covered by tests 3 and 4, but add an explicit assertion-bundle
     test that loops over all (divide, buy_null, sell_null) combinations
     and asserts side is never the opposite of what divide implies.

### 2.2 `_ordered_pivots` (tests/unit/test_ordered_pivots.py)

1. **empty chain yields empty list**
   - When `_ordered_pivots([])`
   - Then returns `[]`

2. **single source single base**
   - Given chain with one SourceConfig, base_currencies=['UAH']
   - Then returns `['UAH']`

3. **single source multiple bases preserves array order**
   - Given base_currencies=['UAH', 'EUR', 'USD']
   - Then returns `['UAH', 'EUR', 'USD']` (exactly this order)

4. **multiple sources disjoint bases in chain order**
   - Given chain: Monobank[UAH], NBU[UAH,EUR], ECB[EUR,USD] — wait, disjoint:
   - Given chain: Monobank[UAH], NBU[PLN], ECB[GBP]
   - Then returns `['UAH', 'PLN', 'GBP']`

5. **overlapping bases first-seen wins**
   - Given chain: Monobank[UAH,EUR], NBU[EUR,USD], ECB[USD,GBP]
   - Then returns `['UAH', 'EUR', 'USD', 'GBP']`

6. **deterministic across calls**
   - Call `_ordered_pivots` twice on identical input; assert results are equal
     (guards against accidental set-based iteration being reintroduced).

### 2.3 `_ensure_tz` (tests/unit/test_ensure_tz.py)

1. **naive datetime gets UTC**
   - Given `datetime(2025, 1, 1, 12, 0)` (no tzinfo)
   - Then result has `tzinfo == UTC`, same wall-clock values

2. **aware datetime normalized to UTC**
   - Given `datetime(2025, 1, 1, 12, 0, tzinfo=timezone(timedelta(hours=3)))`
   - Then result has `tzinfo == UTC`, represents the same instant (`hour == 9`), and compares equal to the input

### 2.4 `RateTier` and `RateSide` (tests/unit/test_tiers_and_sides.py)

1. **RateTier iteration order**
   - `list(RateTier) == [RateTier.FRESH, RateTier.STALE, RateTier.CLOSEST]`

2. **RateTier int values**
   - FRESH=0, STALE=1, CLOSEST=2; assert comparisons: FRESH < STALE < CLOSEST

3. **RateTier max of mixed list**
   - `max([RateTier.FRESH, RateTier.CLOSEST, RateTier.STALE]) == RateTier.CLOSEST`

4. **RateSide values**
   - `RateSide.BUY.value == 'buy'`, `.SELL.value == 'sell'`, `.MID.value == 'mid'`

### 2.5 `_RateStep.apply` (tests/unit/test_rate_step.py)

1. **multiply when divide false**
   - Given step with rate=Decimal("4.0"), divide=False
   - When `step.apply(Decimal("100"))`
   - Then returns `Decimal("400")`

2. **divide when divide true**
   - Given step with rate=Decimal("4.0"), divide=True
   - When `step.apply(Decimal("100"))`
   - Then returns `Decimal("25")`

3. **preserves Decimal precision**
   - Given step with rate=Decimal("3.14159265358979323846"), divide=False
   - Assert no float conversion happened (result is Decimal and has full precision)

### 2.6 `_RatePath` (tests/unit/test_rate_path.py)

1. **max_tier picks worst across steps**
   - Given path with steps of tiers FRESH, STALE, FRESH
   - Then `path.max_tier == RateTier.STALE`

2. **effective_rate compounds multiply steps**
   - Given two steps: rate=2, divide=False; rate=3, divide=False
   - Then `path.effective_rate == Decimal("6")`

3. **effective_rate compounds with divide**
   - Given two steps: rate=2, divide=False; rate=4, divide=True
   - Then `path.effective_rate == Decimal("0.5")`

4. **apply equivalent to compounded effective rate times amount**
   - Assert `path.apply(amount) == amount * path.effective_rate` for a sample

5. **single-step path behaves as step**
   - Given single step, assert path.apply equals step.apply

### 2.7 `_path_metadata` (tests/unit/test_path_metadata.py)

1. **1-hop metadata structure**
   - Given a path with one FRESH BUY step
   - Then metadata has keys: `path`, `effective_rate`, `hops`, `quality`, `sides`
   - `path` is a list of length 1
   - Each path entry has: `from`, `to`, `source`, `rate_id`, `rate`, `rate_side`, `tier`, `op`
   - `hops == 1`, `quality == "fresh"`, `sides == ["buy"]`

2. **2-hop metadata structure**
   - Given path with FRESH BUY + FRESH SELL
   - Then `path` length 2, `hops == 2`, `sides == ["buy", "sell"]` (sorted)

3. **sides deduped and sorted**
   - Given path with steps of sides SELL, BUY, MID, BUY
   - Then `sides == ["buy", "mid", "sell"]`

4. **quality is max tier**
   - Given steps: FRESH, FRESH, STALE
   - Then `quality == "stale"`

5. **effective_rate as string**
   - Assert `effective_rate` is a string (not Decimal) — JSON-serializable

6. **op field reflects divide flag**
   - `divide=False` → `"op": "multiply"`, `divide=True` → `"op": "divide"`

---

## 3. Unit tests — service with in-memory repo

All tests use the `InMemoryRateRepo` seeded via `add_source` and `add_rate`.
Default transaction time `T = datetime(2025, 6, 1, 12, 0, tzinfo=UTC)`.

### 3.1 Same-currency passthrough (tests/unit/test_service_resolution.py)

1. **target equals source currency passthrough**
   - Given event with `currency_code=UAH`, `amount_cents=12345`
   - And UAH is in `_DISPLAY_CURRENCIES`
   - When convert
   - Then `amounts[UAH] == 12345`
   - And no `rate_uah` key in rate_metadata

### 3.2 1-hop direct resolution

2. **direct fresh from bank**
   - Seed: Monobank PLN/UAH rate FRESH at T
   - Event: Monobank PLN 10000 cents
   - Then `amounts[UAH]` computed via Monobank's rate, metadata `rate_uah.quality == "fresh"`, single step with `source == "monobank"`

3. **direct fresh from fallback when bank lacks rate**
   - Seed: Monobank has no PLN/UAH; NBU has FRESH PLN/UAH
   - Event: Monobank PLN
   - Then path uses NBU, quality fresh

4. **reverse-and-divide when only reverse stored**
   - Seed: Monobank has UAH/PLN FRESH (not PLN/UAH)
   - Event: Monobank PLN
   - Then step has `divide=True`, resulting amount = input / rate

5. **direct preferred over reverse at same source+tier**
   - Seed: Monobank has both PLN/UAH FRESH and UAH/PLN FRESH (different rates)
   - Event: Monobank PLN
   - Then direct PLN/UAH is used (divide=False), assert by `rate_id` or rate value

### 3.3 Tier ordering

6. **fresh any-source beats stale at bank**
   - Seed: Monobank PLN/UAH STALE (last_polled way beyond max_staleness),
           NBU PLN/UAH FRESH
   - Then path uses NBU, quality fresh

7. **stale at bank beats stale at fallback when bank has stale**
   - Seed: Monobank PLN/UAH with `last_polled_at = T - (monobank.max_staleness + 3600s)` (so is_fresh=False for monobank)
   - Seed: NBU PLN/UAH with `last_polled_at = T - (nbu.max_staleness + 3600s)` (so is_fresh=False for nbu)
   - Both rates have valid windows covering T (so CLOSEST tier is not triggered).
   - Then path uses Monobank (source priority within STALE tier), quality stale.
   - Note: both rates must be genuinely STALE per their *own* source's max_staleness, not just "old in general". A rate past monobank's 300s but within nbu's 86400s is FRESH for nbu, and test 3.3 #6 (`fresh any-source beats stale at bank`) covers that path instead.

8. **closest only when no valid-window rate exists anywhere**
   - Seed: Monobank and NBU have rates outside valid window at T;
           those same rates are within 7-day closest window
   - Then path quality closest

9. **per-tier exhaustion short-circuits**
   - Mock repo (use `unittest.mock.AsyncMock`) to verify that once FRESH
     resolves, neither `find_rate_at_time` for STALE check nor
     `find_closest_rate` are called for that pair.

10. **closest ranked by date distance not source order**
    - Seed: Monobank PLN/UAH with `valid_from = T - 6d`, `valid_to = T - 5d` (window does NOT cover T).
    - Seed: NBU PLN/UAH with `valid_from = T - 2d`, `valid_to = T - 1d` (window does NOT cover T).
    - Both rates fall outside any valid window at T, so FRESH/STALE lookups miss for both.
    - Both are within the 7-day closest window.
    - Then path uses NBU (distance ~1–2d beats ~5–6d), even though Monobank is earlier in chain.
    - Metadata source is `"nbu"`, quality `"closest"`.

### 3.4 Source priority within tier

11. **within FRESH bank beats fallback**
    - Seed: Monobank and NBU both FRESH
    - Then Monobank wins

12. **within STALE bank beats fallback**
    - Seed: Monobank and NBU both STALE (no FRESH)
    - Then Monobank wins

13. **within FRESH direction matters per source, not globally**
    - Seed: Monobank has only UAH/PLN FRESH (reverse);
            NBU has PLN/UAH FRESH (direct)
    - Then Monobank reverse wins over NBU direct (source priority beats
      direction preference).

### 3.5 Multi-hop resolution

14. **2-hop via pivot when no 1-hop anywhere**
    - Seed: no PLN/USD or USD/PLN anywhere;
            Monobank PLN/UAH FRESH and UAH/USD FRESH
    - Event: Monobank PLN, display target USD
    - Then path has 2 steps, pivot=UAH, both Monobank, both FRESH

15. **2-hop skips pivot if equal to src or tgt**
    - Seed: pivots list would be [UAH, USD]; no direct UAH/USD; 2-hop
      via USD is nonsensical for target USD
    - Event: Monobank UAH, target USD, with only direct UAH/USD missing
    - Then the USD pivot is skipped in iteration (verify via mock call log
      or via success using an alternative pivot).

16. **2-hop both legs must resolve at same tier**
    - Seed: Monobank PLN/UAH FRESH, Monobank UAH/USD STALE;
            NBU PLN/UAH FRESH, NBU UAH/USD FRESH
    - Then at tier FRESH, Monobank's coherent path fails (leg2 not FRESH);
      NBU's coherent path succeeds. Path is NBU+NBU, not Monobank+Monobank.

17. **1-hop FRESH preferred over 2-hop FRESH**
    - Seed: PLN/USD FRESH direct at ECB; PLN/UAH FRESH + UAH/USD FRESH at Monobank
    - Then 1-hop (ECB direct) wins within FRESH tier before 2-hop is tried.

18. **2-hop FRESH preferred over 1-hop STALE**
    - Seed: PLN/USD STALE at Monobank (direct but stale);
            PLN/UAH FRESH + UAH/USD FRESH at Monobank
    - Then 2-hop FRESH wins, tier-major dominates over hops.

19. **2-hop uses ordered_pivots order**
    - Seed chain: Monobank base=[UAH], NBU base=[EUR,USD]
    - Seed rates: 2-hop works via both UAH and EUR pivots at FRESH
    - Then UAH pivot is chosen (chain-major order)

### 3.6 Rate-side in full paths (tests/unit/test_service_rate_side.py)

20. **1-hop direct uses buy side**
    - Seed Monobank PLN/UAH FRESH with `rate_buy=3.9, rate_mid=4.0, rate_sell=4.1`
    - Event PLN 10000
    - Then `amounts[UAH]` computed with rate_buy (39000 cents)
    - Metadata `path[0].rate_side == "buy"`

21. **1-hop reverse uses sell side**
    - Seed Monobank UAH/PLN FRESH with `rate_buy=0.24, rate_mid=0.25, rate_sell=0.26`
    - Event PLN 10000
    - Then step has divide=True, rate_sell used (10000 / 0.26 ≈ 38462 cents)
    - Metadata `path[0].rate_side == "sell"`

22. **2-hop leg1 direct + leg2 direct both buy**
    - Seed Monobank PLN/UAH and UAH/USD both FRESH with buy/sell
    - Event PLN, target USD
    - Then both steps rate_side=buy, `sides == ["buy"]`

23. **2-hop leg1 reverse + leg2 direct**
    - Seed Monobank UAH/PLN (for leg1 divide=True) and UAH/USD FRESH
    - Then sides=["buy", "sell"]

24. **falls back to mid when bank row has NULL buy**
    - Seed Monobank PLN/UAH with rate_buy=None, rate_mid=4.0
    - Then step rate=4.0, rate_side=mid
    - Metadata `sides == ["mid"]`

25. **never falls to opposite side**
    - Seed Monobank PLN/UAH with rate_buy=None, rate_sell=4.1, rate_mid=4.0
    - Event PLN direct (divide=False)
    - Then step rate=4.0 (mid), not 4.1 (sell)

26. **compounded spread in 2-hop multi-hop**
    - Seed PLN/UAH `buy=3.9, mid=4.0, sell=4.1`; UAH/USD `buy=0.025, mid=0.026, sell=0.027`
    - Event PLN 10000 cents, target USD
    - Then effective_rate = 3.9 * 0.025 = 0.0975 (both buy sides)
    - Amount = 10000 * 0.0975 = 975 cents
    - Metadata quality=fresh, sides=["buy"]

### 3.7 No path

27. **no path returns None amount**
    - Seed nothing
    - Event Monobank PLN
    - Then `amounts[UAH] is None`
    - And `"rate_uah"` key absent from rate_metadata
    - Log warning issued (capture with `caplog`)

28. **one target resolvable one not**
    - Seed Monobank PLN/UAH FRESH only
    - Event Monobank PLN
    - Then `amounts[UAH]` populated, `amounts[USD]` is None, `amounts[EUR]` is None

### 3.8 Metadata shape (tests/unit/test_service_metadata.py)

29. **metadata key naming**
    - For successful target UAH: key is `"rate_uah"` (lowercase)
    - For target USD: `"rate_usd"`, EUR: `"rate_eur"`

30. **metadata quality matches path max tier**
    - FRESH+FRESH → "fresh"; FRESH+STALE → "stale"; CLOSEST → "closest"

31. **metadata hops matches step count**
    - 1-hop → 1, 2-hop → 2

32. **metadata effective_rate is stringified Decimal**
    - Assert `isinstance(meta["effective_rate"], str)` and parseable as Decimal

33. **rate_metadata empty when only passthrough (all same-currency)**
    - Not reachable — display set has 3 currencies, only 1 can match source.
    - Instead: event.currency_code=UAH → rate_metadata does not contain rate_uah
      (passthrough skips) but may contain rate_usd, rate_eur.

### 3.9 STALE detection (tests/unit/test_service_resolution.py, continued)

34. **rate within max_staleness is FRESH**
    - Seed source with max_staleness=300; rate with last_polled = T - 60s
    - Then path tier is FRESH

35. **rate beyond max_staleness is STALE**
    - Seed source with max_staleness=300; rate with last_polled = T - 3600s
    - And seed this is the only rate
    - Then path tier is STALE

36. **at_time before last_polled_at treated as fresh**
    - Seed source with max_staleness=300; rate with last_polled = T + 100s
    - Then path tier is FRESH (backfill scenario)

37. **rate with NULL last_polled_at skipped at all tiers**
    - Seed Monobank rate with last_polled_at=None; NBU rate FRESH
    - Then path uses NBU, not Monobank (the NULL-polled row is ignored at FRESH/STALE
      AND at CLOSEST — production SQL has `last_polled_at IS NOT NULL` on the closest-rate query)

38. **NULL last_polled_at excluded from CLOSEST tier**
    - Seed only Monobank rate with NULL last_polled_at AND valid window NOT covering at_time
    - Then no path resolves — `amounts[target]` is None
    - This confirms that NULL-polled rows are excluded from all tiers, not just FRESH/STALE

---

## 4. Integration tests — repo layer

All integration tests use the `conn` fixture (real DB, rolled-back
transaction). Seed data using `insert_source_config` and `insert_rate`
helpers.

### 4.1 `find_rate_at_time` (tests/integration/test_repo_find_rate_at_time.py)

1. **returns matching row by source+pair+time**
   - Insert rate: source=monobank, PLN/UAH, valid_from=T-1h, valid_to=NULL, rate_mid=4.0
   - Call `find_rate_at_time(conn, "monobank", "PLN", "UAH", T)`
   - Expect RateRow with rate_mid=Decimal("4.0"), correct source, id matches

2. **picks latest valid_from when multiple cover time**
   - Insert two rates for same pair: valid_from at T-2h and T-30m, both valid_to=NULL
   - Then returns row with valid_from = T-30m

3. **NULL valid_to is open-ended**
   - Insert rate valid_from = T - 10 days, valid_to=NULL
   - Call at T
   - Expect a hit

4. **finite valid_to excludes time after**
   - Insert rate valid_from = T - 2h, valid_to = T - 1h
   - Call at T
   - Expect None

5. **returns None for nonexistent pair**
   - No rate seeded
   - Returns None

6. **returns rate_buy and rate_sell**
   - Insert rate with rate_buy=3.9, rate_sell=4.1
   - Returned RateRow has rate_buy=Decimal("3.9"), rate_sell=Decimal("4.1")

7. **NULL rate_buy and rate_sell returned as None**
   - Insert rate with those columns NULL
   - RateRow has rate_buy=None, rate_sell=None

8. **returns last_polled_at**
   - Insert rate with last_polled_at = T - 60s
   - RateRow has last_polled_at matching (timezone-aware)

9. **filters by source correctly**
   - Insert monobank and nbu rates for same pair
   - Call with source=nbu → returns nbu's row, not monobank's

### 4.2 `find_closest_rate` (tests/integration/test_repo_find_closest_rate.py)

1. **picks closest by valid_from distance**
   - Insert two rates (same source, pair, no valid_to covering T):
     valid_from = T - 2 days and T - 5 days
   - Then returns T - 2 days rate

2. **considers future rates**
   - Insert rate with valid_from = T + 2 days (same source/pair)
   - And another at valid_from = T - 5 days
   - Then returns T + 2 days rate (closer)

3. **respects source ANY filter**
   - Insert monobank and nbu rates both within window
   - Call with sources=["nbu"]
   - Returns nbu rate only (monobank excluded even if closer)

4. **returns None beyond 7-day window**
   - Insert rate at valid_from = T - 10 days, no others
   - Returns None

5. **exactly at 7-day boundary**
   - Insert rate at valid_from = T - 7 days
   - Whether it's included is controlled by `<=` in SQL: assert behavior matches
     (current: `<= _MAX_CLOSEST_RATE_AGE_SECONDS` is inclusive)

6. **tie in distance is deterministic within a test run**
   - Insert two rates equidistant from T (T-1d and T+1d), same source
   - Call twice, assert both calls return the same row (document: whichever
     Postgres picks, it must be stable within a session)

### 4.3 `load_source_chain` (tests/integration/test_repo_load_source_chain.py)

1. **single source no fallback**
   - Insert config: source=monobank, fallback_source=NULL
   - Returns length-1 list with monobank

2. **two-source chain**
   - Insert monobank→nbu, nbu→NULL
   - `load_source_chain(conn, "monobank")` returns [monobank, nbu] in order

3. **three-source chain**
   - monobank→nbu, nbu→ecb, ecb→NULL
   - Returns [monobank, nbu, ecb]

4. **ordered by depth**
   - Same three-source chain; assert the returned list order exactly matches
     chain depth (not alphabetical, not insertion order)

5. **chain of depth exactly 10 does not raise**
   - Insert 10 configs: s1→s2→…→s10→NULL
   - Call `load_source_chain(conn, "s1")`
   - Returns 10 entries, no exception

6. **chain exceeding depth cap raises RateSourceChainError**
   - Insert 11 configs: s1→s2→…→s11→NULL
   - Call `load_source_chain(conn, "s1")`
   - Raises `RateSourceChainError`
   - Error message contains "depth cap", "10", and all 10 source names

7. **cyclic chain raises RateSourceChainError**
   - Insert: a→b, b→c, c→a
   - Call `load_source_chain(conn, "a")`
   - Raises `RateSourceChainError`
   - Why: `UNION ALL` in the recursive CTE does not deduplicate, so the query walks `a → b → c → a → b → c → …` until `depth < $2` blocks further recursion at depth 10. The last row has `depth=10` and `fallback_source='b'` (not NULL), which trips the post-fetch check.
   - Expected chain in error message: 10 entries alternating `['a', 'b', 'c', 'a', 'b', 'c', 'a', 'b', 'c', 'a']`.
   - Assert: all three source names (`'a'`, `'b'`, `'c'`) appear in the error message (containment check, not a count). Do NOT assert the chain length is 3.

8. **unknown entry source returns empty list**
   - Call with a source string not in rate_source_config
   - Returns `[]` (and does not raise)

9. **base_currencies preserved in order**
   - Insert config with base_currencies=ARRAY['UAH','EUR','USD']
   - Loaded SourceConfig has `base_currencies == ['UAH', 'EUR', 'USD']` in that order

10. **max_staleness_seconds round-trips**
    - Insert config with max_staleness_seconds=1234
    - Loaded SourceConfig has the same value

---

## 5. Integration tests — end-to-end (tests/integration/test_service_end_to_end.py)

Each test seeds `rate_source_config` and `currency_rates` then calls
`service.convert(conn, event)` and asserts on `ConversionResult`.

Fixed chain used across tests unless otherwise noted:
```
monobank (max_staleness=300s, base=['UAH']) → nbu (max_staleness=86400s, base=['UAH','EUR','USD']) → NULL
```

T = `datetime(2025, 6, 1, 12, 0, tzinfo=UTC)`.

1. **passthrough when event currency in display set**
   - Event: source=monobank, currency=UAH, amount=55500
   - Seed monobank UAH/USD FRESH, UAH/EUR FRESH
   - Assert `amounts[UAH] == 55500`
   - Assert `amounts[USD]` and `amounts[EUR]` computed from seeded rates
   - Assert rate_metadata has `rate_usd` and `rate_eur` but no `rate_uah`

2. **Monobank PLN direct to UAH, 2-hop to USD and EUR via UAH pivot**
   - Seed monobank PLN/UAH FRESH (rate_mid=11.0, rate_buy=10.9, rate_sell=11.1)
   - Seed monobank UAH/USD FRESH (rate_mid=0.025, rate_buy=0.024, rate_sell=0.026)
   - Seed monobank UAH/EUR FRESH (rate_mid=0.023, rate_buy=0.022, rate_sell=0.024)
   - Event PLN 10000 cents
   - Expected amounts: UAH = 10000*10.9 = 109000; USD = 10000*10.9*0.024 = 2616; EUR = 10000*10.9*0.022 = 2398
   - Metadata `rate_uah.hops == 1`, `rate_usd.hops == 2`, `rate_eur.hops == 2`
   - All sides "buy", all quality "fresh"

3. **reverse direction when only reverse stored**
   - Seed monobank UAH/PLN FRESH (rate_mid=0.09, rate_buy=0.089, rate_sell=0.091)
     — note: no PLN/UAH stored
   - Event PLN 10000
   - Expected UAH = 10000 / 0.091 = 109890 (ROUND_HALF_UP)
   - Metadata `path[0].op == "divide"`, `rate_side == "sell"`

4. **falls back to nbu when monobank lacks rate**
   - Seed only nbu PLN/UAH FRESH (rate_mid=11.0, rate_buy=NULL, rate_sell=NULL)
   - No monobank rate
   - Event monobank PLN 10000
   - Expected UAH = 10000 * 11.0 = 110000 (mid used since buy NULL)
   - Metadata `source == "nbu"`, `rate_side == "mid"`, quality `"fresh"`

5. **STALE tier when last_polled_at beyond max_staleness**
   - Seed monobank PLN/UAH valid window covers T, last_polled_at = T - 3600s
     (max_staleness=300s → stale)
   - No nbu rate
   - Event monobank PLN 10000
   - Expected amounts still computed
   - Metadata quality `"stale"`

6. **fresh nbu preferred over stale monobank at same pair**
   - Seed monobank PLN/UAH STALE (last_polled = T - 3600s), rate_mid=11.0
   - Seed nbu PLN/UAH FRESH (last_polled = T - 60s), rate_mid=10.5
   - Event monobank PLN 10000
   - Expected UAH uses 10.5 (nbu fresh), not 11.0 (monobank stale)
   - Metadata quality `"fresh"`, source `"nbu"`

7. **CLOSEST fallback when no valid window**
   - Seed monobank PLN/UAH valid_from = T - 2d, valid_to = T - 1d, rate_mid=11.0
     (window does not cover T)
   - No other rates
   - Event monobank PLN 10000 at T
   - Expected amount computed using that rate
   - Metadata quality `"closest"`, warning log emitted (verify via `caplog`)

8. **no path for a target yields None amount and no metadata entry**
   - Seed monobank PLN/UAH FRESH only (nothing for USD or EUR)
   - Event monobank PLN
   - Assert `amounts[UAH]` populated, `amounts[USD] is None`, `amounts[EUR] is None`
   - Assert `"rate_usd"` and `"rate_eur"` not in rate_metadata

9. **rate_buy/rate_sell produce different amounts than rate_mid-only**
   - Scenario A seed: monobank PLN/UAH with rate_buy=10.9, rate_mid=11.0, rate_sell=11.1
   - Scenario B seed: same pair but rate_buy=NULL, rate_sell=NULL, rate_mid=11.0
   - Run service on each, compare `amounts[UAH]`:
     - A: 10000 * 10.9 = 109000, side=buy
     - B: 10000 * 11.0 = 110000, side=mid
   - Assert both differ and sides differ in metadata

10. **2-hop tier coherence: both legs must share tier**
    - Seed monobank PLN/UAH FRESH, monobank UAH/USD STALE
    - Seed nbu PLN/UAH FRESH, nbu UAH/USD FRESH
    - Event monobank PLN → USD
    - Expected: nbu+nbu both FRESH path used (not mixed monobank+monobank spanning FRESH+STALE)
    - Metadata path source list == ["nbu", "nbu"], quality fresh

11. **1-hop stale beats 2-hop closest only if both at STALE tier match, tier-major**
    - Seed monobank PLN/USD STALE direct
    - Seed monobank PLN/UAH and UAH/USD CLOSEST-only (valid window outside T)
    - Then STALE direct wins (tier beats hops)

12. **empty chain behavior**
    - Event with source that has no config row
    - `load_source_chain` returns `[]`
    - Service calls; all `amounts` None (except passthrough for event.currency_code)
    - No crash

13. **multiple display currencies all computed independently**
    - Seed full set; confirm all three targets resolved with independent paths
    - Assert metadata has rate_uah/usd/eur keys (except passthrough one)
    - Assert amounts consistent with seeded rates

14. **source chain is traversed in order**
    - Insert 3 configs monobank→nbu→ecb, all three have PLN/UAH FRESH with distinct rates
    - Event monobank PLN
    - Expected: monobank wins; verify by rate value or by metadata source

15. **rounding ROUND_HALF_UP applied to cents**
    - Seed rate that produces exactly .5 in the last cent place
      (e.g. rate chosen so amount_cents * rate = 12345.5)
    - Assert final integer cents is 12346, not 12345

16. **very large amount no overflow**
    - Event with amount_cents = 10**15
    - Seed rate 1.0
    - Result equals 10**15 (int, not truncated)

17. **timezone-naive event.time handled**
    - Event with `time = datetime(2025, 6, 1, 12, 0)` (no tzinfo)
    - Seed rate with tz-aware validity covering T
    - Service should treat naive as UTC via `_ensure_tz`

18. **effective_rate and apply agree on final amount**
    - For each successful target in several scenarios (at minimum: 1-hop multiply, 1-hop divide, 2-hop multiply+multiply, 2-hop multiply+divide): parse `effective_rate` from metadata, multiply by `event.amount_cents`, quantize with ROUND_HALF_UP, assert equals `amounts[target]`.
    - Why this matters: `_RatePath.effective_rate` compounds steps starting from `Decimal(1)` while `_RatePath.apply(amount)` compounds starting from the actual amount. For multiply-only paths these are algebraically identical. For paths containing divide steps they can diverge at the last digit of Decimal precision in pathological cases (28-digit context, values near a quantize boundary). This test is the canary: if metadata's `effective_rate` ever stops reconstructing the stored amount, the divergence shows up here.
    - Run as a parametrized test across at least 4 scenarios covering each compounding shape.

19. **RateSourceChainError propagates (does not swallow)**
    - Seed cyclic config a→b→a
    - Event with source=a
    - Assert `service.convert(conn, event)` raises `RateSourceChainError`
      (the service must not catch it)

20. **sides aggregation reflects multi-hop side mix**
    - Seed monobank PLN/UAH FRESH (direct; buy/sell populated).
    - Seed monobank USD/UAH FRESH (note: USD/UAH, not UAH/USD — so leg2 resolves via divide=True).
    - Event monobank PLN, target USD.
    - Path: leg1 PLN/UAH direct (rate_side=buy); leg2 UAH→USD computed as reverse of USD/UAH (divide=True, rate_side=sell).
    - Metadata `sides == ["buy", "sell"]` (sorted, deduped).

---

## 6. Test hygiene

- Every test uses `pytest.mark.asyncio` (or module-level `pytestmark = pytest.mark.asyncio`).
- Integration tests never share state between functions — rely on the `conn`
  fixture's per-test transaction rollback.
- Use `caplog` for log-assertion tests; never parse stdout.
- Don't assert on exact floating-point amounts — use `Decimal` throughout.
- Parametrize where it genuinely compresses the code (e.g. test 18's
  "metadata matches amount" loop), otherwise keep tests explicit and readable.
- For mock-based tests (3.3 #9, 3.5 #15), use `unittest.mock.AsyncMock` on
  repo methods and assert `call_count` / `call_args_list`.

## 7. Coverage target

After implementation, run `pytest --cov=grosh_consumer.services.currency_conversion_service --cov=grosh_consumer.repositories.currency_rate_repo --cov-branch`. Target >= 95% line coverage and >= 90% branch coverage on both modules. Any uncovered line should be explained inline in a PR comment, not silently left uncovered.
