"""Integration tests for CurrencyConversionService.convert — end-to-end.

Section 5 — 20 test scenarios (some split into two functions).

Default chain:
    monobank (max_staleness=300, base=['UAH'])
      → nbu (max_staleness=86400, base=['UAH','EUR','USD'])
      → NULL

T = datetime(2025, 6, 1, 12, 0, tzinfo=UTC)

Time constants:
    FRESH_POLLED       = T - 60s        (within monobank 300s max_staleness)
    STALE_POLLED_MONO  = T - (300+3600)s  (beyond monobank, within nbu 86400s)
    STALE_POLLED_NBU   = T - (86400+3600)s
    VALID_FROM         = T - 1h, valid_to = None
"""

import logging
from datetime import timedelta
from decimal import ROUND_HALF_UP, Decimal

import asyncpg
import pytest

from grosh_consumer.repositories.currency_rate_repo import RateSourceChainError
from grosh_consumer.services.currency_conversion_service import (
    CurrencyConversionService,
)
from tests.helpers import T, make_event
from tests.integration.helpers import insert_rate, insert_source_config

VALID_FROM = T - timedelta(hours=1)
FRESH_POLLED = T - timedelta(seconds=60)
STALE_POLLED_MONO = T - timedelta(seconds=300 + 3600)
STALE_POLLED_NBU = T - timedelta(seconds=86400 + 3600)


async def _seed_default_chain(conn: asyncpg.Connection) -> None:
    """Insert the standard monobank → nbu chain used by most tests."""
    await insert_source_config(
        conn,
        source="nbu",
        fallback_source=None,
        max_staleness_seconds=86400,
        base_currencies=["UAH", "EUR", "USD"],
    )
    await insert_source_config(
        conn,
        source="monobank",
        fallback_source="nbu",
        max_staleness_seconds=300,
        base_currencies=["UAH"],
    )


# ---------------------------------------------------------------------------
# Test 1: Same-currency passthrough — UAH event, no rate needed
# ---------------------------------------------------------------------------


async def test_1_same_currency_passthrough(
    conn: asyncpg.Connection,
    service: CurrencyConversionService,
) -> None:
    await _seed_default_chain(conn)
    event = make_event(source="monobank", currency_code="UAH", amount_cents=50000)

    result = await service.convert(conn, event)

    assert result.amounts["UAH"] == 50000
    assert "rate_uah" not in result.rate_metadata


# ---------------------------------------------------------------------------
# Test 2: Direct FRESH 1-hop — Monobank PLN/UAH direct, fresh polled
# ---------------------------------------------------------------------------


async def test_2_direct_fresh_1_hop(
    conn: asyncpg.Connection,
    service: CurrencyConversionService,
) -> None:
    await _seed_default_chain(conn)
    await insert_rate(
        conn,
        source="monobank",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=4.0,
        rate_buy=3.9,
        rate_sell=4.1,
        valid_from=VALID_FROM,
        last_polled_at=FRESH_POLLED,
    )

    result = await service.convert(
        conn, make_event(currency_code="PLN", amount_cents=10000)
    )

    meta = result.rate_metadata["rate_uah"]
    assert meta["quality"] == "fresh"
    assert meta["hops"] == 1
    assert meta["path"][0]["source"] == "monobank"
    assert meta["path"][0]["op"] == "multiply"
    assert result.amounts["UAH"] == 39000  # 10000 * 3.9


# ---------------------------------------------------------------------------
# Test 3: Reverse FRESH 1-hop — UAH/PLN stored, divide applied
# ---------------------------------------------------------------------------


async def test_3_reverse_fresh_1_hop(
    conn: asyncpg.Connection,
    service: CurrencyConversionService,
) -> None:
    await _seed_default_chain(conn)
    await insert_rate(
        conn,
        source="monobank",
        currency_from="UAH",
        currency_to="PLN",
        rate_mid=Decimal("0.25"),
        rate_buy=Decimal("0.24"),
        rate_sell=Decimal("0.26"),
        valid_from=VALID_FROM,
        last_polled_at=FRESH_POLLED,
    )

    result = await service.convert(
        conn, make_event(currency_code="PLN", amount_cents=10000)
    )

    meta = result.rate_metadata["rate_uah"]
    assert meta["path"][0]["op"] == "divide"
    assert meta["path"][0]["rate_side"] == "sell"
    # 10000 / 0.26 = 38461.538... → ROUND_HALF_UP → 38462
    expected = int(
        (Decimal("10000") / Decimal("0.26")).quantize(
            Decimal(0), rounding=ROUND_HALF_UP
        )
    )
    assert result.amounts["UAH"] == expected


# ---------------------------------------------------------------------------
# Test 4: Fallback to NBU when Monobank has no rate
# ---------------------------------------------------------------------------


async def test_4_fallback_to_nbu_when_monobank_missing(
    conn: asyncpg.Connection,
    service: CurrencyConversionService,
) -> None:
    await _seed_default_chain(conn)
    await insert_rate(
        conn,
        source="nbu",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=Decimal("4.5"),
        valid_from=VALID_FROM,
        last_polled_at=FRESH_POLLED,
    )

    result = await service.convert(
        conn, make_event(currency_code="PLN", amount_cents=10000)
    )

    meta = result.rate_metadata["rate_uah"]
    assert meta["quality"] == "fresh"
    assert meta["path"][0]["source"] == "nbu"


# ---------------------------------------------------------------------------
# Test 5: FRESH beats STALE — NBU fresh overrides Monobank stale
# ---------------------------------------------------------------------------


async def test_5_fresh_beats_stale(
    conn: asyncpg.Connection,
    service: CurrencyConversionService,
) -> None:
    await _seed_default_chain(conn)
    # Monobank stale
    await insert_rate(
        conn,
        source="monobank",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=Decimal("4.0"),
        rate_buy=Decimal("3.9"),
        rate_sell=Decimal("4.1"),
        valid_from=VALID_FROM,
        last_polled_at=STALE_POLLED_MONO,
    )
    # NBU fresh
    await insert_rate(
        conn,
        source="nbu",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=Decimal("4.5"),
        valid_from=VALID_FROM,
        last_polled_at=FRESH_POLLED,
    )

    result = await service.convert(conn, make_event(currency_code="PLN"))

    meta = result.rate_metadata["rate_uah"]
    assert meta["quality"] == "fresh"
    assert meta["path"][0]["source"] == "nbu"


# ---------------------------------------------------------------------------
# Test 6: STALE resolution — Monobank stale returned when nothing fresh
# ---------------------------------------------------------------------------


async def test_6_stale_resolution(
    conn: asyncpg.Connection,
    service: CurrencyConversionService,
) -> None:
    await _seed_default_chain(conn)
    await insert_rate(
        conn,
        source="monobank",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=Decimal("4.0"),
        rate_buy=Decimal("3.9"),
        rate_sell=Decimal("4.1"),
        valid_from=VALID_FROM,
        last_polled_at=STALE_POLLED_MONO,
    )

    result = await service.convert(conn, make_event(currency_code="PLN"))

    meta = result.rate_metadata["rate_uah"]
    assert meta["quality"] == "stale"
    assert meta["path"][0]["source"] == "monobank"


# ---------------------------------------------------------------------------
# Test 7: CLOSEST fallback — rate outside validity window
# ---------------------------------------------------------------------------


async def test_7_closest_fallback_with_log(
    conn: asyncpg.Connection,
    service: CurrencyConversionService,
    caplog: pytest.LogCaptureFixture,
) -> None:
    await _seed_default_chain(conn)
    # Rate window does NOT cover T
    await insert_rate(
        conn,
        source="monobank",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=Decimal("4.0"),
        valid_from=T - timedelta(days=2),
        valid_to=T - timedelta(days=1),
        last_polled_at=T - timedelta(days=2),
    )

    with caplog.at_level(logging.WARNING):
        result = await service.convert(conn, make_event(currency_code="PLN"))

    meta = result.rate_metadata["rate_uah"]
    assert meta["quality"] == "closest"
    assert any("PLN" in r.message and "UAH" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# Test 8: No rate path — amount is None
# ---------------------------------------------------------------------------


async def test_8_no_rate_path_returns_none(
    conn: asyncpg.Connection,
    service: CurrencyConversionService,
) -> None:
    await _seed_default_chain(conn)
    # No rates seeded at all

    result = await service.convert(conn, make_event(currency_code="PLN"))

    assert result.amounts["UAH"] is None
    assert "rate_uah" not in result.rate_metadata


# ---------------------------------------------------------------------------
# Test 9a: rate_buy used on direct leg
# ---------------------------------------------------------------------------


async def test_9a_rate_buy_used_on_direct_leg(
    conn: asyncpg.Connection,
    service: CurrencyConversionService,
) -> None:
    await _seed_default_chain(conn)
    await insert_rate(
        conn,
        source="monobank",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=Decimal("4.0"),
        rate_buy=Decimal("3.9"),
        rate_sell=Decimal("4.1"),
        valid_from=VALID_FROM,
        last_polled_at=FRESH_POLLED,
    )

    result = await service.convert(
        conn, make_event(currency_code="PLN", amount_cents=10000)
    )

    meta = result.rate_metadata["rate_uah"]
    assert meta["path"][0]["rate_side"] == "buy"
    assert result.amounts["UAH"] == 39000


# ---------------------------------------------------------------------------
# Test 9b: rate_sell used on reverse (divide) leg
# ---------------------------------------------------------------------------


async def test_9b_rate_sell_used_on_reverse_leg(
    conn: asyncpg.Connection,
    service: CurrencyConversionService,
) -> None:
    await _seed_default_chain(conn)
    await insert_rate(
        conn,
        source="monobank",
        currency_from="UAH",
        currency_to="PLN",
        rate_mid=Decimal("0.25"),
        rate_buy=Decimal("0.24"),
        rate_sell=Decimal("0.26"),
        valid_from=VALID_FROM,
        last_polled_at=FRESH_POLLED,
    )

    result = await service.convert(
        conn, make_event(currency_code="PLN", amount_cents=10000)
    )

    meta = result.rate_metadata["rate_uah"]
    assert meta["path"][0]["rate_side"] == "sell"
    assert meta["path"][0]["op"] == "divide"


# ---------------------------------------------------------------------------
# Test 10: Tier coherence — both legs must be at same tier
#          Monobank both legs STALE, NBU both FRESH → FRESH path uses NBU
# ---------------------------------------------------------------------------


async def test_10_tier_coherence_both_legs_same_tier(
    conn: asyncpg.Connection,
    service: CurrencyConversionService,
) -> None:
    await _seed_default_chain(conn)
    # Monobank: BOTH legs STALE
    await insert_rate(
        conn,
        source="monobank",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=Decimal("4.0"),
        rate_buy=Decimal("3.9"),
        rate_sell=Decimal("4.1"),
        valid_from=VALID_FROM,
        last_polled_at=STALE_POLLED_MONO,
    )
    await insert_rate(
        conn,
        source="monobank",
        currency_from="UAH",
        currency_to="USD",
        rate_mid=Decimal("0.025"),
        rate_buy=Decimal("0.024"),
        rate_sell=Decimal("0.026"),
        valid_from=VALID_FROM,
        last_polled_at=STALE_POLLED_MONO,
    )
    # NBU: BOTH legs FRESH
    await insert_rate(
        conn,
        source="nbu",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=Decimal("4.5"),
        valid_from=VALID_FROM,
        last_polled_at=FRESH_POLLED,
    )
    await insert_rate(
        conn,
        source="nbu",
        currency_from="UAH",
        currency_to="USD",
        rate_mid=Decimal("0.026"),
        valid_from=VALID_FROM,
        last_polled_at=FRESH_POLLED,
    )

    result = await service.convert(conn, make_event(currency_code="PLN"))

    meta = result.rate_metadata["rate_usd"]
    assert meta["quality"] == "fresh"
    assert meta["path"][0]["source"] == "nbu"
    assert meta["path"][1]["source"] == "nbu"


# ---------------------------------------------------------------------------
# Test 11: 2-hop via UAH pivot — PLN→UAH→USD
# ---------------------------------------------------------------------------


async def test_11_two_hop_via_uah_pivot(
    conn: asyncpg.Connection,
    service: CurrencyConversionService,
) -> None:
    await _seed_default_chain(conn)
    await insert_rate(
        conn,
        source="monobank",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=Decimal("4.0"),
        rate_buy=Decimal("3.9"),
        rate_sell=Decimal("4.1"),
        valid_from=VALID_FROM,
        last_polled_at=FRESH_POLLED,
    )
    await insert_rate(
        conn,
        source="monobank",
        currency_from="UAH",
        currency_to="USD",
        rate_mid=Decimal("0.025"),
        rate_buy=Decimal("0.024"),
        rate_sell=Decimal("0.026"),
        valid_from=VALID_FROM,
        last_polled_at=FRESH_POLLED,
    )

    result = await service.convert(conn, make_event(currency_code="PLN"))

    meta = result.rate_metadata["rate_usd"]
    assert meta["hops"] == 2
    assert meta["quality"] == "fresh"
    assert meta["path"][0]["from"] == "PLN"
    assert meta["path"][0]["to"] == "UAH"
    assert meta["path"][1]["from"] == "UAH"
    assert meta["path"][1]["to"] == "USD"


# ---------------------------------------------------------------------------
# Test 12: 1-hop preferred over 2-hop at same tier
# ---------------------------------------------------------------------------


async def test_12_one_hop_preferred_over_two_hop(
    conn: asyncpg.Connection,
    service: CurrencyConversionService,
) -> None:
    await _seed_default_chain(conn)
    # Direct PLN/USD (1-hop)
    await insert_rate(
        conn,
        source="nbu",
        currency_from="PLN",
        currency_to="USD",
        rate_mid=Decimal("0.23"),
        valid_from=VALID_FROM,
        last_polled_at=FRESH_POLLED,
    )
    # 2-hop via UAH
    await insert_rate(
        conn,
        source="monobank",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=Decimal("4.0"),
        rate_buy=Decimal("3.9"),
        rate_sell=Decimal("4.1"),
        valid_from=VALID_FROM,
        last_polled_at=FRESH_POLLED,
    )
    await insert_rate(
        conn,
        source="monobank",
        currency_from="UAH",
        currency_to="USD",
        rate_mid=Decimal("0.025"),
        rate_buy=Decimal("0.024"),
        rate_sell=Decimal("0.026"),
        valid_from=VALID_FROM,
        last_polled_at=FRESH_POLLED,
    )

    result = await service.convert(conn, make_event(currency_code="PLN"))

    meta = result.rate_metadata["rate_usd"]
    assert meta["hops"] == 1


# ---------------------------------------------------------------------------
# Test 13: 2-hop FRESH beats 1-hop STALE
# ---------------------------------------------------------------------------


async def test_13_two_hop_fresh_beats_one_hop_stale(
    conn: asyncpg.Connection,
    service: CurrencyConversionService,
) -> None:
    await _seed_default_chain(conn)
    # Direct PLN/USD — STALE for Monobank
    await insert_rate(
        conn,
        source="monobank",
        currency_from="PLN",
        currency_to="USD",
        rate_mid=Decimal("0.23"),
        valid_from=VALID_FROM,
        last_polled_at=STALE_POLLED_MONO,
    )
    # 2-hop via UAH — both FRESH
    await insert_rate(
        conn,
        source="monobank",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=Decimal("4.0"),
        rate_buy=Decimal("3.9"),
        rate_sell=Decimal("4.1"),
        valid_from=VALID_FROM,
        last_polled_at=FRESH_POLLED,
    )
    await insert_rate(
        conn,
        source="monobank",
        currency_from="UAH",
        currency_to="USD",
        rate_mid=Decimal("0.025"),
        rate_buy=Decimal("0.024"),
        rate_sell=Decimal("0.026"),
        valid_from=VALID_FROM,
        last_polled_at=FRESH_POLLED,
    )

    result = await service.convert(conn, make_event(currency_code="PLN"))

    meta = result.rate_metadata["rate_usd"]
    assert meta["quality"] == "fresh"
    assert meta["hops"] == 2


# ---------------------------------------------------------------------------
# Test 14: Pivot order — UAH before EUR (Monobank depth 1, NBU depth 2)
# ---------------------------------------------------------------------------


async def test_14_pivot_order_uah_before_eur(
    conn: asyncpg.Connection,
    service: CurrencyConversionService,
) -> None:
    await _seed_default_chain(conn)
    # UAH pivot path (monobank depth=1, so UAH pivot listed first)
    await insert_rate(
        conn,
        source="monobank",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=Decimal("4.0"),
        rate_buy=Decimal("3.9"),
        rate_sell=Decimal("4.1"),
        valid_from=VALID_FROM,
        last_polled_at=FRESH_POLLED,
    )
    await insert_rate(
        conn,
        source="monobank",
        currency_from="UAH",
        currency_to="USD",
        rate_mid=Decimal("0.025"),
        rate_buy=Decimal("0.024"),
        rate_sell=Decimal("0.026"),
        valid_from=VALID_FROM,
        last_polled_at=FRESH_POLLED,
    )
    # EUR pivot path via NBU
    await insert_rate(
        conn,
        source="nbu",
        currency_from="PLN",
        currency_to="EUR",
        rate_mid=Decimal("0.23"),
        valid_from=VALID_FROM,
        last_polled_at=FRESH_POLLED,
    )
    await insert_rate(
        conn,
        source="nbu",
        currency_from="EUR",
        currency_to="USD",
        rate_mid=Decimal("1.1"),
        valid_from=VALID_FROM,
        last_polled_at=FRESH_POLLED,
    )

    result = await service.convert(conn, make_event(currency_code="PLN"))

    meta = result.rate_metadata["rate_usd"]
    # UAH pivot chosen first (monobank is depth 1, provides UAH pivot)
    assert meta["path"][0]["to"] == "UAH"


# ---------------------------------------------------------------------------
# Test 15: mid fallback when buy/sell is NULL
# ---------------------------------------------------------------------------


async def test_15_mid_fallback_when_buy_null(
    conn: asyncpg.Connection,
    service: CurrencyConversionService,
) -> None:
    await _seed_default_chain(conn)
    await insert_rate(
        conn,
        source="nbu",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=Decimal("4.0"),
        rate_buy=None,
        rate_sell=None,
        valid_from=VALID_FROM,
        last_polled_at=FRESH_POLLED,
    )

    result = await service.convert(
        conn, make_event(currency_code="PLN", amount_cents=10000)
    )

    meta = result.rate_metadata["rate_uah"]
    assert meta["path"][0]["rate_side"] == "mid"
    assert result.amounts["UAH"] == 40000


# ---------------------------------------------------------------------------
# Test 16: Partial resolution — UAH resolved, USD not
# ---------------------------------------------------------------------------


async def test_16_partial_resolution(
    conn: asyncpg.Connection,
    service: CurrencyConversionService,
) -> None:
    await _seed_default_chain(conn)
    await insert_rate(
        conn,
        source="monobank",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=Decimal("4.0"),
        rate_buy=Decimal("3.9"),
        rate_sell=Decimal("4.1"),
        valid_from=VALID_FROM,
        last_polled_at=FRESH_POLLED,
    )

    result = await service.convert(conn, make_event(currency_code="PLN"))

    assert result.amounts["UAH"] is not None
    assert result.amounts["USD"] is None
    assert result.amounts["EUR"] is None


# ---------------------------------------------------------------------------
# Test 17: Backfill — rate polled after T still treated as FRESH
# ---------------------------------------------------------------------------


async def test_17_backfill_rate_polled_after_transaction_is_fresh(
    conn: asyncpg.Connection,
    service: CurrencyConversionService,
) -> None:
    await _seed_default_chain(conn)
    await insert_rate(
        conn,
        source="monobank",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=Decimal("4.0"),
        rate_buy=Decimal("3.9"),
        rate_sell=Decimal("4.1"),
        valid_from=VALID_FROM,
        last_polled_at=T
        + timedelta(seconds=100),  # polled after T → lag negative → FRESH
    )

    result = await service.convert(conn, make_event(currency_code="PLN"))

    assert result.rate_metadata["rate_uah"]["quality"] == "fresh"


# ---------------------------------------------------------------------------
# Test 18: Arithmetic correctness — 4 scenarios as dedicated test functions
# ---------------------------------------------------------------------------


async def test_18_1hop_multiply(
    conn: asyncpg.Connection,
    service: CurrencyConversionService,
) -> None:
    """Scenario: PLN→UAH direct, buy side: 10000 * 3.9 = 39000."""
    await _seed_default_chain(conn)
    await insert_rate(
        conn,
        source="monobank",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=Decimal("4.0"),
        rate_buy=Decimal("3.9"),
        rate_sell=Decimal("4.1"),
        valid_from=VALID_FROM,
        last_polled_at=FRESH_POLLED,
    )

    result = await service.convert(
        conn, make_event(currency_code="PLN", amount_cents=10000)
    )

    assert result.amounts["UAH"] == 39000


async def test_18_1hop_divide(
    conn: asyncpg.Connection,
    service: CurrencyConversionService,
) -> None:
    """Scenario: UAH/PLN stored, PLN→UAH divide by sell: 10000 / 0.26 → 38462."""
    await _seed_default_chain(conn)
    await insert_rate(
        conn,
        source="monobank",
        currency_from="UAH",
        currency_to="PLN",
        rate_mid=Decimal("0.25"),
        rate_buy=Decimal("0.24"),
        rate_sell=Decimal("0.26"),
        valid_from=VALID_FROM,
        last_polled_at=FRESH_POLLED,
    )

    result = await service.convert(
        conn, make_event(currency_code="PLN", amount_cents=10000)
    )

    expected = int(
        (Decimal("10000") / Decimal("0.26")).quantize(
            Decimal(0), rounding=ROUND_HALF_UP
        )
    )
    assert result.amounts["UAH"] == expected  # 38462


async def test_18_2hop_multiply_multiply(
    conn: asyncpg.Connection,
    service: CurrencyConversionService,
) -> None:
    """Scenario: PLN→UAH→USD both direct: 10000 * 3.9 * 0.024 = 936."""
    await _seed_default_chain(conn)
    await insert_rate(
        conn,
        source="monobank",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=Decimal("4.0"),
        rate_buy=Decimal("3.9"),
        rate_sell=Decimal("4.1"),
        valid_from=VALID_FROM,
        last_polled_at=FRESH_POLLED,
    )
    await insert_rate(
        conn,
        source="monobank",
        currency_from="UAH",
        currency_to="USD",
        rate_mid=Decimal("0.025"),
        rate_buy=Decimal("0.024"),
        rate_sell=Decimal("0.026"),
        valid_from=VALID_FROM,
        last_polled_at=FRESH_POLLED,
    )

    result = await service.convert(
        conn, make_event(currency_code="PLN", amount_cents=10000)
    )

    # 10000 * 3.9 * 0.024 = 936 (exact)
    expected = int(
        (Decimal("10000") * Decimal("3.9") * Decimal("0.024")).quantize(
            Decimal(0), rounding=ROUND_HALF_UP
        )
    )
    assert result.amounts["USD"] == expected


async def test_18_2hop_multiply_divide(
    conn: asyncpg.Connection,
    service: CurrencyConversionService,
) -> None:
    """Scenario: PLN→UAH direct then USD/UAH stored, divide leg.

    Path: PLN * buy(PLN/UAH) / sell(USD/UAH)
    = 10000 * 3.9 / 42.0 ≈ 928.57 → 929
    """
    await _seed_default_chain(conn)
    await insert_rate(
        conn,
        source="monobank",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=Decimal("4.0"),
        rate_buy=Decimal("3.9"),
        rate_sell=Decimal("4.1"),
        valid_from=VALID_FROM,
        last_polled_at=FRESH_POLLED,
    )
    # USD/UAH stored (reverse of UAH→USD); sell side used for divide leg
    await insert_rate(
        conn,
        source="monobank",
        currency_from="USD",
        currency_to="UAH",
        rate_mid=Decimal("41.0"),
        rate_buy=Decimal("40.0"),
        rate_sell=Decimal("42.0"),
        valid_from=VALID_FROM,
        last_polled_at=FRESH_POLLED,
    )

    result = await service.convert(
        conn, make_event(currency_code="PLN", amount_cents=10000)
    )

    # 10000 * 3.9 / 42.0 = 39000 / 42.0 = 928.571... → 929
    expected = int(
        (Decimal("10000") * Decimal("3.9") / Decimal("42.0")).quantize(
            Decimal(0), rounding=ROUND_HALF_UP
        )
    )
    assert result.amounts["USD"] == expected


# ---------------------------------------------------------------------------
# Test 19: Cyclic chain raises RateSourceChainError
# ---------------------------------------------------------------------------


async def test_19_cyclic_chain_raises(
    conn: asyncpg.Connection,
    service: CurrencyConversionService,
) -> None:
    # Build: monobank → cyc_a → cyc_b → cyc_a (cycle)
    # cyc_a/cyc_b are new names; insert leaf-first to satisfy FK
    await insert_source_config(conn, source="cyc_a", fallback_source=None)
    await insert_source_config(conn, source="cyc_b", fallback_source="cyc_a")
    await conn.execute(
        "UPDATE rate_source_config SET fallback_source = 'cyc_b' WHERE source = 'cyc_a'"
    )
    # Re-point monobank's fallback into the cycle
    await insert_source_config(conn, source="monobank", fallback_source="cyc_a")

    event = make_event(source="monobank", currency_code="PLN")

    with pytest.raises(RateSourceChainError):
        await service.convert(conn, event)


# ---------------------------------------------------------------------------
# Test 20: rate_metadata contains effective_rate, hops, quality, path
# ---------------------------------------------------------------------------


async def test_20_rate_metadata_shape(
    conn: asyncpg.Connection,
    service: CurrencyConversionService,
) -> None:
    await _seed_default_chain(conn)
    await insert_rate(
        conn,
        source="monobank",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=Decimal("4.0"),
        rate_buy=Decimal("3.9"),
        rate_sell=Decimal("4.1"),
        valid_from=VALID_FROM,
        last_polled_at=FRESH_POLLED,
    )

    result = await service.convert(
        conn, make_event(currency_code="PLN", amount_cents=10000)
    )

    meta = result.rate_metadata["rate_uah"]
    assert "effective_rate" in meta
    assert "hops" in meta
    assert "quality" in meta
    assert "path" in meta
    assert "sides" in meta
    path_step = meta["path"][0]
    assert "from" in path_step
    assert "to" in path_step
    assert "source" in path_step
    assert "rate_id" in path_step
    assert "rate" in path_step
    assert "rate_side" in path_step
    assert "tier" in path_step
    assert "op" in path_step
    # effective_rate should be a parseable Decimal string
    Decimal(meta["effective_rate"])
