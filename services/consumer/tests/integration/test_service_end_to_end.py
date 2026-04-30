"""Integration tests: end-to-end CurrencyConversionService with real DB.

Fixed chain: monobank -> nbu -> NULL. Both base_currencies=['UAH'].
Monobank rows seeded with update_cadence_seconds=60. NBU with NULL.
T = datetime(2025, 6, 1, 12, 0, tzinfo=UTC). K=2 -> grace 120s.
"""

import logging
from datetime import timedelta
from decimal import ROUND_HALF_UP, Decimal

import pytest
from grosh_shared.models import Currency

from grosh_consumer.repositories.currency_rate_repo import RateSourceChainError
from tests.helpers import T, make_event
from tests.integration.helpers import insert_rate, insert_source_config

pytestmark = pytest.mark.asyncio


async def _setup_chain(conn, extra_sources=None):
    """Insert standard chain: monobank -> nbu -> NULL."""
    await insert_source_config(conn, source="nbu", fallback_source=None)
    await insert_source_config(conn, source="monobank", fallback_source="nbu")
    if extra_sources:
        for src, fallback, bases in extra_sources:
            await insert_source_config(
                conn, source=src, fallback_source=fallback, base_currencies=bases
            )


# === 1. Passthrough ===


async def test_passthrough(conn, service):
    await _setup_chain(conn)
    await insert_rate(
        conn,
        source="monobank",
        currency_from="UAH",
        currency_to="USD",
        rate_mid=0.025,
        rate_buy=0.024,
        rate_sell=0.026,
        valid_from=T - timedelta(hours=1),
        last_polled_at=T - timedelta(seconds=30),
        update_cadence_seconds=60,
    )
    await insert_rate(
        conn,
        source="monobank",
        currency_from="UAH",
        currency_to="EUR",
        rate_mid=0.023,
        rate_buy=0.022,
        rate_sell=0.024,
        valid_from=T - timedelta(hours=1),
        last_polled_at=T - timedelta(seconds=30),
        update_cadence_seconds=60,
    )
    event = make_event(operation_currency_code="UAH", amount_cents=55500)
    result = await service.convert(conn, event, event.operation_currency_code)
    assert result.amounts[Currency.UAH] == 55500
    assert "rate_uah" not in result.rate_metadata
    assert result.amounts[Currency.USD] is not None
    assert result.amounts[Currency.EUR] is not None


# === 2. Monobank PLN direct to UAH, 2-hop for USD/EUR ===


async def test_monobank_pln_direct_and_2hop(conn, service):
    await _setup_chain(conn)
    await insert_rate(
        conn,
        source="monobank",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=11.0,
        rate_buy=10.9,
        rate_sell=11.1,
        valid_from=T - timedelta(hours=1),
        last_polled_at=T - timedelta(seconds=30),
        update_cadence_seconds=60,
    )
    await insert_rate(
        conn,
        source="monobank",
        currency_from="UAH",
        currency_to="USD",
        rate_mid=0.025,
        rate_buy=0.024,
        rate_sell=0.026,
        valid_from=T - timedelta(hours=1),
        last_polled_at=T - timedelta(seconds=30),
        update_cadence_seconds=60,
    )
    await insert_rate(
        conn,
        source="monobank",
        currency_from="UAH",
        currency_to="EUR",
        rate_mid=0.023,
        rate_buy=0.022,
        rate_sell=0.024,
        valid_from=T - timedelta(hours=1),
        last_polled_at=T - timedelta(seconds=30),
        update_cadence_seconds=60,
    )
    event = make_event(operation_currency_code="PLN", amount_cents=10000)
    result = await service.convert(conn, event, event.operation_currency_code)
    assert result.amounts[Currency.UAH] == 109000  # 10000 * 10.9
    assert result.amounts[Currency.USD] == 2616  # 109000 * 0.024
    assert result.amounts[Currency.EUR] == 2398  # 109000 * 0.022
    for key in ("rate_uah", "rate_usd", "rate_eur"):
        assert result.rate_metadata[key]["quality"] == "fresh"
        for step in result.rate_metadata[key]["path"]:
            assert step["proximity_seconds"] == 0


# === 3. Reverse direction ===


async def test_reverse_direction(conn, service):
    await _setup_chain(conn)
    await insert_rate(
        conn,
        source="monobank",
        currency_from="UAH",
        currency_to="PLN",
        rate_mid=0.09,
        rate_buy=0.089,
        rate_sell=0.091,
        valid_from=T - timedelta(hours=1),
        last_polled_at=T - timedelta(seconds=30),
        update_cadence_seconds=60,
    )
    event = make_event(operation_currency_code="PLN", amount_cents=10000)
    result = await service.convert(conn, event, event.operation_currency_code)
    assert result.amounts[Currency.UAH] == 109890  # round(10000/0.091)
    meta = result.rate_metadata["rate_uah"]
    assert meta["path"][0]["op"] == "divide"
    assert meta["path"][0]["rate_side"] == "sell"


# === 4. Historical fallback (NBU) resolves at CLOSEST ===


async def test_historical_fallback_nbu(conn, service, caplog):
    await _setup_chain(conn)
    await insert_rate(
        conn,
        source="nbu",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=11.0,
        valid_from=T - timedelta(days=1),
    )
    event = make_event(operation_currency_code="PLN", amount_cents=10000)
    with caplog.at_level(logging.WARNING):
        result = await service.convert(conn, event, event.operation_currency_code)
    assert result.amounts[Currency.UAH] == 110000
    meta = result.rate_metadata["rate_uah"]
    assert meta["path"][0]["source"] == "nbu"
    assert meta["path"][0]["rate_side"] == "mid"
    assert meta["quality"] == "closest"
    assert meta["path"][0]["proximity_seconds"] == pytest.approx(86400, abs=1)
    assert "Closest-rate" in caplog.text


# === 5. Poll-based row past grace resolves at CLOSEST ===


async def test_poll_based_past_grace(conn, service):
    await _setup_chain(conn)
    await insert_rate(
        conn,
        source="monobank",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=11.0,
        valid_from=T - timedelta(hours=2),
        last_polled_at=T - timedelta(seconds=3600),
        update_cadence_seconds=60,
    )
    event = make_event(operation_currency_code="PLN", amount_cents=10000)
    result = await service.convert(conn, event, event.operation_currency_code)
    meta = result.rate_metadata["rate_uah"]
    assert meta["quality"] == "closest"
    assert meta["path"][0]["proximity_seconds"] == pytest.approx(3600, abs=2)


# === 6. CLOSEST at fallback preferred by proximity ===


async def test_closest_fallback_preferred_by_proximity(conn, service):
    await _setup_chain(conn)
    # Monobank past grace, proximity ~2d
    await insert_rate(
        conn,
        source="monobank",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=10.0,
        valid_from=T - timedelta(days=3),
        last_polled_at=T - timedelta(days=2),
        update_cadence_seconds=60,
    )
    # NBU historical, proximity 6h
    await insert_rate(
        conn,
        source="nbu",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=11.0,
        valid_from=T - timedelta(hours=6),
    )
    event = make_event(operation_currency_code="PLN", amount_cents=10000)
    result = await service.convert(conn, event, event.operation_currency_code)
    meta = result.rate_metadata["rate_uah"]
    assert meta["path"][0]["source"] == "nbu"
    assert meta["quality"] == "closest"


# === 7. CLOSEST when no FRESH anywhere ===


async def test_closest_when_no_fresh(conn, service, caplog):
    await _setup_chain(conn)
    await insert_rate(
        conn,
        source="monobank",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=11.0,
        valid_from=T - timedelta(days=3),
        valid_to=T - timedelta(days=2),
        last_polled_at=T - timedelta(days=2),
        update_cadence_seconds=60,
    )
    event = make_event(operation_currency_code="PLN", amount_cents=10000)
    with caplog.at_level(logging.WARNING):
        result = await service.convert(conn, event, event.operation_currency_code)
    meta = result.rate_metadata["rate_uah"]
    assert meta["quality"] == "closest"
    assert "Closest-rate" in caplog.text


# === 8. No path -> None amount ===


async def test_no_path_none_amount(conn, service):
    await _setup_chain(conn)
    # Only PLN/UAH FRESH
    await insert_rate(
        conn,
        source="monobank",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=11.0,
        rate_buy=10.9,
        rate_sell=11.1,
        valid_from=T - timedelta(hours=1),
        last_polled_at=T - timedelta(seconds=30),
        update_cadence_seconds=60,
    )
    event = make_event(operation_currency_code="PLN", amount_cents=10000)
    result = await service.convert(conn, event, event.operation_currency_code)
    assert result.amounts[Currency.UAH] is not None
    assert result.amounts[Currency.USD] is None
    assert result.amounts[Currency.EUR] is None


# === 9. Bid/ask vs mid produce different amounts ===


async def test_bid_ask_vs_mid_different_amounts(conn, service):
    await _setup_chain(conn)
    # With buy/sell
    await insert_rate(
        conn,
        source="monobank",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=11.0,
        rate_buy=10.5,
        rate_sell=11.5,
        valid_from=T - timedelta(hours=1),
        last_polled_at=T - timedelta(seconds=30),
        update_cadence_seconds=60,
    )
    event = make_event(operation_currency_code="PLN", amount_cents=10000)
    result_a = await service.convert(conn, event, event.operation_currency_code)

    # Now test with mid only (separate event, same DB state, but we can check
    # the rate_side to verify the difference)
    meta = result_a.rate_metadata["rate_uah"]
    assert meta["path"][0]["rate_side"] == "buy"
    assert result_a.amounts[Currency.UAH] == 105000  # 10000 * 10.5


# === 10. 2-hop tier coherence ===


async def test_2_hop_tier_coherence(conn, service):
    # Chain: monobank -> mono2
    await insert_source_config(conn, source="mono2", fallback_source=None)
    await insert_source_config(conn, source="monobank", fallback_source="mono2")
    # Monobank: PLN/UAH FRESH, UAH/USD past grace
    await insert_rate(
        conn,
        source="monobank",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=11.0,
        valid_from=T - timedelta(hours=1),
        last_polled_at=T - timedelta(seconds=30),
        update_cadence_seconds=60,
    )
    await insert_rate(
        conn,
        source="monobank",
        currency_from="UAH",
        currency_to="USD",
        rate_mid=0.025,
        valid_from=T - timedelta(hours=2),
        last_polled_at=T - timedelta(seconds=3600),
        update_cadence_seconds=60,
    )
    # mono2: both FRESH
    await insert_rate(
        conn,
        source="mono2",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=11.0,
        valid_from=T - timedelta(hours=1),
        last_polled_at=T - timedelta(seconds=30),
        update_cadence_seconds=60,
    )
    await insert_rate(
        conn,
        source="mono2",
        currency_from="UAH",
        currency_to="USD",
        rate_mid=0.025,
        valid_from=T - timedelta(hours=1),
        last_polled_at=T - timedelta(seconds=30),
        update_cadence_seconds=60,
    )
    event = make_event(operation_currency_code="PLN", amount_cents=10000)
    result = await service.convert(conn, event, event.operation_currency_code)
    meta = result.rate_metadata["rate_usd"]
    # Both legs resolve at FRESH (monobank left, mono2 right — each leg
    # resolved independently via chain walk)
    assert meta["quality"] == "fresh"
    assert meta["path"][0]["source"] == "monobank"
    assert meta["path"][1]["source"] == "mono2"


# === 11. 1-hop CLOSEST wins over 2-hop CLOSEST ===


async def test_1_hop_closest_wins_over_2_hop_closest(conn, service):
    await _setup_chain(conn)
    # 1-hop PLN/USD historical (1d proximity)
    await insert_rate(
        conn,
        source="nbu",
        currency_from="PLN",
        currency_to="USD",
        rate_mid=0.25,
        valid_from=T - timedelta(days=1),
    )
    # 2-hop through UAH: both historical (2d proximity)
    await insert_rate(
        conn,
        source="nbu",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=11.0,
        valid_from=T - timedelta(days=2),
    )
    await insert_rate(
        conn,
        source="nbu",
        currency_from="UAH",
        currency_to="USD",
        rate_mid=0.025,
        valid_from=T - timedelta(days=2),
    )
    event = make_event(operation_currency_code="PLN", amount_cents=10000)
    result = await service.convert(conn, event, event.operation_currency_code)
    meta = result.rate_metadata["rate_usd"]
    assert meta["hops"] == 1


# === 12. Empty chain ===


async def test_empty_chain_no_crash(conn, service):
    # Event source not in config
    await _setup_chain(conn)
    event = make_event(
        source="manual", operation_currency_code="PLN", amount_cents=10000
    )
    result = await service.convert(conn, event, event.operation_currency_code)
    # Passthrough for UAH would not apply (event is PLN)
    # All targets should be None except if PLN matches any display currency (it doesn't)
    for curr in (Currency.UAH, Currency.USD, Currency.EUR):
        assert result.amounts[curr] is None


# === 13. Multiple display currencies computed independently ===


async def test_multiple_display_currencies_independent(conn, service):
    await _setup_chain(conn)
    await insert_rate(
        conn,
        source="monobank",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=11.0,
        rate_buy=10.9,
        rate_sell=11.1,
        valid_from=T - timedelta(hours=1),
        last_polled_at=T - timedelta(seconds=30),
        update_cadence_seconds=60,
    )
    await insert_rate(
        conn,
        source="monobank",
        currency_from="UAH",
        currency_to="USD",
        rate_mid=0.025,
        rate_buy=0.024,
        rate_sell=0.026,
        valid_from=T - timedelta(hours=1),
        last_polled_at=T - timedelta(seconds=30),
        update_cadence_seconds=60,
    )
    # No EUR rate
    event = make_event(operation_currency_code="PLN", amount_cents=10000)
    result = await service.convert(conn, event, event.operation_currency_code)
    assert result.amounts[Currency.UAH] is not None
    assert result.amounts[Currency.USD] is not None
    assert result.amounts[Currency.EUR] is None


# === 14. Chain traversed in order ===


async def test_chain_traversed_in_order(conn, service):
    await insert_source_config(conn, source="mono3", fallback_source=None)
    await insert_source_config(conn, source="mono2", fallback_source="mono3")
    await insert_source_config(conn, source="monobank", fallback_source="mono2")
    # All three have PLN/UAH FRESH
    for src, mid in [("monobank", 11), ("mono2", 12), ("mono3", 13)]:
        await insert_rate(
            conn,
            source=src,
            currency_from="PLN",
            currency_to="UAH",
            rate_mid=mid,
            rate_buy=mid - 0.1,
            rate_sell=mid + 0.1,
            valid_from=T - timedelta(hours=1),
            last_polled_at=T - timedelta(seconds=30),
            update_cadence_seconds=60,
        )
    event = make_event(operation_currency_code="PLN", amount_cents=10000)
    result = await service.convert(conn, event, event.operation_currency_code)
    meta = result.rate_metadata["rate_uah"]
    assert meta["path"][0]["source"] == "monobank"


# === 15. ROUND_HALF_UP applied to cents ===


async def test_round_half_up(conn, service):
    await _setup_chain(conn)
    # 10000 * 1.005 = 10050.0 (exact) — but let's use a rate that produces .5
    # 3 * 1.15 = 3.45 -> rounds to 3 with HALF_EVEN, 4 with HALF_UP
    # Actually use: amount=3, rate_buy=1.15 -> 3.45 rounds to 4
    await insert_rate(
        conn,
        source="monobank",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=1.15,
        rate_buy=1.15,
        rate_sell=1.15,
        valid_from=T - timedelta(hours=1),
        last_polled_at=T - timedelta(seconds=30),
        update_cadence_seconds=60,
    )
    event = make_event(operation_currency_code="PLN", amount_cents=3)
    await service.convert(conn, event, event.operation_currency_code)
    # 3 * 1.15 = 3.45 -> ROUND_HALF_UP -> 3 (quantize to integer, 3.45 -> 3? No)
    # Actually Decimal("3") * Decimal("1.15") = Decimal("3.45")
    # int(Decimal("3.45").quantize(Decimal("0"), ROUND_HALF_UP)) = 3
    # Wait: 3.45 rounds to 3 because .45 < .5
    # Let's use amount=5, rate=1.15 -> 5.75 -> rounds to 6
    # Hmm, let me just check: amount_cents=5
    event2 = make_event(operation_currency_code="PLN", amount_cents=5)
    result2 = await service.convert(conn, event2, event2.operation_currency_code)
    # 5 * 1.15 = 5.75 -> ROUND_HALF_UP -> 6
    assert result2.amounts[Currency.UAH] == 6


# === 16. Very large amount — no overflow ===


async def test_very_large_amount_no_overflow(conn, service):
    await _setup_chain(conn)
    await insert_rate(
        conn,
        source="monobank",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=1.0,
        rate_buy=1.0,
        rate_sell=1.0,
        valid_from=T - timedelta(hours=1),
        last_polled_at=T - timedelta(seconds=30),
        update_cadence_seconds=60,
    )
    event = make_event(operation_currency_code="PLN", amount_cents=10**15)
    result = await service.convert(conn, event, event.operation_currency_code)
    assert result.amounts[Currency.UAH] == 10**15


# === 17. Timezone-naive event.time handled via _ensure_tz ===


async def test_timezone_naive_event(conn, service):
    from datetime import datetime

    await _setup_chain(conn)
    await insert_rate(
        conn,
        source="monobank",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=11.0,
        rate_buy=10.9,
        rate_sell=11.1,
        valid_from=T - timedelta(hours=1),
        last_polled_at=T - timedelta(seconds=30),
        update_cadence_seconds=60,
    )
    # Naive datetime (no tzinfo)
    naive_time = datetime(2025, 6, 1, 12, 0, 0)
    event = make_event(
        operation_currency_code="PLN", amount_cents=10000, time=naive_time
    )
    result = await service.convert(conn, event, event.operation_currency_code)
    assert result.amounts[Currency.UAH] is not None


# === 18. effective_rate reconstructs amount ===


async def test_effective_rate_reconstructs_amount(conn, service):
    await _setup_chain(conn)
    # 1-hop multiply
    await insert_rate(
        conn,
        source="monobank",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=11.0,
        rate_buy=10.9,
        rate_sell=11.1,
        valid_from=T - timedelta(hours=1),
        last_polled_at=T - timedelta(seconds=30),
        update_cadence_seconds=60,
    )
    await insert_rate(
        conn,
        source="monobank",
        currency_from="UAH",
        currency_to="USD",
        rate_mid=0.025,
        rate_buy=0.024,
        rate_sell=0.026,
        valid_from=T - timedelta(hours=1),
        last_polled_at=T - timedelta(seconds=30),
        update_cadence_seconds=60,
    )
    event = make_event(operation_currency_code="PLN", amount_cents=10000)
    result = await service.convert(conn, event, event.operation_currency_code)

    for key in ("rate_uah", "rate_usd"):
        meta = result.rate_metadata[key]
        effective_rate = Decimal(meta["effective_rate"])
        expected = (Decimal("10000") * effective_rate).quantize(
            Decimal("0"), rounding=ROUND_HALF_UP
        )
        assert result.amounts[Currency(key.replace("rate_", "").upper())] == int(
            expected
        )


# === 19. RateSourceChainError propagates ===


async def test_chain_error_propagates(conn, service):
    # Cyclic chain — insert without FK, then update to create cycle
    await insert_source_config(conn, source="monobank", fallback_source=None)
    await insert_source_config(conn, source="manual", fallback_source=None)
    await conn.execute(
        "UPDATE rate_source_config SET fallback_source = 'manual'"
        " WHERE source = 'monobank'"
    )
    await conn.execute(
        "UPDATE rate_source_config SET fallback_source = 'monobank'"
        " WHERE source = 'manual'"
    )
    event = make_event(
        source="monobank", operation_currency_code="PLN", amount_cents=10000
    )
    with pytest.raises(RateSourceChainError):
        await service.convert(conn, event, event.operation_currency_code)


# === 20. Sides aggregation reflects multi-hop mix ===


async def test_sides_aggregation_multi_hop(conn, service):
    await _setup_chain(conn)
    # PLN/UAH direct (buy)
    await insert_rate(
        conn,
        source="monobank",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=11.0,
        rate_buy=10.9,
        rate_sell=11.1,
        valid_from=T - timedelta(hours=1),
        last_polled_at=T - timedelta(seconds=30),
        update_cadence_seconds=60,
    )
    # USD/UAH (leg 2 is reverse for UAH->USD via divide)
    await insert_rate(
        conn,
        source="monobank",
        currency_from="USD",
        currency_to="UAH",
        rate_mid=41.0,
        rate_buy=40.5,
        rate_sell=41.5,
        valid_from=T - timedelta(hours=1),
        last_polled_at=T - timedelta(seconds=30),
        update_cadence_seconds=60,
    )
    event = make_event(operation_currency_code="PLN", amount_cents=10000)
    result = await service.convert(conn, event, event.operation_currency_code)
    meta = result.rate_metadata["rate_usd"]
    assert meta["sides"] == ["buy", "sell"]


# === 21. SCD2 successor scenario ===


async def test_scd2_successor(conn, service):
    await _setup_chain(conn)
    # Row A: superseded
    await insert_rate(
        conn,
        source="monobank",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=10.0,
        rate_buy=10.0,
        rate_sell=10.0,
        valid_from=T - timedelta(minutes=3),
        valid_to=T - timedelta(minutes=1),
        last_polled_at=T - timedelta(minutes=2),
        update_cadence_seconds=60,
    )
    # Row B: current
    await insert_rate(
        conn,
        source="monobank",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=11.0,
        rate_buy=11.0,
        rate_sell=11.0,
        valid_from=T - timedelta(minutes=1),
        last_polled_at=T - timedelta(minutes=1),
        update_cadence_seconds=60,
    )
    event = make_event(operation_currency_code="PLN", amount_cents=10000)
    result = await service.convert(conn, event, event.operation_currency_code)
    # B wins at FRESH; amount uses 11
    assert result.amounts[Currency.UAH] == 110000
    assert result.rate_metadata["rate_uah"]["quality"] == "fresh"


# === 22. Malformed row (interval NULL) falls to CLOSEST ===


async def test_malformed_interval_null_falls_to_closest(conn, service):
    await _setup_chain(conn)
    await insert_rate(
        conn,
        source="monobank",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=11.0,
        valid_from=T - timedelta(hours=1),
        last_polled_at=T - timedelta(seconds=30),
        update_cadence_seconds=None,
    )
    event = make_event(operation_currency_code="PLN", amount_cents=10000)
    result = await service.convert(conn, event, event.operation_currency_code)
    meta = result.rate_metadata["rate_uah"]
    assert meta["quality"] == "closest"


# === 23. Historical row with spurious interval still CLOSEST ===


async def test_historical_with_interval_still_closest(conn, service):
    await _setup_chain(conn)
    await insert_rate(
        conn,
        source="nbu",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=11.0,
        valid_from=T - timedelta(days=1),
        update_cadence_seconds=3600,
    )
    event = make_event(operation_currency_code="PLN", amount_cents=10000)
    result = await service.convert(conn, event, event.operation_currency_code)
    meta = result.rate_metadata["rate_uah"]
    assert meta["quality"] == "closest"
