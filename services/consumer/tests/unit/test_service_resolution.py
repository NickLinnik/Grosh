"""Service-level tests: resolution logic with InMemoryRateRepo.

Covers spec sections 3.1–3.5, 3.7, and 3.9.
"""

import logging
from datetime import timedelta
from unittest.mock import AsyncMock

from grosh_shared.models import Currency

from grosh_consumer.services.currency_conversion_service import (
    CurrencyConversionService,
)
from tests.helpers import T, make_event

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

VALID_FROM = T - timedelta(hours=1)  # valid window covering T
FRESH_POLLED = T - timedelta(seconds=60)
STALE_POLLED_MONO = T - timedelta(
    seconds=300 + 3600
)  # beyond monobank max_staleness=300
STALE_POLLED_NBU = T - timedelta(seconds=86400 + 3600)  # beyond nbu max_staleness=86400


# ---------------------------------------------------------------------------
# 3.1  Same-currency passthrough
# ---------------------------------------------------------------------------


async def test_passthrough_same_currency(repo, service):
    repo.add_source("monobank", max_staleness_seconds=300, base_currencies=["UAH"])
    event = make_event(currency_code="UAH", amount_cents=12345)
    result = await service.convert(None, event)
    assert result.amounts["UAH"] == 12345
    assert "rate_uah" not in result.rate_metadata


# ---------------------------------------------------------------------------
# 3.2  1-hop direct resolution
# ---------------------------------------------------------------------------


async def test_direct_fresh_from_bank(repo, service):
    repo.add_source("monobank", max_staleness_seconds=300, base_currencies=["UAH"])
    repo.add_rate(
        "monobank",
        "PLN",
        "UAH",
        mid=4.0,
        buy=3.9,
        sell=4.1,
        valid_from=VALID_FROM,
        polled=FRESH_POLLED,
    )
    result = await service.convert(None, make_event())
    meta = result.rate_metadata["rate_uah"]
    assert result.amounts["UAH"] is not None
    assert meta["quality"] == "fresh"
    assert meta["path"][0]["source"] == "monobank"
    assert len(meta["path"]) == 1


async def test_direct_fresh_from_fallback_when_bank_lacks_rate(repo, service):
    repo.add_source("monobank", max_staleness_seconds=300, base_currencies=["UAH"])
    repo.add_source("nbu", max_staleness_seconds=86400, base_currencies=["UAH"])
    repo.add_rate(
        "nbu",
        "PLN",
        "UAH",
        mid=4.0,
        buy=3.9,
        sell=4.1,
        valid_from=VALID_FROM,
        polled=FRESH_POLLED,
    )
    result = await service.convert(None, make_event())
    meta = result.rate_metadata["rate_uah"]
    assert meta["quality"] == "fresh"
    assert meta["path"][0]["source"] == "nbu"


async def test_reverse_and_divide_when_only_reverse_stored(repo, service):
    repo.add_source("monobank", max_staleness_seconds=300, base_currencies=["UAH"])
    repo.add_rate(
        "monobank",
        "UAH",
        "PLN",
        mid=0.25,
        buy=0.24,
        sell=0.26,
        valid_from=VALID_FROM,
        polled=FRESH_POLLED,
    )
    result = await service.convert(None, make_event(amount_cents=10000))
    meta = result.rate_metadata["rate_uah"]
    assert meta["path"][0]["op"] == "divide"
    # amount = 10000 / 0.26 ≈ 38462
    assert result.amounts["UAH"] is not None


async def test_direct_preferred_over_reverse_at_same_source_and_tier(repo, service):
    repo.add_source("monobank", max_staleness_seconds=300, base_currencies=["UAH"])
    # Direct PLN/UAH
    repo.add_rate(
        "monobank",
        "PLN",
        "UAH",
        mid=4.0,
        buy=3.9,
        sell=4.1,
        valid_from=VALID_FROM,
        polled=FRESH_POLLED,
    )
    # Reverse UAH/PLN — different mid so we can tell which was used
    repo.add_rate(
        "monobank",
        "UAH",
        "PLN",
        mid=0.25,
        buy=0.24,
        sell=0.26,
        valid_from=VALID_FROM,
        polled=FRESH_POLLED,
    )
    result = await service.convert(None, make_event(amount_cents=10000))
    meta = result.rate_metadata["rate_uah"]
    assert meta["path"][0]["op"] == "multiply"


# ---------------------------------------------------------------------------
# 3.3  Tier ordering
# ---------------------------------------------------------------------------


async def test_fresh_any_source_beats_stale_at_bank(repo, service):
    repo.add_source("monobank", max_staleness_seconds=300, base_currencies=["UAH"])
    repo.add_source("nbu", max_staleness_seconds=86400, base_currencies=["UAH"])
    # Monobank stale
    repo.add_rate(
        "monobank",
        "PLN",
        "UAH",
        mid=4.0,
        buy=3.9,
        sell=4.1,
        valid_from=VALID_FROM,
        polled=STALE_POLLED_MONO,
    )
    # NBU fresh
    repo.add_rate(
        "nbu",
        "PLN",
        "UAH",
        mid=4.5,
        buy=4.4,
        sell=4.6,
        valid_from=VALID_FROM,
        polled=FRESH_POLLED,
    )
    result = await service.convert(None, make_event())
    meta = result.rate_metadata["rate_uah"]
    assert meta["quality"] == "fresh"
    assert meta["path"][0]["source"] == "nbu"


async def test_stale_bank_beats_stale_fallback(repo, service):
    repo.add_source("monobank", max_staleness_seconds=300, base_currencies=["UAH"])
    repo.add_source("nbu", max_staleness_seconds=86400, base_currencies=["UAH"])
    # Monobank stale per its own max_staleness=300
    repo.add_rate(
        "monobank",
        "PLN",
        "UAH",
        mid=4.0,
        buy=3.9,
        sell=4.1,
        valid_from=VALID_FROM,
        polled=STALE_POLLED_MONO,
    )
    # NBU stale per its own max_staleness=86400
    repo.add_rate(
        "nbu",
        "PLN",
        "UAH",
        mid=4.5,
        buy=4.4,
        sell=4.6,
        valid_from=VALID_FROM,
        polled=STALE_POLLED_NBU,
    )
    result = await service.convert(None, make_event())
    meta = result.rate_metadata["rate_uah"]
    assert meta["quality"] == "stale"
    assert meta["path"][0]["source"] == "monobank"


async def test_closest_only_when_no_valid_window_rate(repo, service):
    repo.add_source("monobank", max_staleness_seconds=300, base_currencies=["UAH"])
    # Rate window does NOT cover T (valid_to is 1 day before T)
    # polled must be non-NULL for closest eligibility
    repo.add_rate(
        "monobank",
        "PLN",
        "UAH",
        mid=4.0,
        buy=3.9,
        sell=4.1,
        valid_from=T - timedelta(days=2),
        valid_to=T - timedelta(days=1),
        polled=T - timedelta(days=1),
    )
    result = await service.convert(None, make_event())
    meta = result.rate_metadata["rate_uah"]
    assert meta["quality"] == "closest"


async def test_per_tier_exhaustion_short_circuits(repo):
    """Once FRESH resolves, STALE and CLOSEST calls must not be made."""
    repo.add_source("monobank", max_staleness_seconds=300, base_currencies=["UAH"])
    repo.add_rate(
        "monobank",
        "PLN",
        "UAH",
        mid=4.0,
        buy=3.9,
        sell=4.1,
        valid_from=VALID_FROM,
        polled=FRESH_POLLED,
    )

    # Wrap repo methods with AsyncMock so we can inspect call counts
    original_find = repo.find_rate_at_time
    original_closest = repo.find_closest_rate
    repo.find_rate_at_time = AsyncMock(side_effect=original_find)
    repo.find_closest_rate = AsyncMock(side_effect=original_closest)

    svc = CurrencyConversionService(repo)
    result = await svc.convert(None, make_event())

    # find_rate_at_time signature: (conn, source, currency_from, currency_to, at_time)
    # The UAH-target probe passes Currency.UAH (enum) as args[3].
    # The service calls _resolve_path(PLN→UAH) once and exits at FRESH success.
    # All remaining PLN/UAH find_rate_at_time calls come from the USD/EUR 2-hop
    # pivot searches — they are not a STALE retry for the UAH target.
    assert result.amounts["UAH"] is not None
    assert result.rate_metadata["rate_uah"]["quality"] == "fresh"

    # UAH target must not trigger a CLOSEST probe: _resolve_path exits immediately
    # at FRESH success. find_closest_rate is called for the UAH target only if the
    # FRESH and STALE tiers both failed. The service passes Currency.UAH (the enum)
    # as currency_to when resolving the UAH display target, while pivot-leg calls
    # for USD/EUR 2-hop attempts pass the string "UAH". Using identity (`is`) on
    # the enum singleton distinguishes target calls from pivot calls.
    closest_uah_target_calls = [
        c
        for c in repo.find_closest_rate.call_args_list
        if str(c.args[1]) == "PLN" and c.args[2] is Currency.UAH
    ]
    assert (
        len(closest_uah_target_calls) == 0
    ), "find_closest_rate was called for the UAH target despite FRESH resolution"


async def test_closest_ranked_by_date_distance_not_source_order(repo, service):
    """CLOSEST picks rate nearest in time, not chain order."""
    repo.add_source("monobank", max_staleness_seconds=300, base_currencies=["UAH"])
    repo.add_source("nbu", max_staleness_seconds=86400, base_currencies=["UAH"])
    # Monobank: distance ~5-6 days (polled at valid_from — old but not NULL)
    repo.add_rate(
        "monobank",
        "PLN",
        "UAH",
        mid=4.0,
        buy=3.9,
        sell=4.1,
        valid_from=T - timedelta(days=6),
        valid_to=T - timedelta(days=5),
        polled=T - timedelta(days=5),
    )
    # NBU: distance ~1-2 days — should win
    repo.add_rate(
        "nbu",
        "PLN",
        "UAH",
        mid=4.5,
        buy=4.4,
        sell=4.6,
        valid_from=T - timedelta(days=2),
        valid_to=T - timedelta(days=1),
        polled=T - timedelta(days=1),
    )
    result = await service.convert(None, make_event())
    meta = result.rate_metadata["rate_uah"]
    assert meta["quality"] == "closest"
    assert meta["path"][0]["source"] == "nbu"


# ---------------------------------------------------------------------------
# 3.4  Source priority within tier
# ---------------------------------------------------------------------------


async def test_within_fresh_bank_beats_fallback(repo, service):
    repo.add_source("monobank", max_staleness_seconds=300, base_currencies=["UAH"])
    repo.add_source("nbu", max_staleness_seconds=86400, base_currencies=["UAH"])
    repo.add_rate(
        "monobank",
        "PLN",
        "UAH",
        mid=4.0,
        buy=3.9,
        sell=4.1,
        valid_from=VALID_FROM,
        polled=FRESH_POLLED,
    )
    repo.add_rate(
        "nbu",
        "PLN",
        "UAH",
        mid=4.5,
        buy=4.4,
        sell=4.6,
        valid_from=VALID_FROM,
        polled=FRESH_POLLED,
    )
    result = await service.convert(None, make_event())
    meta = result.rate_metadata["rate_uah"]
    assert meta["path"][0]["source"] == "monobank"


async def test_within_stale_bank_beats_fallback(repo, service):
    repo.add_source("monobank", max_staleness_seconds=300, base_currencies=["UAH"])
    repo.add_source("nbu", max_staleness_seconds=86400, base_currencies=["UAH"])
    repo.add_rate(
        "monobank",
        "PLN",
        "UAH",
        mid=4.0,
        buy=3.9,
        sell=4.1,
        valid_from=VALID_FROM,
        polled=STALE_POLLED_MONO,
    )
    repo.add_rate(
        "nbu",
        "PLN",
        "UAH",
        mid=4.5,
        buy=4.4,
        sell=4.6,
        valid_from=VALID_FROM,
        polled=STALE_POLLED_NBU,
    )
    result = await service.convert(None, make_event())
    meta = result.rate_metadata["rate_uah"]
    assert meta["quality"] == "stale"
    assert meta["path"][0]["source"] == "monobank"


async def test_within_fresh_source_priority_beats_direction_preference(repo, service):
    """Monobank reverse (divide) wins over NBU direct when both FRESH."""
    repo.add_source("monobank", max_staleness_seconds=300, base_currencies=["UAH"])
    repo.add_source("nbu", max_staleness_seconds=86400, base_currencies=["UAH"])
    # Monobank has only reverse
    repo.add_rate(
        "monobank",
        "UAH",
        "PLN",
        mid=0.25,
        buy=0.24,
        sell=0.26,
        valid_from=VALID_FROM,
        polled=FRESH_POLLED,
    )
    # NBU has direct
    repo.add_rate(
        "nbu",
        "PLN",
        "UAH",
        mid=4.5,
        buy=4.4,
        sell=4.6,
        valid_from=VALID_FROM,
        polled=FRESH_POLLED,
    )
    result = await service.convert(None, make_event())
    meta = result.rate_metadata["rate_uah"]
    assert meta["path"][0]["source"] == "monobank"
    assert meta["path"][0]["op"] == "divide"


# ---------------------------------------------------------------------------
# 3.5  Multi-hop resolution
# ---------------------------------------------------------------------------


async def test_two_hop_via_pivot_when_no_one_hop(repo, service):
    """Event is PLN, target is USD, no direct PLN/USD; 2-hop via UAH pivot."""
    repo.add_source("monobank", max_staleness_seconds=300, base_currencies=["UAH"])
    repo.add_rate(
        "monobank",
        "PLN",
        "UAH",
        mid=4.0,
        buy=3.9,
        sell=4.1,
        valid_from=VALID_FROM,
        polled=FRESH_POLLED,
    )
    repo.add_rate(
        "monobank",
        "UAH",
        "USD",
        mid=0.025,
        buy=0.024,
        sell=0.026,
        valid_from=VALID_FROM,
        polled=FRESH_POLLED,
    )
    result = await service.convert(None, make_event(currency_code="PLN"))
    meta = result.rate_metadata["rate_usd"]
    assert meta["hops"] == 2
    assert meta["path"][0]["source"] == "monobank"
    assert meta["path"][1]["source"] == "monobank"
    assert meta["quality"] == "fresh"


async def test_two_hop_skips_pivot_equal_to_src_or_tgt(repo, service):
    """USD pivot skipped when target is USD; conversion via UAH pivot works."""
    repo.add_source(
        "monobank", max_staleness_seconds=300, base_currencies=["UAH", "USD"]
    )
    # Only UAH-pivot path available for UAH→USD
    repo.add_rate(
        "monobank",
        "UAH",
        "USD",
        mid=0.025,
        buy=0.024,
        sell=0.026,
        valid_from=VALID_FROM,
        polled=FRESH_POLLED,
    )
    # Event is UAH, target is USD → direct 1-hop
    result = await service.convert(
        None, make_event(currency_code="UAH", amount_cents=10000)
    )
    # Direct conversion UAH→USD should work as a 1-hop
    assert result.amounts["USD"] is not None
    meta = result.rate_metadata["rate_usd"]
    assert meta["hops"] == 1


async def test_two_hop_both_legs_must_resolve_at_same_tier(repo, service):
    """Only paths where both legs share the same tier are accepted at a given tier.

    Seeding: Monobank has BOTH legs STALE; NBU has BOTH legs FRESH.
    At FRESH tier the resolver skips Monobank (both legs stale) and picks the
    NBU all-FRESH 2-hop path.  Without tier isolation the Monobank stale path
    would leak into the FRESH result.
    """
    repo.add_source("monobank", max_staleness_seconds=300, base_currencies=["UAH"])
    repo.add_source("nbu", max_staleness_seconds=86400, base_currencies=["UAH"])
    # Monobank: BOTH legs STALE — neither should appear in a FRESH path
    repo.add_rate(
        "monobank",
        "PLN",
        "UAH",
        mid=4.0,
        buy=3.9,
        sell=4.1,
        valid_from=VALID_FROM,
        polled=STALE_POLLED_MONO,
    )
    repo.add_rate(
        "monobank",
        "UAH",
        "USD",
        mid=0.025,
        buy=0.024,
        sell=0.026,
        valid_from=VALID_FROM,
        polled=STALE_POLLED_MONO,
    )
    # NBU: both legs FRESH
    repo.add_rate(
        "nbu",
        "PLN",
        "UAH",
        mid=4.5,
        buy=4.4,
        sell=4.6,
        valid_from=VALID_FROM,
        polled=FRESH_POLLED,
    )
    repo.add_rate(
        "nbu",
        "UAH",
        "USD",
        mid=0.026,
        buy=0.025,
        sell=0.027,
        valid_from=VALID_FROM,
        polled=FRESH_POLLED,
    )
    result = await service.convert(None, make_event(currency_code="PLN"))
    meta = result.rate_metadata["rate_usd"]
    assert meta["quality"] == "fresh"
    assert meta["path"][0]["source"] == "nbu"
    assert meta["path"][1]["source"] == "nbu"


async def test_one_hop_fresh_preferred_over_two_hop_fresh(repo, service):
    """1-hop direct ECB PLN/USD beats 2-hop via UAH."""
    repo.add_source("monobank", max_staleness_seconds=300, base_currencies=["UAH"])
    repo.add_source("nbu", max_staleness_seconds=86400, base_currencies=["UAH"])
    repo.add_source("ecb", max_staleness_seconds=86400, base_currencies=["USD"])
    # ECB has direct PLN/USD
    repo.add_rate(
        "ecb",
        "PLN",
        "USD",
        mid=0.23,
        buy=None,
        sell=None,
        valid_from=VALID_FROM,
        polled=FRESH_POLLED,
    )
    # Monobank has 2-hop path
    repo.add_rate(
        "monobank",
        "PLN",
        "UAH",
        mid=4.0,
        buy=3.9,
        sell=4.1,
        valid_from=VALID_FROM,
        polled=FRESH_POLLED,
    )
    repo.add_rate(
        "monobank",
        "UAH",
        "USD",
        mid=0.025,
        buy=0.024,
        sell=0.026,
        valid_from=VALID_FROM,
        polled=FRESH_POLLED,
    )
    result = await service.convert(None, make_event(currency_code="PLN"))
    meta = result.rate_metadata["rate_usd"]
    assert meta["hops"] == 1
    assert meta["path"][0]["source"] == "ecb"


async def test_two_hop_fresh_preferred_over_one_hop_stale(repo, service):
    """2-hop FRESH beats 1-hop STALE — tier is dominant."""
    repo.add_source("monobank", max_staleness_seconds=300, base_currencies=["UAH"])
    # Direct PLN/USD but STALE
    repo.add_rate(
        "monobank",
        "PLN",
        "USD",
        mid=0.23,
        buy=0.22,
        sell=0.24,
        valid_from=VALID_FROM,
        polled=STALE_POLLED_MONO,
    )
    # 2-hop via UAH both FRESH
    repo.add_rate(
        "monobank",
        "PLN",
        "UAH",
        mid=4.0,
        buy=3.9,
        sell=4.1,
        valid_from=VALID_FROM,
        polled=FRESH_POLLED,
    )
    repo.add_rate(
        "monobank",
        "UAH",
        "USD",
        mid=0.025,
        buy=0.024,
        sell=0.026,
        valid_from=VALID_FROM,
        polled=FRESH_POLLED,
    )
    result = await service.convert(None, make_event(currency_code="PLN"))
    meta = result.rate_metadata["rate_usd"]
    assert meta["quality"] == "fresh"
    assert meta["hops"] == 2


async def test_two_hop_uses_ordered_pivots_order(repo, service):
    """UAH pivot chosen before EUR — Monobank (UAH) precedes NBU (EUR) in chain."""
    repo.add_source("monobank", max_staleness_seconds=300, base_currencies=["UAH"])
    repo.add_source("nbu", max_staleness_seconds=86400, base_currencies=["EUR", "USD"])
    # UAH pivot path
    repo.add_rate(
        "monobank",
        "PLN",
        "UAH",
        mid=4.0,
        buy=3.9,
        sell=4.1,
        valid_from=VALID_FROM,
        polled=FRESH_POLLED,
    )
    repo.add_rate(
        "monobank",
        "UAH",
        "USD",
        mid=0.025,
        buy=0.024,
        sell=0.026,
        valid_from=VALID_FROM,
        polled=FRESH_POLLED,
    )
    # EUR pivot path also available
    repo.add_rate(
        "nbu",
        "PLN",
        "EUR",
        mid=3.5,
        buy=None,
        sell=None,
        valid_from=VALID_FROM,
        polled=FRESH_POLLED,
    )
    repo.add_rate(
        "nbu",
        "EUR",
        "USD",
        mid=1.1,
        buy=None,
        sell=None,
        valid_from=VALID_FROM,
        polled=FRESH_POLLED,
    )
    result = await service.convert(None, make_event(currency_code="PLN"))
    meta = result.rate_metadata["rate_usd"]
    assert meta["hops"] == 2
    # UAH pivot chosen — intermediate currency is UAH
    assert meta["path"][0]["to"] == "UAH"


# ---------------------------------------------------------------------------
# 3.7  No path
# ---------------------------------------------------------------------------


async def test_no_path_returns_none_amount(repo, service, caplog):
    repo.add_source("monobank", max_staleness_seconds=300, base_currencies=["UAH"])
    with caplog.at_level(logging.WARNING):
        result = await service.convert(None, make_event(currency_code="PLN"))
    assert result.amounts["UAH"] is None
    assert "rate_uah" not in result.rate_metadata
    assert any("PLN" in r.message and "UAH" in r.message for r in caplog.records)


async def test_one_target_resolvable_one_not(repo, service):
    repo.add_source("monobank", max_staleness_seconds=300, base_currencies=["UAH"])
    repo.add_rate(
        "monobank",
        "PLN",
        "UAH",
        mid=4.0,
        buy=3.9,
        sell=4.1,
        valid_from=VALID_FROM,
        polled=FRESH_POLLED,
    )
    result = await service.convert(None, make_event(currency_code="PLN"))
    assert result.amounts["UAH"] is not None
    assert result.amounts["USD"] is None
    assert result.amounts["EUR"] is None


# ---------------------------------------------------------------------------
# 3.9  STALE detection
# ---------------------------------------------------------------------------


async def test_rate_within_max_staleness_is_fresh(repo, service):
    repo.add_source("monobank", max_staleness_seconds=300, base_currencies=["UAH"])
    repo.add_rate(
        "monobank",
        "PLN",
        "UAH",
        mid=4.0,
        buy=3.9,
        sell=4.1,
        valid_from=VALID_FROM,
        polled=T - timedelta(seconds=60),  # 60s < 300s → FRESH
    )
    result = await service.convert(None, make_event())
    assert result.rate_metadata["rate_uah"]["quality"] == "fresh"


async def test_rate_beyond_max_staleness_is_stale(repo, service):
    repo.add_source("monobank", max_staleness_seconds=300, base_currencies=["UAH"])
    repo.add_rate(
        "monobank",
        "PLN",
        "UAH",
        mid=4.0,
        buy=3.9,
        sell=4.1,
        valid_from=VALID_FROM,
        polled=T - timedelta(seconds=3600),  # 3600s > 300s → STALE
    )
    result = await service.convert(None, make_event())
    assert result.rate_metadata["rate_uah"]["quality"] == "stale"


async def test_at_time_before_last_polled_at_treated_as_fresh(repo, service):
    """Backfill scenario: rate polled in the future relative to transaction time."""
    repo.add_source("monobank", max_staleness_seconds=300, base_currencies=["UAH"])
    repo.add_rate(
        "monobank",
        "PLN",
        "UAH",
        mid=4.0,
        buy=3.9,
        sell=4.1,
        valid_from=VALID_FROM,
        polled=T + timedelta(seconds=100),  # polled after T → lag is negative → fresh
    )
    result = await service.convert(None, make_event())
    assert result.rate_metadata["rate_uah"]["quality"] == "fresh"


async def test_null_last_polled_at_skipped_at_fresh_stale(repo, service):
    """Row with NULL last_polled_at must be skipped at FRESH and STALE tiers."""
    repo.add_source("monobank", max_staleness_seconds=300, base_currencies=["UAH"])
    repo.add_source("nbu", max_staleness_seconds=86400, base_currencies=["UAH"])
    # Monobank rate with NULL polled — should be skipped
    repo.add_rate(
        "monobank",
        "PLN",
        "UAH",
        mid=4.0,
        buy=3.9,
        sell=4.1,
        valid_from=VALID_FROM,
        polled=None,
    )
    # NBU rate FRESH
    repo.add_rate(
        "nbu",
        "PLN",
        "UAH",
        mid=4.5,
        buy=4.4,
        sell=4.6,
        valid_from=VALID_FROM,
        polled=FRESH_POLLED,
    )
    result = await service.convert(None, make_event())
    meta = result.rate_metadata["rate_uah"]
    assert meta["quality"] == "fresh"
    assert meta["path"][0]["source"] == "nbu"


async def test_null_last_polled_at_excluded_from_closest(repo, service):
    """NULL last_polled_at rows are excluded from all tiers including CLOSEST."""
    repo.add_source("monobank", max_staleness_seconds=300, base_currencies=["UAH"])
    # polled=None → skipped at FRESH/STALE (no staleness check possible)
    # AND skipped at CLOSEST (production SQL: last_polled_at IS NOT NULL)
    repo.add_rate(
        "monobank",
        "PLN",
        "UAH",
        mid=4.0,
        buy=3.9,
        sell=4.1,
        valid_from=T - timedelta(days=2),
        valid_to=T - timedelta(days=1),
        polled=None,
    )
    result = await service.convert(None, make_event())
    assert result.amounts["UAH"] is None
    assert "rate_uah" not in result.rate_metadata
