"""Tests for CurrencyConversionService path resolution with InMemoryRateRepo.

Covers: same-currency passthrough, 1-hop direct, CLOSEST fallback,
reverse-and-divide, direction preference, tier ordering, source priority,
multi-hop, and no-path scenarios.
"""

from datetime import timedelta
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from grosh_shared.domain.models import Currency

from grosh_enrichment.services.currency_conversion_service import (
    CurrencyConversionService,
)
from tests.helpers import T, make_event

pytestmark = pytest.mark.asyncio


def _setup_default_chain(repo):
    """monobank -> nbu -> NULL, both base_currencies=[UAH]."""
    repo.add_source("monobank", fallback="nbu")
    repo.add_source("nbu", fallback=None)


# === 3.1 Same-currency passthrough ===


async def test_passthrough_target_equals_event_currency(repo, service):
    _setup_default_chain(repo)
    event = make_event(operation_currency_code="UAH", amount_cents=12345)
    result = await service.convert(None, event, event.operation_currency_code)
    assert result.amounts[Currency.UAH] == 12345
    assert "uah" not in result.rate_metadata


# === 3.2 1-hop direct resolution ===


async def test_direct_fresh_from_bank(repo, service):
    _setup_default_chain(repo)
    repo.add_rate(
        "monobank",
        "PLN",
        "UAH",
        mid=11,
        buy=10.9,
        sell=11.1,
        valid_from=T - timedelta(hours=1),
        polled=T - timedelta(seconds=30),
        interval=60,
    )
    event = make_event()
    result = await service.convert(None, event, event.operation_currency_code)
    assert result.amounts[Currency.UAH] is not None
    meta = result.rate_metadata["uah"]
    assert meta["quality"] == "fresh"
    assert meta["hops"] == 1
    assert meta["path"][0]["source"] == "monobank"
    assert meta["path"][0]["proximity_seconds"] == 0


async def test_closest_from_historical_fallback(repo, service):
    _setup_default_chain(repo)
    # No monobank rate
    repo.add_rate(
        "nbu",
        "PLN",
        "UAH",
        mid=11,
        valid_from=T - timedelta(days=1),
    )
    event = make_event()
    result = await service.convert(None, event, event.operation_currency_code)
    assert result.amounts[Currency.UAH] is not None
    meta = result.rate_metadata["uah"]
    assert meta["quality"] == "closest"
    assert meta["path"][0]["source"] == "nbu"
    assert meta["path"][0]["proximity_seconds"] == pytest.approx(86400, abs=1)


async def test_reverse_and_divide_when_only_reverse_stored(repo, service):
    _setup_default_chain(repo)
    repo.add_rate(
        "monobank",
        "UAH",
        "PLN",
        mid=0.09,
        buy=0.089,
        sell=0.091,
        valid_from=T - timedelta(hours=1),
        polled=T - timedelta(seconds=30),
        interval=60,
    )
    event = make_event()
    result = await service.convert(None, event, event.operation_currency_code)
    meta = result.rate_metadata["uah"]
    assert meta["path"][0]["op"] == "divide"


async def test_direct_preferred_over_reverse_at_same_source_and_tier(repo, service):
    _setup_default_chain(repo)
    repo.add_rate(
        "monobank",
        "PLN",
        "UAH",
        mid=11,
        buy=10.9,
        sell=11.1,
        valid_from=T - timedelta(hours=1),
        polled=T - timedelta(seconds=30),
        interval=60,
    )
    repo.add_rate(
        "monobank",
        "UAH",
        "PLN",
        mid=0.09,
        buy=0.089,
        sell=0.091,
        valid_from=T - timedelta(hours=1),
        polled=T - timedelta(seconds=30),
        interval=60,
    )
    event = make_event()
    result = await service.convert(None, event, event.operation_currency_code)
    meta = result.rate_metadata["uah"]
    # Direct uses id=UUID(int=1) (first rate added), reverse id=UUID(int=2)
    assert meta["path"][0]["rate_id"] == str(UUID(int=1))
    assert meta["path"][0]["op"] == "multiply"


# === 3.3 Tier ordering ===


async def test_closest_at_fallback_beats_nothing_at_bank(repo, service):
    _setup_default_chain(repo)
    # Monobank past grace — CLOSEST eligible at ~1h proximity
    repo.add_rate(
        "monobank",
        "PLN",
        "UAH",
        mid=11,
        valid_from=T - timedelta(hours=2),
        polled=T - timedelta(seconds=3600),
        interval=60,
    )
    # NBU historical at 1d proximity
    repo.add_rate(
        "nbu",
        "PLN",
        "UAH",
        mid=11,
        valid_from=T - timedelta(days=1),
    )
    event = make_event()
    result = await service.convert(None, event, event.operation_currency_code)
    meta = result.rate_metadata["uah"]
    assert meta["quality"] == "closest"
    # Monobank's proximity (3600s) beats NBU's (~86400s)
    assert meta["path"][0]["source"] == "monobank"
    assert meta["path"][0]["proximity_seconds"] == pytest.approx(3600, abs=2)


async def test_closest_only_when_no_fresh_anywhere(repo, service):
    _setup_default_chain(repo)
    repo.add_rate(
        "monobank",
        "PLN",
        "UAH",
        mid=11,
        valid_from=T - timedelta(days=5),
        polled=T - timedelta(days=5),
        interval=60,
    )
    repo.add_rate(
        "nbu",
        "PLN",
        "UAH",
        mid=11,
        valid_from=T - timedelta(days=3),
    )
    event = make_event()
    result = await service.convert(None, event, event.operation_currency_code)
    meta = result.rate_metadata["uah"]
    assert meta["quality"] == "closest"


async def test_per_tier_exhaustion_short_circuits():
    mock_repo = AsyncMock()
    mock_repo.load_source_chain.return_value = [
        __import__(
            "grosh_enrichment.repositories.currency_rate_repo",
            fromlist=["SourceConfig"],
        ).SourceConfig(source="monobank", base_currencies=["UAH"])
    ]
    from grosh_enrichment.repositories.currency_rate_repo import RateRow

    mock_repo.find_fresh_rate.return_value = RateRow(
        id=UUID(int=1),
        source="monobank",
        rate_mid=__import__("decimal").Decimal("11"),
    )
    svc = CurrencyConversionService(mock_repo)
    event = make_event()
    await svc.convert(None, event, event.operation_currency_code)
    mock_repo.find_closest_rate.assert_not_called()


async def test_closest_picks_by_proximity_across_chain(repo, service):
    _setup_default_chain(repo)
    # Monobank poll-based outside grace, 5d proximity
    repo.add_rate(
        "monobank",
        "PLN",
        "UAH",
        mid=11,
        valid_from=T - timedelta(days=6),
        polled=T - timedelta(days=5),
        interval=60,
    )
    # NBU historical, 2d proximity
    repo.add_rate(
        "nbu",
        "PLN",
        "UAH",
        mid=11,
        valid_from=T - timedelta(days=2),
    )
    event = make_event()
    result = await service.convert(None, event, event.operation_currency_code)
    meta = result.rate_metadata["uah"]
    assert meta["path"][0]["source"] == "nbu"
    assert meta["quality"] == "closest"
    assert meta["path"][0]["proximity_seconds"] == pytest.approx(172800, abs=1)


# === 3.4 Source priority within FRESH ===


async def test_bank_beats_fallback_at_fresh(repo, service):
    repo.add_source("monobank", fallback="mono2")
    repo.add_source("mono2", fallback=None)
    repo.add_rate(
        "monobank",
        "PLN",
        "UAH",
        mid=11,
        valid_from=T - timedelta(hours=1),
        polled=T - timedelta(seconds=30),
        interval=60,
    )
    repo.add_rate(
        "mono2",
        "PLN",
        "UAH",
        mid=12,
        valid_from=T - timedelta(hours=1),
        polled=T - timedelta(seconds=30),
        interval=60,
    )
    event = make_event()
    result = await service.convert(None, event, event.operation_currency_code)
    meta = result.rate_metadata["uah"]
    assert meta["path"][0]["source"] == "monobank"


async def test_source_priority_beats_direction_within_fresh(repo, service):
    repo.add_source("monobank", fallback="mono2")
    repo.add_source("mono2", fallback=None)
    # Monobank only has reverse (UAH/PLN)
    repo.add_rate(
        "monobank",
        "UAH",
        "PLN",
        mid=0.09,
        valid_from=T - timedelta(hours=1),
        polled=T - timedelta(seconds=30),
        interval=60,
    )
    # mono2 has direct (PLN/UAH)
    repo.add_rate(
        "mono2",
        "PLN",
        "UAH",
        mid=11,
        valid_from=T - timedelta(hours=1),
        polled=T - timedelta(seconds=30),
        interval=60,
    )
    event = make_event()
    result = await service.convert(None, event, event.operation_currency_code)
    meta = result.rate_metadata["uah"]
    assert meta["path"][0]["source"] == "monobank"
    assert meta["path"][0]["op"] == "divide"


# === 3.5 Multi-hop resolution ===


async def test_2_hop_via_pivot_when_no_1_hop(repo, service):
    _setup_default_chain(repo)
    # No direct PLN/USD rates
    repo.add_rate(
        "monobank",
        "PLN",
        "UAH",
        mid=11,
        buy=10.9,
        sell=11.1,
        valid_from=T - timedelta(hours=1),
        polled=T - timedelta(seconds=30),
        interval=60,
    )
    repo.add_rate(
        "monobank",
        "UAH",
        "USD",
        mid=0.025,
        buy=0.024,
        sell=0.026,
        valid_from=T - timedelta(hours=1),
        polled=T - timedelta(seconds=30),
        interval=60,
    )
    event = make_event(operation_currency_code="PLN")
    result = await service.convert(None, event, event.operation_currency_code)
    meta = result.rate_metadata["usd"]
    assert meta["hops"] == 2
    assert meta["quality"] == "fresh"
    assert meta["path"][0]["proximity_seconds"] == 0
    assert meta["path"][1]["proximity_seconds"] == 0


async def test_2_hop_skips_pivot_equal_to_src_or_tgt(repo, service):
    _setup_default_chain(repo)
    repo._sources["monobank"]["base_currencies"] = ["UAH", "USD"]
    # UAH -> USD conversion: USD pivot should be skipped
    repo.add_rate(
        "monobank",
        "UAH",
        "USD",
        mid=0.025,
        valid_from=T - timedelta(hours=1),
        polled=T - timedelta(seconds=30),
        interval=60,
    )
    event = make_event(operation_currency_code="UAH", amount_cents=100000)
    result = await service.convert(None, event, event.operation_currency_code)
    meta = result.rate_metadata["usd"]
    # Should be 1-hop, not 2-hop via USD
    assert meta["hops"] == 1


async def test_2_hop_both_legs_must_resolve_at_same_tier(repo, service):
    repo.add_source("monobank", fallback="mono2")
    repo.add_source("mono2", fallback=None)
    # Monobank: PLN/UAH FRESH, UAH/USD past grace
    repo.add_rate(
        "monobank",
        "PLN",
        "UAH",
        mid=11,
        valid_from=T - timedelta(hours=1),
        polled=T - timedelta(seconds=30),
        interval=60,
    )
    repo.add_rate(
        "monobank",
        "UAH",
        "USD",
        mid=0.025,
        valid_from=T - timedelta(hours=2),
        polled=T - timedelta(seconds=3600),
        interval=60,
    )
    # mono2: both legs FRESH
    repo.add_rate(
        "mono2",
        "PLN",
        "UAH",
        mid=11,
        valid_from=T - timedelta(hours=1),
        polled=T - timedelta(seconds=30),
        interval=60,
    )
    repo.add_rate(
        "mono2",
        "UAH",
        "USD",
        mid=0.025,
        valid_from=T - timedelta(hours=1),
        polled=T - timedelta(seconds=30),
        interval=60,
    )
    event = make_event(operation_currency_code="PLN")
    result = await service.convert(None, event, event.operation_currency_code)
    meta = result.rate_metadata["usd"]
    # Both legs resolve at FRESH (monobank left, mono2 right)
    assert meta["quality"] == "fresh"
    assert meta["path"][0]["source"] == "monobank"
    assert meta["path"][1]["source"] == "mono2"


async def test_1_hop_fresh_preferred_over_2_hop_fresh(repo, service):
    repo.add_source("monobank", fallback="ecb")
    repo.add_source("ecb", fallback=None)
    # ECB has direct PLN/USD FRESH
    repo.add_rate(
        "ecb",
        "PLN",
        "USD",
        mid=0.25,
        valid_from=T - timedelta(hours=1),
        polled=T - timedelta(seconds=30),
        interval=60,
    )
    # Monobank has PLN/UAH + UAH/USD FRESH
    repo.add_rate(
        "monobank",
        "PLN",
        "UAH",
        mid=11,
        valid_from=T - timedelta(hours=1),
        polled=T - timedelta(seconds=30),
        interval=60,
    )
    repo.add_rate(
        "monobank",
        "UAH",
        "USD",
        mid=0.025,
        valid_from=T - timedelta(hours=1),
        polled=T - timedelta(seconds=30),
        interval=60,
    )
    event = make_event(operation_currency_code="PLN")
    result = await service.convert(None, event, event.operation_currency_code)
    meta = result.rate_metadata["usd"]
    # 1-hop wins even from lower-priority source
    assert meta["hops"] == 1


async def test_2_hop_fresh_preferred_over_1_hop_closest(repo, service):
    _setup_default_chain(repo)
    # Monobank PLN/USD past grace (CLOSEST)
    repo.add_rate(
        "monobank",
        "PLN",
        "USD",
        mid=0.25,
        valid_from=T - timedelta(hours=2),
        polled=T - timedelta(seconds=3600),
        interval=60,
    )
    # Monobank PLN/UAH + UAH/USD FRESH
    repo.add_rate(
        "monobank",
        "PLN",
        "UAH",
        mid=11,
        valid_from=T - timedelta(hours=1),
        polled=T - timedelta(seconds=30),
        interval=60,
    )
    repo.add_rate(
        "monobank",
        "UAH",
        "USD",
        mid=0.025,
        valid_from=T - timedelta(hours=1),
        polled=T - timedelta(seconds=30),
        interval=60,
    )
    event = make_event(operation_currency_code="PLN")
    result = await service.convert(None, event, event.operation_currency_code)
    meta = result.rate_metadata["usd"]
    # 2-hop FRESH beats 1-hop CLOSEST
    assert meta["quality"] == "fresh"
    assert meta["hops"] == 2


async def test_2_hop_uses_ordered_pivots_order(repo, service):
    repo.add_source("monobank", fallback="nbu", base_currencies=["UAH"])
    repo.add_source("nbu", fallback=None, base_currencies=["EUR", "USD"])
    # Both UAH and EUR pivots resolve at FRESH for PLN->USD
    # UAH leg
    repo.add_rate(
        "monobank",
        "PLN",
        "UAH",
        mid=11,
        valid_from=T - timedelta(hours=1),
        polled=T - timedelta(seconds=30),
        interval=60,
    )
    repo.add_rate(
        "monobank",
        "UAH",
        "USD",
        mid=0.025,
        valid_from=T - timedelta(hours=1),
        polled=T - timedelta(seconds=30),
        interval=60,
    )
    # EUR leg
    repo.add_rate(
        "monobank",
        "PLN",
        "EUR",
        mid=0.23,
        valid_from=T - timedelta(hours=1),
        polled=T - timedelta(seconds=30),
        interval=60,
    )
    repo.add_rate(
        "monobank",
        "EUR",
        "USD",
        mid=1.1,
        valid_from=T - timedelta(hours=1),
        polled=T - timedelta(seconds=30),
        interval=60,
    )
    event = make_event(operation_currency_code="PLN")
    result = await service.convert(None, event, event.operation_currency_code)
    meta = result.rate_metadata["usd"]
    # UAH chosen (chain-major order: monobank base=UAH comes first)
    assert meta["path"][0]["to"] == "UAH"


# === 3.7 No path ===


async def test_no_path_returns_none_amount_no_metadata(repo, service):
    _setup_default_chain(repo)
    # Empty repo — no rates
    event = make_event()
    result = await service.convert(None, event, event.operation_currency_code)
    assert result.amounts[Currency.UAH] is None
    assert "uah" not in result.rate_metadata


async def test_one_target_resolvable_one_not(repo, service):
    _setup_default_chain(repo)
    repo.add_rate(
        "monobank",
        "PLN",
        "UAH",
        mid=11,
        valid_from=T - timedelta(hours=1),
        polled=T - timedelta(seconds=30),
        interval=60,
    )
    event = make_event()
    result = await service.convert(None, event, event.operation_currency_code)
    assert result.amounts[Currency.UAH] is not None
    assert result.amounts[Currency.USD] is None
    assert result.amounts[Currency.EUR] is None
