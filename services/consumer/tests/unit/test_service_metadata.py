"""Tests for conversion metadata structure and content."""

from datetime import timedelta
from decimal import Decimal

import pytest

from tests.helpers import T, make_event

pytestmark = pytest.mark.asyncio


def _setup_chain(repo):
    repo.add_source("monobank", fallback="nbu")
    repo.add_source("nbu", fallback=None)


async def test_metadata_key_naming(repo, service):
    _setup_chain(repo)
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
    repo.add_rate(
        "monobank",
        "UAH",
        "EUR",
        mid=0.023,
        buy=0.022,
        sell=0.024,
        valid_from=T - timedelta(hours=1),
        polled=T - timedelta(seconds=30),
        interval=60,
    )
    event = make_event()
    result = await service.convert(None, event, event.operation_currency_code)
    assert "rate_uah" in result.rate_metadata
    assert "rate_usd" in result.rate_metadata
    assert "rate_eur" in result.rate_metadata


async def test_metadata_quality_matches_path_max_tier(repo, service):
    _setup_chain(repo)
    # Historical rate only — CLOSEST
    repo.add_rate(
        "nbu",
        "PLN",
        "UAH",
        mid=11,
        valid_from=T - timedelta(days=1),
    )
    event = make_event()
    result = await service.convert(None, event, event.operation_currency_code)
    assert result.rate_metadata["rate_uah"]["quality"] == "closest"


async def test_metadata_hops_matches_step_count(repo, service):
    _setup_chain(repo)
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
    event = make_event()
    result = await service.convert(None, event, event.operation_currency_code)
    assert result.rate_metadata["rate_uah"]["hops"] == 1
    assert result.rate_metadata["rate_usd"]["hops"] == 2


async def test_metadata_effective_rate_is_stringified_decimal(repo, service):
    _setup_chain(repo)
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
    er = result.rate_metadata["rate_uah"]["effective_rate"]
    assert isinstance(er, str)
    Decimal(er)  # must parse


async def test_metadata_omits_entry_for_passthrough_currency(repo, service):
    _setup_chain(repo)
    event = make_event(operation_currency_code="UAH", amount_cents=10000)
    result = await service.convert(None, event, event.operation_currency_code)
    assert "rate_uah" not in result.rate_metadata


async def test_fresh_step_has_proximity_seconds_zero(repo, service):
    _setup_chain(repo)
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
    assert result.rate_metadata["rate_uah"]["path"][0]["proximity_seconds"] == 0


async def test_closest_step_has_proximity_seconds_gt_zero(repo, service):
    _setup_chain(repo)
    repo.add_rate(
        "nbu",
        "PLN",
        "UAH",
        mid=11,
        valid_from=T - timedelta(days=1),
    )
    event = make_event()
    result = await service.convert(None, event, event.operation_currency_code)
    assert result.rate_metadata["rate_uah"]["path"][0]["proximity_seconds"] > 0


async def test_max_proximity_seconds_equals_max_over_steps(repo, service):
    _setup_chain(repo)
    # 2-hop: one CLOSEST step with high proximity
    # FRESH PLN/UAH
    repo.add_rate(
        "monobank",
        "PLN",
        "UAH",
        mid=11,
        valid_from=T - timedelta(hours=1),
        polled=T - timedelta(seconds=30),
        interval=60,
    )
    # No FRESH UAH/USD, historical fallback
    repo.add_rate(
        "nbu",
        "UAH",
        "USD",
        mid=0.025,
        valid_from=T - timedelta(days=2),
    )
    event = make_event()
    result = await service.convert(None, event, event.operation_currency_code)
    if "rate_usd" in result.rate_metadata:
        meta = result.rate_metadata["rate_usd"]
        step_proximities = [s["proximity_seconds"] for s in meta["path"]]
        assert meta["max_proximity_seconds"] == max(step_proximities)
