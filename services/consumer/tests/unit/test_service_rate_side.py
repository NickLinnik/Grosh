"""Tests for rate-side selection through the full service path.

Covers: buy/sell/mid selection in 1-hop and 2-hop scenarios.
"""

from datetime import timedelta
from decimal import Decimal

import pytest
from grosh_shared.models import Currency

from tests.helpers import T, make_event

pytestmark = pytest.mark.asyncio


def _setup_chain(repo):
    repo.add_source("monobank", fallback="nbu")
    repo.add_source("nbu", fallback=None)


async def test_1_hop_direct_uses_buy_side(repo, service):
    _setup_chain(repo)
    repo.add_rate(
        "monobank",
        "PLN",
        "UAH",
        mid=4.0,
        buy=3.9,
        sell=4.1,
        valid_from=T - timedelta(hours=1),
        polled=T - timedelta(seconds=30),
        interval=60,
    )
    event = make_event(amount_cents=10000)
    result = await service.convert(None, event)
    assert result.amounts[Currency.UAH] == 39000
    assert result.rate_metadata["rate_uah"]["path"][0]["rate_side"] == "buy"


async def test_1_hop_reverse_uses_sell_side(repo, service):
    _setup_chain(repo)
    repo.add_rate(
        "monobank",
        "UAH",
        "PLN",
        mid=0.25,
        buy=0.24,
        sell=0.26,
        valid_from=T - timedelta(hours=1),
        polled=T - timedelta(seconds=30),
        interval=60,
    )
    event = make_event(amount_cents=10000)
    result = await service.convert(None, event)
    assert result.amounts[Currency.UAH] == 38462
    meta = result.rate_metadata["rate_uah"]
    assert meta["path"][0]["rate_side"] == "sell"
    assert meta["path"][0]["op"] == "divide"


async def test_2_hop_direct_direct_both_buy(repo, service):
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
    event = make_event(amount_cents=10000)
    result = await service.convert(None, event)
    meta = result.rate_metadata["rate_usd"]
    assert meta["sides"] == ["buy"]


async def test_2_hop_reverse_direct_buy_and_sell(repo, service):
    _setup_chain(repo)
    # Leg 1: reverse UAH/PLN
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
    # Leg 2: direct UAH/USD
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
    event = make_event(amount_cents=10000)
    result = await service.convert(None, event)
    meta = result.rate_metadata["rate_usd"]
    assert meta["sides"] == ["buy", "sell"]


async def test_falls_back_to_mid_when_buy_null(repo, service):
    _setup_chain(repo)
    repo.add_rate(
        "monobank",
        "PLN",
        "UAH",
        mid=4.0,
        buy=None,
        sell=4.1,
        valid_from=T - timedelta(hours=1),
        polled=T - timedelta(seconds=30),
        interval=60,
    )
    event = make_event(amount_cents=10000)
    result = await service.convert(None, event)
    # Direct conversion uses mid since buy is NULL
    assert result.amounts[Currency.UAH] == 40000
    assert result.rate_metadata["rate_uah"]["path"][0]["rate_side"] == "mid"


async def test_never_falls_to_opposite_side(repo, service):
    _setup_chain(repo)
    repo.add_rate(
        "monobank",
        "PLN",
        "UAH",
        mid=4.0,
        buy=None,
        sell=4.1,
        valid_from=T - timedelta(hours=1),
        polled=T - timedelta(seconds=30),
        interval=60,
    )
    event = make_event(amount_cents=10000)
    result = await service.convert(None, event)
    # Direct conversion: buy=NULL, should use mid=4.0, NOT sell=4.1
    assert result.amounts[Currency.UAH] == 40000


async def test_compounded_spread_in_2_hop(repo, service):
    _setup_chain(repo)
    repo.add_rate(
        "monobank",
        "PLN",
        "UAH",
        mid=4.0,
        buy=3.9,
        sell=4.1,
        valid_from=T - timedelta(hours=1),
        polled=T - timedelta(seconds=30),
        interval=60,
    )
    repo.add_rate(
        "monobank",
        "UAH",
        "USD",
        mid=0.025,
        buy=0.025,
        sell=0.026,
        valid_from=T - timedelta(hours=1),
        polled=T - timedelta(seconds=30),
        interval=60,
    )
    event = make_event(amount_cents=10000)
    result = await service.convert(None, event)
    meta = result.rate_metadata["rate_usd"]
    # effective_rate = 3.9 * 0.025 = 0.0975
    assert meta["effective_rate"] == str(Decimal("3.9") * Decimal("0.025"))
    assert result.amounts[Currency.USD] == 975
