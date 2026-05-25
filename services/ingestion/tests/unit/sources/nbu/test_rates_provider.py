"""Unit tests for the NBU live currency rates provider."""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, patch

from grosh_shared.domain.models import RateSource

from grosh_ingestion.sources.nbu.client import NbuRate
from grosh_ingestion.sources.nbu.rates_provider import fetch_rates

_PATCH_TARGET = "grosh_ingestion.sources.nbu.rates_provider.fetch_nbu_rates"


def _make_nbu_rate(*, cc: str, rate: float, exchange_date: str) -> NbuRate:
    return NbuRate(
        r030=840,
        txt=cc,
        rate=rate,
        cc=cc,
        exchangedate=exchange_date,
    )


async def test_normalizes_single_entry():
    raw = _make_nbu_rate(cc="USD", rate=41.0, exchange_date="01.06.2025")
    with patch(_PATCH_TARGET, new=AsyncMock(return_value=[raw])):
        result = await fetch_rates()

    assert len(result) == 1
    r = result[0]
    assert r.source == RateSource.nbu
    assert r.currency_from == "USD"
    assert r.currency_to == "UAH"
    assert r.rate_buy is None
    assert r.rate_sell is None
    assert r.rate_mid == Decimal("41.0")
    assert r.at_time == datetime(2025, 6, 1, tzinfo=UTC)


async def test_handles_multiple_currencies():
    raw = [
        _make_nbu_rate(cc="USD", rate=41.0, exchange_date="01.06.2025"),
        _make_nbu_rate(cc="EUR", rate=45.0, exchange_date="01.06.2025"),
        _make_nbu_rate(cc="GBP", rate=52.0, exchange_date="01.06.2025"),
    ]
    with patch(_PATCH_TARGET, new=AsyncMock(return_value=raw)):
        result = await fetch_rates()

    assert len(result) == 3
    assert all(r.currency_to == "UAH" for r in result)
    codes = {r.currency_from for r in result}
    assert codes == {"USD", "EUR", "GBP"}


async def test_parses_nbu_date_format():
    raw = _make_nbu_rate(cc="USD", rate=41.0, exchange_date="15.03.2024")
    with patch(_PATCH_TARGET, new=AsyncMock(return_value=[raw])):
        result = await fetch_rates()

    assert result[0].at_time == datetime(2024, 3, 15, tzinfo=UTC)


async def test_rate_buy_and_sell_always_none():
    raw = [
        _make_nbu_rate(cc="USD", rate=41.0, exchange_date="01.06.2025"),
        _make_nbu_rate(cc="EUR", rate=45.0, exchange_date="01.06.2025"),
    ]
    with patch(_PATCH_TARGET, new=AsyncMock(return_value=raw)):
        result = await fetch_rates()

    for r in result:
        assert r.rate_buy is None
        assert r.rate_sell is None
