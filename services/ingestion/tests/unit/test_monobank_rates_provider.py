"""Unit tests for the Monobank currency rates provider."""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import pytest
from grosh_shared.models import RateSource

from grosh_ingestion.sources.monobank.models import MonobankCurrencyRate
from grosh_ingestion.sources.monobank.rates_provider import fetch_rates

_PATCH_TARGET = "grosh_ingestion.sources.monobank.rates_provider.fetch_currency_rates"

_DATE_TS = 1717243200  # 2024-06-01T12:00:00Z


def _make_raw(
    *,
    currency_code_a: int = 840,
    currency_code_b: int = 980,
    date: int = _DATE_TS,
    rate_buy: float | None = None,
    rate_sell: float | None = None,
    rate_cross: float | None = None,
) -> MonobankCurrencyRate:
    return MonobankCurrencyRate(
        currencyCodeA=currency_code_a,
        currencyCodeB=currency_code_b,
        date=date,
        rateBuy=rate_buy,
        rateSell=rate_sell,
        rateCross=rate_cross,
    )


async def test_normalizes_standard_entry_with_buy_sell():
    raw = _make_raw(rate_buy=39.0, rate_sell=41.0)
    with patch(_PATCH_TARGET, new=AsyncMock(return_value=[raw])):
        result = await fetch_rates()

    assert len(result) == 1
    r = result[0]
    assert r.source == RateSource.monobank
    assert r.currency_from == "USD"
    assert r.currency_to == "UAH"
    assert r.rate_buy == Decimal("39.0")
    assert r.rate_sell == Decimal("41.0")
    assert r.rate_mid == Decimal("40.0")
    assert r.at_time == datetime.fromtimestamp(_DATE_TS, tz=UTC)
    assert r.at_time.tzinfo is UTC


async def test_prefers_rate_cross_over_midpoint():
    raw = _make_raw(rate_buy=39.0, rate_sell=41.0, rate_cross=40.5)
    with patch(_PATCH_TARGET, new=AsyncMock(return_value=[raw])):
        result = await fetch_rates()

    assert len(result) == 1
    assert result[0].rate_mid == Decimal("40.5")


async def test_computes_midpoint_when_only_buy_sell():
    raw = _make_raw(rate_buy=39.0, rate_sell=41.0, rate_cross=None)
    with patch(_PATCH_TARGET, new=AsyncMock(return_value=[raw])):
        result = await fetch_rates()

    assert len(result) == 1
    assert result[0].rate_mid == Decimal("40.0")


async def test_drops_entry_with_no_rate_data():
    raw = _make_raw(rate_buy=None, rate_sell=None, rate_cross=None)
    with patch(_PATCH_TARGET, new=AsyncMock(return_value=[raw])):
        result = await fetch_rates()

    assert result == []


async def test_drops_entry_with_buy_only():
    raw = _make_raw(rate_buy=39.0, rate_sell=None, rate_cross=None)
    with patch(_PATCH_TARGET, new=AsyncMock(return_value=[raw])):
        result = await fetch_rates()

    assert result == []


async def test_drops_entry_with_sell_only():
    raw = _make_raw(rate_buy=None, rate_sell=41.0, rate_cross=None)
    with patch(_PATCH_TARGET, new=AsyncMock(return_value=[raw])):
        result = await fetch_rates()

    assert result == []


async def test_drops_entry_with_unknown_numeric_code():
    raw = _make_raw(currency_code_a=1, rate_buy=39.0, rate_sell=41.0)
    with patch(_PATCH_TARGET, new=AsyncMock(return_value=[raw])):
        result = await fetch_rates()

    assert result == []


async def test_processes_multiple_entries_independently():
    valid = _make_raw(rate_buy=39.0, rate_sell=41.0)
    unknown_code = _make_raw(currency_code_a=1, rate_buy=39.0, rate_sell=41.0)
    missing_rates = _make_raw(rate_buy=None, rate_sell=None, rate_cross=None)

    with patch(
        _PATCH_TARGET, new=AsyncMock(return_value=[valid, unknown_code, missing_rates])
    ):
        result = await fetch_rates()

    assert len(result) == 1
    assert result[0].currency_from == "USD"


@pytest.mark.parametrize(
    "rate_buy,rate_sell,rate_cross,expect_buy,expect_sell",
    [
        (39.0, 41.0, None, Decimal("39.0"), Decimal("41.0")),
        (None, None, 40.0, None, None),
        (39.0, 41.0, 40.5, Decimal("39.0"), Decimal("41.0")),
    ],
)
async def test_rate_buy_sell_decimal_or_none(
    rate_buy, rate_sell, rate_cross, expect_buy, expect_sell
):
    raw = _make_raw(rate_buy=rate_buy, rate_sell=rate_sell, rate_cross=rate_cross)
    with patch(_PATCH_TARGET, new=AsyncMock(return_value=[raw])):
        result = await fetch_rates()

    assert len(result) == 1
    r = result[0]
    assert r.rate_buy == expect_buy
    assert r.rate_sell == expect_sell
    if expect_buy is not None:
        assert isinstance(r.rate_buy, Decimal)
    if expect_sell is not None:
        assert isinstance(r.rate_sell, Decimal)
