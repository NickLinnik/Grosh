"""Unit tests for the NBU historical currency rates provider."""

from datetime import UTC, date, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, patch

from grosh_ingestion.sources.nbu.client import NbuHistoricalRate
from grosh_ingestion.sources.nbu.rates_provider import fetch_historical_rates

_PATCH_FETCH = "grosh_ingestion.sources.nbu.rates_provider.fetch_nbu_historical_rates"
_PATCH_CODES = "grosh_ingestion.sources.nbu.rates_provider.all_alpha_codes"

_FROM = date(2024, 1, 1)
_TO = date(2024, 1, 2)


def _make_hist(
    *, currency_code_l: str, amount: float, units: int = 1, start_date: str
) -> NbuHistoricalRate:
    return NbuHistoricalRate(
        StartDate=start_date,
        CurrencyCode="XXX",
        CurrencyCodeL=currency_code_l,
        Units=units,
        Amount=amount,
    )


async def test_iterates_currencies_excludes_uah():
    mock_fetch = AsyncMock(return_value=[])
    with (
        patch(_PATCH_CODES, return_value=["USD", "EUR", "UAH"]),
        patch(_PATCH_FETCH, new=mock_fetch),
    ):
        await fetch_historical_rates(_FROM, _TO)

    assert mock_fetch.call_count == 2
    called_codes = {c.args[2] for c in mock_fetch.call_args_list}
    assert called_codes == {"USD", "EUR"}
    uah_calls = [c for c in mock_fetch.call_args_list if c.args[2] == "UAH"]
    assert uah_calls == []


async def test_normalizes_amount_units():
    raw = _make_hist(
        currency_code_l="JPY", amount=100, units=2, start_date="01.01.2024"
    )
    with (
        patch(_PATCH_CODES, return_value=["JPY"]),
        patch(_PATCH_FETCH, new=AsyncMock(return_value=[raw])),
    ):
        result = await fetch_historical_rates(_FROM, _TO)

    assert len(result) == 1
    assert result[0].rate_mid == Decimal("100") / Decimal("2")


async def test_preserves_precision_for_non_integer_rates():
    raw = _make_hist(
        currency_code_l="USD", amount=37.1234, units=1, start_date="01.01.2024"
    )
    with (
        patch(_PATCH_CODES, return_value=["USD"]),
        patch(_PATCH_FETCH, new=AsyncMock(return_value=[raw])),
    ):
        result = await fetch_historical_rates(_FROM, _TO)

    assert result[0].rate_mid == Decimal("37.1234")


async def test_returns_one_rate_per_currency_per_date():
    dates = ["01.01.2024", "02.01.2024", "03.01.2024"]
    usd_rates = [
        _make_hist(currency_code_l="USD", amount=41.0, units=1, start_date=d)
        for d in dates
    ]
    eur_rates = [
        _make_hist(currency_code_l="EUR", amount=45.0, units=1, start_date=d)
        for d in dates
    ]

    async def _side_effect(from_date, to_date, val_code):
        if val_code == "USD":
            return usd_rates
        return eur_rates

    with (
        patch(_PATCH_CODES, return_value=["USD", "EUR"]),
        patch(_PATCH_FETCH, new=AsyncMock(side_effect=_side_effect)),
    ):
        result = await fetch_historical_rates(_FROM, _TO)

    assert len(result) == 6


async def test_date_parsing_dd_mm_yyyy():
    raw = _make_hist(
        currency_code_l="USD", amount=41.0, units=1, start_date="15.03.2024"
    )
    with (
        patch(_PATCH_CODES, return_value=["USD"]),
        patch(_PATCH_FETCH, new=AsyncMock(return_value=[raw])),
    ):
        result = await fetch_historical_rates(_FROM, _TO)

    assert result[0].at_time == datetime(2024, 3, 15, tzinfo=UTC)
