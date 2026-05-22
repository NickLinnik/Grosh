"""Shared test fixtures for grosh-ingestion.

Provides time constants and NormalizedRate builder used by both unit and
integration tests.
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from grosh_shared.domain.models import RateSource

from grosh_ingestion.models import NormalizedRate

T = datetime(2025, 6, 1, 12, 0, tzinfo=UTC)
T_minus_1m = T - timedelta(minutes=1)
T_minus_1h = T - timedelta(hours=1)
T_minus_1d = T - timedelta(days=1)
T_plus_1d = T + timedelta(days=1)


@pytest.fixture
def make_rate():
    def _make(
        source: str = RateSource.monobank,
        currency_from: str = "USD",
        currency_to: str = "UAH",
        rate_mid: Decimal | int | str = 40,
        rate_buy: Decimal | int | str | None = None,
        rate_sell: Decimal | int | str | None = None,
        at_time: datetime = T,
    ) -> NormalizedRate:
        return NormalizedRate(
            source=source,
            currency_from=currency_from,
            currency_to=currency_to,
            rate_buy=Decimal(str(rate_buy)) if rate_buy is not None else None,
            rate_sell=Decimal(str(rate_sell)) if rate_sell is not None else None,
            rate_mid=Decimal(str(rate_mid)),
            at_time=at_time,
        )

    return _make
