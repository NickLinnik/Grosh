"""Unit test fixtures for grosh-consumer, including InMemoryRateRepo."""

from decimal import Decimal

import pytest

from grosh_consumer.repositories.currency_rate_repo import RateRow, SourceConfig
from grosh_consumer.services.currency_conversion_service import (
    CurrencyConversionService,
)


class InMemoryRateRepo:
    """In-memory implementation of CurrencyRateRepo for unit tests."""

    def __init__(self):
        self._sources: list[SourceConfig] = []
        self._rates: list[dict] = []

    def add_source(
        self, source, max_staleness_seconds=300, base_currencies=("UAH",), fallback=None
    ):
        """Add a source config. Call in chain order (head first)."""
        self._sources.append(
            SourceConfig(
                source=source,
                max_staleness_seconds=max_staleness_seconds,
                base_currencies=list(base_currencies),
            )
        )

    def add_rate(
        self,
        source,
        from_,
        to_,
        *,
        mid,
        buy=None,
        sell=None,
        valid_from,
        valid_to=None,
        polled=None,
    ):
        """Add a rate record."""
        self._rates.append(
            {
                "id": len(self._rates) + 1,
                "source": source,
                "currency_from": from_,
                "currency_to": to_,
                "rate_mid": Decimal(str(mid)),
                "rate_buy": Decimal(str(buy)) if buy is not None else None,
                "rate_sell": Decimal(str(sell)) if sell is not None else None,
                "valid_from": valid_from,
                "valid_to": valid_to,
                "last_polled_at": polled,
            }
        )

    async def find_rate_at_time(
        self, conn, source, currency_from, currency_to, at_time
    ):
        """Match CurrencyRateRepo.find_rate_at_time semantics."""
        matches = [
            r
            for r in self._rates
            if r["source"] == source
            and r["currency_from"] == currency_from
            and r["currency_to"] == currency_to
            and r["valid_from"] <= at_time
            and (r["valid_to"] is None or r["valid_to"] > at_time)
        ]
        if not matches:
            return None
        best = max(matches, key=lambda r: r["valid_from"])
        return RateRow(
            id=best["id"],
            source=best["source"],
            rate_mid=best["rate_mid"],
            rate_buy=best["rate_buy"],
            rate_sell=best["rate_sell"],
            last_polled_at=best["last_polled_at"],
        )

    async def find_closest_rate(
        self, conn, currency_from, currency_to, at_time, sources
    ):
        """Match CurrencyRateRepo.find_closest_rate production semantics.

        Filters: source in allowed list, last_polled_at IS NOT NULL,
        rate within 7-day window (past via last_polled_at distance,
        future via valid_from distance). Ranks by the closer of the
        two distances.
        """
        max_age = 7 * 86400
        matches = []
        for r in self._rates:
            if r["currency_from"] != currency_from:
                continue
            if r["currency_to"] != currency_to:
                continue
            if r["source"] not in sources:
                continue
            if r["last_polled_at"] is None:
                continue

            polled = r["last_polled_at"]
            vf = r["valid_from"]
            past_dist = (at_time - polled).total_seconds()
            future_dist = (vf - at_time).total_seconds()

            # Past window: last_polled_at < at_time
            # and within max_age seconds
            in_past = 0 < past_dist <= max_age
            # Future window: valid_from > at_time
            # and within max_age seconds
            in_future = 0 < future_dist <= max_age

            if not (in_past or in_future):
                continue

            dist = min(
                abs(past_dist) if in_past else float("inf"),
                abs(future_dist) if in_future else float("inf"),
            )
            matches.append((r, dist))

        if not matches:
            return None
        best, _ = min(matches, key=lambda x: x[1])
        return RateRow(
            id=best["id"],
            source=best["source"],
            rate_mid=best["rate_mid"],
            rate_buy=best["rate_buy"],
            rate_sell=best["rate_sell"],
        )

    async def load_source_chain(self, conn, entry_source):
        """Return the configured source chain."""
        return list(self._sources)


@pytest.fixture
def repo():
    return InMemoryRateRepo()


@pytest.fixture
def service(repo):
    return CurrencyConversionService(repo)
