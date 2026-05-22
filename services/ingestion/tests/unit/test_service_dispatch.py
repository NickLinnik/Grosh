"""Unit tests for CurrencyRateService dispatch logic."""

import logging
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import asyncpg
import pytest
from grosh_shared.domain.models import RateSource

from grosh_ingestion.models import NormalizedRate, RateKind, RateProviderConfig
from grosh_ingestion.services.currency_rate_service import CurrencyRateService

T = datetime(2025, 6, 1, 12, 0, tzinfo=UTC)


def _make_rate(
    *,
    source: str = RateSource.monobank,
    currency_from: str = "USD",
    currency_to: str = "UAH",
    rate_mid: Decimal = Decimal("40"),
    at_time: datetime = T,
) -> NormalizedRate:
    return NormalizedRate(
        source=source,
        currency_from=currency_from,
        currency_to=currency_to,
        rate_buy=None,
        rate_sell=None,
        rate_mid=rate_mid,
        at_time=at_time,
    )


def _make_pool() -> MagicMock:
    """Return a mock asyncpg.Pool whose acquire() context manager yields a mock conn."""
    mock_conn = AsyncMock(spec=asyncpg.Connection)
    pool = MagicMock(spec=asyncpg.Pool)
    pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
    pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
    return pool


def _make_repo() -> AsyncMock:
    repo = AsyncMock()
    repo.upsert_polled = AsyncMock()
    repo.upsert_historical = AsyncMock()
    return repo


async def test_historical_kind_routes_to_upsert_historical():
    repo = _make_repo()
    service = CurrencyRateService(repo)
    config = RateProviderConfig(
        source=RateSource.nbu,
        fetch=AsyncMock(),
        interval_seconds=86400,
        kind=RateKind.HISTORICAL,
    )
    rate = _make_rate(source=RateSource.nbu)

    await service.ingest_rates(_make_pool(), [rate], config)

    repo.upsert_historical.assert_called_once()
    repo.upsert_polled.assert_not_called()


async def test_polled_kind_routes_to_upsert_polled():
    repo = _make_repo()
    service = CurrencyRateService(repo)
    config = RateProviderConfig(
        source=RateSource.monobank,
        fetch=AsyncMock(),
        interval_seconds=300,
        kind=RateKind.POLLED,
    )
    rate = _make_rate()

    await service.ingest_rates(_make_pool(), [rate], config)

    repo.upsert_polled.assert_called_once()
    call_kwargs = repo.upsert_polled.call_args.kwargs
    assert call_kwargs["update_cadence_seconds"] == 300
    repo.upsert_historical.assert_not_called()


async def test_batch_of_n_rates_calls_upsert_n_times():
    repo = _make_repo()
    service = CurrencyRateService(repo)
    config = RateProviderConfig(
        source=RateSource.monobank,
        fetch=AsyncMock(),
        interval_seconds=300,
        kind=RateKind.POLLED,
    )
    rates = [_make_rate(currency_from=f"C{i}") for i in range(5)]

    await service.ingest_rates(_make_pool(), rates, config)

    assert repo.upsert_polled.call_count == 5


@pytest.mark.parametrize("kind", [RateKind.POLLED, RateKind.HISTORICAL])
async def test_only_relevant_upsert_called_per_kind(kind: RateKind):
    repo = _make_repo()
    service = CurrencyRateService(repo)
    config = RateProviderConfig(
        source=RateSource.monobank,
        fetch=AsyncMock(),
        interval_seconds=300,
        kind=kind,
    )
    rate = _make_rate()

    await service.ingest_rates(_make_pool(), [rate], config)

    if kind is RateKind.POLLED:
        repo.upsert_polled.assert_called_once()
        repo.upsert_historical.assert_not_called()
    else:
        repo.upsert_historical.assert_called_once()
        repo.upsert_polled.assert_not_called()


async def test_per_rate_at_time_passed_through():
    repo = _make_repo()
    service = CurrencyRateService(repo)
    config = RateProviderConfig(
        source=RateSource.monobank,
        fetch=AsyncMock(),
        interval_seconds=300,
        kind=RateKind.POLLED,
    )
    rate = _make_rate(at_time=T)

    await service.ingest_rates(_make_pool(), [rate], config)

    call_kwargs = repo.upsert_polled.call_args.kwargs
    assert call_kwargs["at_time"] == T


async def test_interval_seconds_passed_as_update_cadence_seconds():
    repo = _make_repo()
    service = CurrencyRateService(repo)
    config = RateProviderConfig(
        source=RateSource.monobank,
        fetch=AsyncMock(),
        interval_seconds=600,
        kind=RateKind.POLLED,
    )
    rate = _make_rate()

    await service.ingest_rates(_make_pool(), [rate], config)

    call_kwargs = repo.upsert_polled.call_args.kwargs
    assert call_kwargs["update_cadence_seconds"] == 600


async def test_empty_batch_is_noop(caplog: pytest.LogCaptureFixture):
    repo = _make_repo()
    service = CurrencyRateService(repo)
    config = RateProviderConfig(
        source=RateSource.monobank,
        fetch=AsyncMock(),
        interval_seconds=300,
        kind=RateKind.POLLED,
    )

    with caplog.at_level(
        logging.INFO, logger="grosh_ingestion.services.currency_rate_service"
    ):
        await service.ingest_rates(_make_pool(), [], config)

    repo.upsert_polled.assert_not_called()
    repo.upsert_historical.assert_not_called()
    assert any("0" in msg for msg in caplog.messages)


async def test_repo_exception_propagates():
    repo = _make_repo()
    repo.upsert_polled.side_effect = RuntimeError("db down")
    service = CurrencyRateService(repo)
    config = RateProviderConfig(
        source=RateSource.monobank,
        fetch=AsyncMock(),
        interval_seconds=300,
        kind=RateKind.POLLED,
    )
    rate = _make_rate()

    with pytest.raises(RuntimeError, match="db down"):
        await service.ingest_rates(_make_pool(), [rate], config)
