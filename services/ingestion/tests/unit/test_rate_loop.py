"""Unit tests for the _rate_loop background task in main.py."""

import asyncio
import logging
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import asyncpg
import pytest
from grosh_shared.models import RateSource

from grosh_ingestion.main import _rate_loop
from grosh_ingestion.models import NormalizedRate, RateKind, RateProviderConfig

T = datetime(2025, 6, 1, 12, 0, tzinfo=UTC)


def _make_rate() -> NormalizedRate:
    return NormalizedRate(
        source=RateSource.monobank,
        currency_from="USD",
        currency_to="UAH",
        rate_buy=None,
        rate_sell=None,
        rate_mid=Decimal("40"),
        at_time=T,
    )


def _make_config(interval_seconds: int = 300) -> RateProviderConfig:
    return RateProviderConfig(
        source=RateSource.monobank,
        fetch=AsyncMock(return_value=[_make_rate()]),
        interval_seconds=interval_seconds,
        kind=RateKind.POLLED,
    )


async def test_normal_cycle_fetch_ingest_sleep():
    config = _make_config()
    pool = MagicMock(spec=asyncpg.Pool)
    service = MagicMock()
    service.ingest_rates = AsyncMock()

    call_order: list[str] = []

    async def fake_fetch():
        call_order.append("fetch")
        return [_make_rate()]

    async def fake_ingest(p, rates, cfg):
        call_order.append("ingest")

    async def fake_sleep(seconds):
        call_order.append("sleep")
        raise asyncio.CancelledError

    config = RateProviderConfig(
        source=RateSource.monobank,
        fetch=fake_fetch,
        interval_seconds=300,
        kind=RateKind.POLLED,
    )
    service.ingest_rates = fake_ingest

    with patch("grosh_ingestion.main.asyncio.sleep", side_effect=fake_sleep):
        with pytest.raises(asyncio.CancelledError):
            await _rate_loop(pool, service, config)

    assert call_order == ["fetch", "ingest", "sleep"]


async def test_fetch_raises_logs_skips_ingest_still_sleeps(
    caplog: pytest.LogCaptureFixture,
):
    pool = MagicMock(spec=asyncpg.Pool)
    service = MagicMock()
    service.ingest_rates = AsyncMock()

    sleep_called = False

    async def fake_sleep(seconds):
        nonlocal sleep_called
        sleep_called = True
        raise asyncio.CancelledError

    config = RateProviderConfig(
        source=RateSource.monobank,
        fetch=AsyncMock(side_effect=RuntimeError("api down")),
        interval_seconds=300,
        kind=RateKind.POLLED,
    )

    with caplog.at_level(logging.ERROR, logger="grosh_ingestion.main"):
        with patch("grosh_ingestion.main.asyncio.sleep", side_effect=fake_sleep):
            with pytest.raises(asyncio.CancelledError):
                await _rate_loop(pool, service, config)

    service.ingest_rates.assert_not_called()
    assert sleep_called
    assert any(RateSource.monobank in msg for msg in caplog.messages)


async def test_ingest_raises_logs_still_sleeps_fetch_next_cycle(
    caplog: pytest.LogCaptureFixture,
):
    pool = MagicMock(spec=asyncpg.Pool)

    cycle = 0
    sleep_count = 0
    ingest_count = 0
    fetch_count = 0

    async def fake_fetch():
        nonlocal fetch_count
        fetch_count += 1
        return [_make_rate()]

    async def fake_ingest(p, rates, cfg):
        nonlocal ingest_count, cycle
        ingest_count += 1
        if cycle == 0:
            raise RuntimeError("write failed")

    async def fake_sleep(seconds):
        nonlocal cycle, sleep_count
        sleep_count += 1
        cycle += 1
        if cycle >= 2:
            raise asyncio.CancelledError

    service = MagicMock()
    service.ingest_rates = fake_ingest

    config = RateProviderConfig(
        source=RateSource.monobank,
        fetch=fake_fetch,
        interval_seconds=300,
        kind=RateKind.POLLED,
    )

    with caplog.at_level(logging.ERROR, logger="grosh_ingestion.main"):
        with patch("grosh_ingestion.main.asyncio.sleep", side_effect=fake_sleep):
            with pytest.raises(asyncio.CancelledError):
                await _rate_loop(pool, service, config)

    assert sleep_count >= 1
    assert fetch_count >= 2
    assert ingest_count >= 2
    assert any("currency rates" in msg for msg in caplog.messages)


async def test_sleep_duration_matches_config():
    pool = MagicMock(spec=asyncpg.Pool)
    service = MagicMock()
    service.ingest_rates = AsyncMock()

    sleep_args: list[int] = []

    async def fake_sleep(seconds):
        sleep_args.append(seconds)
        raise asyncio.CancelledError

    config = RateProviderConfig(
        source=RateSource.monobank,
        fetch=AsyncMock(return_value=[_make_rate()]),
        interval_seconds=42,
        kind=RateKind.POLLED,
    )

    with patch("grosh_ingestion.main.asyncio.sleep", side_effect=fake_sleep):
        with pytest.raises(asyncio.CancelledError):
            await _rate_loop(pool, service, config)

    assert sleep_args == [42]
