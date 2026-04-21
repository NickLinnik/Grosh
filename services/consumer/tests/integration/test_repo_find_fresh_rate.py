"""Integration tests for CurrencyRateRepo.find_fresh_rate.

Tests run against real TimescaleDB. Each test gets a rolled-back transaction.
"""

from datetime import timedelta

import pytest

from tests.helpers import T
from tests.integration.helpers import insert_rate, insert_source_config

pytestmark = pytest.mark.asyncio


async def test_returns_matching_row(conn, rate_repo):
    await insert_source_config(conn, source="monobank")
    await insert_rate(
        conn,
        source="monobank",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=11.0,
        valid_from=T - timedelta(hours=1),
        last_polled_at=T - timedelta(seconds=30),
        update_cadence_seconds=60,
    )
    row = await rate_repo.find_fresh_rate(conn, "monobank", "PLN", "UAH", T, 2)
    assert row is not None
    assert row.source == "monobank"


async def test_returns_none_for_historical_row(conn, rate_repo):
    await insert_source_config(conn, source="nbu")
    await insert_rate(
        conn,
        source="nbu",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=11.0,
        valid_from=T - timedelta(days=1),
    )
    row = await rate_repo.find_fresh_rate(conn, "nbu", "PLN", "UAH", T, 2)
    assert row is None


async def test_returns_none_for_poll_based_past_grace(conn, rate_repo):
    await insert_source_config(conn, source="monobank")
    await insert_rate(
        conn,
        source="monobank",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=11.0,
        valid_from=T - timedelta(hours=2),
        last_polled_at=T - timedelta(seconds=3600),
        update_cadence_seconds=60,
    )
    row = await rate_repo.find_fresh_rate(conn, "monobank", "PLN", "UAH", T, 2)
    assert row is None


async def test_at_exactly_cutoff_is_fresh(conn, rate_repo):
    await insert_source_config(conn, source="monobank")
    # polled=T-120s, interval=60, tolerance=2 -> grace=120s -> cutoff at T
    await insert_rate(
        conn,
        source="monobank",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=11.0,
        valid_from=T - timedelta(hours=1),
        last_polled_at=T - timedelta(seconds=120),
        update_cadence_seconds=60,
    )
    row = await rate_repo.find_fresh_rate(conn, "monobank", "PLN", "UAH", T, 2)
    assert row is not None


async def test_1s_past_cutoff_is_not_fresh(conn, rate_repo):
    await insert_source_config(conn, source="monobank")
    # polled=T-121s, interval=60, tolerance=2 -> grace=120s -> cutoff at T-1s
    await insert_rate(
        conn,
        source="monobank",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=11.0,
        valid_from=T - timedelta(hours=1),
        last_polled_at=T - timedelta(seconds=121),
        update_cadence_seconds=60,
    )
    row = await rate_repo.find_fresh_rate(conn, "monobank", "PLN", "UAH", T, 2)
    assert row is None


async def test_at_time_before_valid_from_returns_none(conn, rate_repo):
    await insert_source_config(conn, source="monobank")
    await insert_rate(
        conn,
        source="monobank",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=11.0,
        valid_from=T + timedelta(hours=1),
        last_polled_at=T - timedelta(seconds=30),
        update_cadence_seconds=60,
    )
    row = await rate_repo.find_fresh_rate(conn, "monobank", "PLN", "UAH", T, 2)
    assert row is None


async def test_at_time_before_last_polled_at_backfill_is_fresh(conn, rate_repo):
    await insert_source_config(conn, source="monobank")
    # valid_from far in past, polled in future — event at T is within grace
    await insert_rate(
        conn,
        source="monobank",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=11.0,
        valid_from=T - timedelta(days=30),
        last_polled_at=T + timedelta(seconds=100),
        update_cadence_seconds=60,
    )
    row = await rate_repo.find_fresh_rate(conn, "monobank", "PLN", "UAH", T, 2)
    assert row is not None


async def test_scd2_cap_valid_to_before_at_time(conn, rate_repo):
    await insert_source_config(conn, source="monobank")
    # Row has valid_to = T - 1min, so it's been superseded
    await insert_rate(
        conn,
        source="monobank",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=11.0,
        valid_from=T - timedelta(minutes=3),
        valid_to=T - timedelta(minutes=1),
        last_polled_at=T - timedelta(minutes=2),
        update_cadence_seconds=60,
    )
    row = await rate_repo.find_fresh_rate(conn, "monobank", "PLN", "UAH", T, 2)
    assert row is None


async def test_scd2_successor_returns_successor(conn, rate_repo):
    await insert_source_config(conn, source="monobank")
    # Row A: superseded
    await insert_rate(
        conn,
        source="monobank",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=10.0,
        valid_from=T - timedelta(minutes=3),
        valid_to=T - timedelta(minutes=1),
        last_polled_at=T - timedelta(minutes=2),
        update_cadence_seconds=60,
    )
    # Row B: current
    await insert_rate(
        conn,
        source="monobank",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=11.0,
        valid_from=T - timedelta(minutes=1),
        last_polled_at=T - timedelta(minutes=1),
        update_cadence_seconds=60,
    )
    row = await rate_repo.find_fresh_rate(conn, "monobank", "PLN", "UAH", T, 2)
    assert row is not None
    from decimal import Decimal

    assert row.rate_mid == Decimal("11.0")


async def test_malformed_row_interval_null_returns_none(conn, rate_repo):
    await insert_source_config(conn, source="monobank")
    await insert_rate(
        conn,
        source="monobank",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=11.0,
        valid_from=T - timedelta(hours=1),
        last_polled_at=T - timedelta(seconds=30),
        update_cadence_seconds=None,
    )
    row = await rate_repo.find_fresh_rate(conn, "monobank", "PLN", "UAH", T, 2)
    assert row is None


async def test_picks_latest_valid_from_when_multiple_fresh(conn, rate_repo):
    await insert_source_config(conn, source="monobank")
    await insert_rate(
        conn,
        source="monobank",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=10.0,
        valid_from=T - timedelta(hours=2),
        last_polled_at=T - timedelta(seconds=30),
        update_cadence_seconds=60,
    )
    await insert_rate(
        conn,
        source="monobank",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=11.0,
        valid_from=T - timedelta(hours=1),
        last_polled_at=T - timedelta(seconds=30),
        update_cadence_seconds=60,
    )
    row = await rate_repo.find_fresh_rate(conn, "monobank", "PLN", "UAH", T, 2)
    from decimal import Decimal

    assert row.rate_mid == Decimal("11.0")


async def test_filters_by_source(conn, rate_repo):
    await insert_source_config(conn, source="monobank")
    await insert_source_config(conn, source="nbu")
    await insert_rate(
        conn,
        source="nbu",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=11.0,
        valid_from=T - timedelta(hours=1),
        last_polled_at=T - timedelta(seconds=30),
        update_cadence_seconds=60,
    )
    row = await rate_repo.find_fresh_rate(conn, "monobank", "PLN", "UAH", T, 2)
    assert row is None


async def test_returns_all_expected_fields(conn, rate_repo):
    await insert_source_config(conn, source="monobank")
    await insert_rate(
        conn,
        source="monobank",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=11.0,
        rate_buy=10.9,
        rate_sell=11.1,
        valid_from=T - timedelta(hours=1),
        valid_to=T + timedelta(hours=1),
        last_polled_at=T - timedelta(seconds=30),
        update_cadence_seconds=60,
    )
    row = await rate_repo.find_fresh_rate(conn, "monobank", "PLN", "UAH", T, 2)
    from decimal import Decimal

    assert row.rate_mid == Decimal("11.0")
    assert row.rate_buy == Decimal("10.9")
    assert row.rate_sell == Decimal("11.1")
    assert row.valid_from is not None
    assert row.valid_to is not None
    assert row.last_polled_at is not None
    assert row.update_cadence_seconds == 60


async def test_poll_tolerance_parameter_respected(conn, rate_repo):
    await insert_source_config(conn, source="monobank")
    # polled=T-150s, interval=60
    # tolerance=2: grace=120s, cutoff=T-30s -> not FRESH
    # tolerance=3: grace=180s, cutoff=T+30s -> FRESH
    await insert_rate(
        conn,
        source="monobank",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=11.0,
        valid_from=T - timedelta(hours=1),
        last_polled_at=T - timedelta(seconds=150),
        update_cadence_seconds=60,
    )
    row2 = await rate_repo.find_fresh_rate(conn, "monobank", "PLN", "UAH", T, 2)
    assert row2 is None
    row3 = await rate_repo.find_fresh_rate(conn, "monobank", "PLN", "UAH", T, 3)
    assert row3 is not None
