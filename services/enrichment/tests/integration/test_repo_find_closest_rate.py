"""Integration tests for CurrencyRateRepo.find_closest_rate.

Tests run against real TimescaleDB. Each test gets a rolled-back transaction.
"""

from datetime import timedelta

import pytest

from tests.helpers import T
from tests.integration.helpers import insert_rate, insert_source_config

pytestmark = pytest.mark.asyncio


async def test_poll_based_rank_by_last_polled_at_distance(conn, rate_repo):
    await insert_source_config(conn, source="monobank")
    await insert_rate(
        conn,
        source="monobank",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=10.0,
        valid_from=T - timedelta(days=6),
        last_polled_at=T - timedelta(days=5),
        update_cadence_seconds=60,
    )
    await insert_rate(
        conn,
        source="monobank",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=11.0,
        valid_from=T - timedelta(days=3),
        last_polled_at=T - timedelta(days=2),
        update_cadence_seconds=60,
    )
    row = await rate_repo.find_closest_rate(conn, "PLN", "UAH", T, ["monobank"])
    from decimal import Decimal

    assert row.rate_mid == Decimal("11.0")
    assert row.proximity_seconds == pytest.approx(172800, abs=1)


async def test_poll_based_rank_by_valid_from_when_before(conn, rate_repo):
    await insert_source_config(conn, source="monobank")
    await insert_rate(
        conn,
        source="monobank",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=11.0,
        valid_from=T + timedelta(days=2),
        last_polled_at=T + timedelta(days=5),
        update_cadence_seconds=60,
    )
    row = await rate_repo.find_closest_rate(conn, "PLN", "UAH", T, ["monobank"])
    assert row is not None
    assert row.proximity_seconds == pytest.approx(172800, abs=1)


async def test_poll_based_proximity_zero_inside_interval(conn, rate_repo):
    await insert_source_config(conn, source="monobank")
    await insert_rate(
        conn,
        source="monobank",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=11.0,
        valid_from=T - timedelta(days=10),
        last_polled_at=T - timedelta(days=2),
        update_cadence_seconds=60,
    )
    at_time = T - timedelta(days=5)
    row = await rate_repo.find_closest_rate(conn, "PLN", "UAH", at_time, ["monobank"])
    assert row is not None
    assert row.proximity_seconds == 0


async def test_historical_rank_by_valid_from_distance(conn, rate_repo):
    await insert_source_config(conn, source="nbu")
    await insert_rate(
        conn,
        source="nbu",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=10.0,
        valid_from=T - timedelta(days=10),
    )
    await insert_rate(
        conn,
        source="nbu",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=11.0,
        valid_from=T - timedelta(days=2),
    )
    row = await rate_repo.find_closest_rate(conn, "PLN", "UAH", T, ["nbu"])
    from decimal import Decimal

    assert row.rate_mid == Decimal("11.0")


async def test_historical_future_vs_past_ranks_by_proximity(conn, rate_repo):
    await insert_source_config(conn, source="nbu")
    await insert_rate(
        conn,
        source="nbu",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=10.0,
        valid_from=T - timedelta(days=3),
    )
    await insert_rate(
        conn,
        source="nbu",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=11.0,
        valid_from=T + timedelta(days=1),
    )
    row = await rate_repo.find_closest_rate(conn, "PLN", "UAH", T, ["nbu"])
    from decimal import Decimal

    assert row.rate_mid == Decimal("11.0")  # 1d < 3d


async def test_mixed_poll_and_historical_unified_ranking(conn, rate_repo):
    await insert_source_config(conn, source="monobank")
    await insert_source_config(conn, source="nbu")
    await insert_rate(
        conn,
        source="monobank",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=10.0,
        valid_from=T - timedelta(days=5),
        last_polled_at=T - timedelta(days=3),
        update_cadence_seconds=60,
    )
    await insert_rate(
        conn,
        source="nbu",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=11.0,
        valid_from=T - timedelta(days=1),
    )
    row = await rate_repo.find_closest_rate(conn, "PLN", "UAH", T, ["monobank", "nbu"])
    from decimal import Decimal

    assert row.rate_mid == Decimal("11.0")  # NBU 1d < Monobank 3d


async def test_tiebreak_chain_order_wins(conn, rate_repo):
    await insert_source_config(conn, source="monobank")
    await insert_source_config(conn, source="nbu")
    # Both at 1d proximity
    await insert_rate(
        conn,
        source="monobank",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=10.0,
        valid_from=T - timedelta(days=2),
        last_polled_at=T - timedelta(days=1),
        update_cadence_seconds=60,
    )
    await insert_rate(
        conn,
        source="nbu",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=11.0,
        valid_from=T - timedelta(days=1),
    )
    row = await rate_repo.find_closest_rate(conn, "PLN", "UAH", T, ["monobank", "nbu"])
    assert row.source == "monobank"


async def test_tiebreak_lower_id_wins_within_source(conn, rate_repo):
    await insert_source_config(conn, source="monobank")
    # Two rows with same proximity (both polled at T-2d)
    await insert_rate(
        conn,
        source="monobank",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=10.0,
        valid_from=T - timedelta(days=3),
        last_polled_at=T - timedelta(days=2),
        update_cadence_seconds=60,
    )
    await insert_rate(
        conn,
        source="monobank",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=11.0,
        valid_from=T - timedelta(days=4),
        last_polled_at=T - timedelta(days=2),
        update_cadence_seconds=60,
    )
    row = await rate_repo.find_closest_rate(conn, "PLN", "UAH", T, ["monobank"])
    # Lower id wins — first inserted row
    from decimal import Decimal

    assert row.rate_mid == Decimal("10.0")


async def test_respects_source_any_filter(conn, rate_repo):
    await insert_source_config(conn, source="monobank")
    await insert_source_config(conn, source="nbu")
    await insert_rate(
        conn,
        source="monobank",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=11.0,
        valid_from=T - timedelta(days=1),
        last_polled_at=T - timedelta(days=1),
        update_cadence_seconds=60,
    )
    # Only ask for nbu
    row = await rate_repo.find_closest_rate(conn, "PLN", "UAH", T, ["nbu"])
    assert row is None


async def test_none_beyond_7_day_window(conn, rate_repo):
    await insert_source_config(conn, source="monobank")
    await insert_rate(
        conn,
        source="monobank",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=11.0,
        valid_from=T - timedelta(days=10),
        last_polled_at=T - timedelta(days=8),
        update_cadence_seconds=60,
    )
    row = await rate_repo.find_closest_rate(conn, "PLN", "UAH", T, ["monobank"])
    assert row is None


async def test_exactly_at_7_day_boundary(conn, rate_repo):
    await insert_source_config(conn, source="nbu")
    await insert_rate(
        conn,
        source="nbu",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=11.0,
        valid_from=T - timedelta(days=7),
    )
    row = await rate_repo.find_closest_rate(conn, "PLN", "UAH", T, ["nbu"])
    assert row is not None


async def test_eligibility_uses_each_rows_own_metric(conn, rate_repo):
    await insert_source_config(conn, source="monobank")
    await insert_source_config(conn, source="nbu")
    # Monobank: polled 8d ago -> proximity 8d > 7d
    await insert_rate(
        conn,
        source="monobank",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=10.0,
        valid_from=T - timedelta(days=9),
        last_polled_at=T - timedelta(days=8),
        update_cadence_seconds=60,
    )
    # NBU: valid_from 8d ago -> proximity 8d > 7d
    await insert_rate(
        conn,
        source="nbu",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=11.0,
        valid_from=T - timedelta(days=8),
    )
    row = await rate_repo.find_closest_rate(conn, "PLN", "UAH", T, ["monobank", "nbu"])
    assert row is None


async def test_returns_proximity_seconds_as_integer(conn, rate_repo):
    await insert_source_config(conn, source="nbu")
    await insert_rate(
        conn,
        source="nbu",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=11.0,
        valid_from=T - timedelta(days=1),
    )
    row = await rate_repo.find_closest_rate(conn, "PLN", "UAH", T, ["nbu"])
    assert isinstance(row.proximity_seconds, int)
