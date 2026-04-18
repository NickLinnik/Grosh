"""Integration tests for CurrencyRateRepo.find_closest_rate.

Section 4.2 — 6 test functions.
"""

from datetime import timedelta
from decimal import Decimal

import asyncpg

from grosh_consumer.repositories.currency_rate_repo import CurrencyRateRepo
from tests.helpers import T
from tests.integration.helpers import insert_rate, insert_source_config

_7_DAYS = 7 * 86400


# ---------------------------------------------------------------------------
# 4.2.1  Picks closest by valid_from distance
# ---------------------------------------------------------------------------


async def test_picks_closest_by_distance(
    conn: asyncpg.Connection, rate_repo: CurrencyRateRepo
) -> None:
    await insert_source_config(conn, source="nbu")
    # Farther — 3 days before T (outside valid window, so CLOSEST applies)
    await insert_rate(
        conn,
        source="nbu",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=3.0,
        valid_from=T - timedelta(days=3),
        valid_to=T - timedelta(days=2),
        last_polled_at=T - timedelta(days=3),
    )
    # Closer — 1 day before T
    await insert_rate(
        conn,
        source="nbu",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=4.0,
        valid_from=T - timedelta(days=1),
        valid_to=T - timedelta(hours=1),
        last_polled_at=T - timedelta(days=1),
    )

    row = await rate_repo.find_closest_rate(conn, "PLN", "UAH", T, ["nbu"])

    assert row is not None
    assert row.rate_mid == Decimal("4.0")


# ---------------------------------------------------------------------------
# 4.2.2  Considers future rates
# ---------------------------------------------------------------------------


async def test_considers_future_rates(
    conn: asyncpg.Connection, rate_repo: CurrencyRateRepo
) -> None:
    await insert_source_config(conn, source="nbu")
    # Past rate — 2 days before T
    await insert_rate(
        conn,
        source="nbu",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=3.0,
        valid_from=T - timedelta(days=2),
        valid_to=T - timedelta(days=1),
        last_polled_at=T - timedelta(days=2),
    )
    # Future rate — 1 day after T (closer)
    await insert_rate(
        conn,
        source="nbu",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=4.5,
        valid_from=T + timedelta(days=1),
        last_polled_at=T + timedelta(days=1),
    )

    row = await rate_repo.find_closest_rate(conn, "PLN", "UAH", T, ["nbu"])

    assert row is not None
    assert row.rate_mid == Decimal("4.5")


# ---------------------------------------------------------------------------
# 4.2.3  Respects source ANY filter — ignores sources not in the list
# ---------------------------------------------------------------------------


async def test_respects_source_filter(
    conn: asyncpg.Connection, rate_repo: CurrencyRateRepo
) -> None:
    await insert_source_config(conn, source="monobank")
    await insert_source_config(conn, source="nbu")
    await insert_rate(
        conn,
        source="monobank",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=4.0,
        valid_from=T - timedelta(days=1),
        valid_to=T - timedelta(hours=1),
        last_polled_at=T - timedelta(days=1),
    )
    await insert_rate(
        conn,
        source="nbu",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=4.5,
        valid_from=T - timedelta(days=2),
        valid_to=T - timedelta(days=1),
        last_polled_at=T - timedelta(days=2),
    )

    # Only nbu in sources — monobank result (closer) must not appear
    row = await rate_repo.find_closest_rate(conn, "PLN", "UAH", T, ["nbu"])

    assert row is not None
    assert row.source == "nbu"


# ---------------------------------------------------------------------------
# 4.2.4  Returns None beyond 7-day window
# ---------------------------------------------------------------------------


async def test_returns_none_beyond_7_day_window(
    conn: asyncpg.Connection, rate_repo: CurrencyRateRepo
) -> None:
    await insert_source_config(conn, source="nbu")
    # 8 days before T — outside the window
    await insert_rate(
        conn,
        source="nbu",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=4.0,
        valid_from=T - timedelta(days=8),
        valid_to=T - timedelta(days=7, seconds=1),
        last_polled_at=T - timedelta(days=8),
    )

    row = await rate_repo.find_closest_rate(conn, "PLN", "UAH", T, ["nbu"])

    assert row is None


# ---------------------------------------------------------------------------
# 4.2.5  Exactly at 7-day boundary is inclusive
# ---------------------------------------------------------------------------


async def test_exactly_at_7_day_boundary_is_inclusive(
    conn: asyncpg.Connection, rate_repo: CurrencyRateRepo
) -> None:
    await insert_source_config(conn, source="nbu")
    # Exactly 7 days before T — must be returned
    await insert_rate(
        conn,
        source="nbu",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=4.0,
        valid_from=T - timedelta(days=7),
        valid_to=T - timedelta(days=6),
        last_polled_at=T - timedelta(days=7),
    )

    row = await rate_repo.find_closest_rate(conn, "PLN", "UAH", T, ["nbu"])

    assert row is not None
    assert row.rate_mid == Decimal("4.0")


# ---------------------------------------------------------------------------
# 4.2.6  Tie in distance is deterministic within a test run
# ---------------------------------------------------------------------------


async def test_tie_in_distance_is_deterministic(
    conn: asyncpg.Connection, rate_repo: CurrencyRateRepo
) -> None:
    await insert_source_config(conn, source="nbu")
    # Two rates equidistant from T (both 1 day away)
    await insert_rate(
        conn,
        source="nbu",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=3.0,
        valid_from=T - timedelta(days=1),
        valid_to=T - timedelta(hours=12),
        last_polled_at=T - timedelta(days=1),
    )
    await insert_rate(
        conn,
        source="nbu",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=5.0,
        valid_from=T + timedelta(days=1),
        last_polled_at=T + timedelta(days=1),
    )

    # Call twice — must return the same result both times
    row1 = await rate_repo.find_closest_rate(conn, "PLN", "UAH", T, ["nbu"])
    row2 = await rate_repo.find_closest_rate(conn, "PLN", "UAH", T, ["nbu"])

    assert row1 is not None
    assert row2 is not None
    assert row1.rate_mid == row2.rate_mid
