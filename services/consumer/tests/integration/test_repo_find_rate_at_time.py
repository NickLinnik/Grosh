"""Integration tests for CurrencyRateRepo.find_rate_at_time.

Section 4.1 — 9 test functions.
"""

from datetime import UTC, timedelta
from decimal import Decimal

import asyncpg

from grosh_consumer.repositories.currency_rate_repo import CurrencyRateRepo
from tests.helpers import T
from tests.integration.helpers import insert_rate, insert_source_config

# ---------------------------------------------------------------------------
# 4.1.1  Returns matching row by source + pair + time
# ---------------------------------------------------------------------------


async def test_returns_matching_row(
    conn: asyncpg.Connection, rate_repo: CurrencyRateRepo
) -> None:
    await insert_source_config(conn, source="nbu")
    await insert_rate(
        conn,
        source="nbu",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=4.0,
        valid_from=T - timedelta(hours=1),
    )

    row = await rate_repo.find_rate_at_time(conn, "nbu", "PLN", "UAH", T)

    assert row is not None
    assert row.source == "nbu"
    assert row.rate_mid == Decimal("4.0")


# ---------------------------------------------------------------------------
# 4.1.2  Picks latest valid_from when multiple rows cover the time
# ---------------------------------------------------------------------------


async def test_picks_latest_valid_from_when_multiple_cover_time(
    conn: asyncpg.Connection, rate_repo: CurrencyRateRepo
) -> None:
    await insert_source_config(conn, source="nbu")
    await insert_rate(
        conn,
        source="nbu",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=3.0,
        valid_from=T - timedelta(hours=3),
    )
    await insert_rate(
        conn,
        source="nbu",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=4.0,
        valid_from=T - timedelta(hours=1),
    )

    row = await rate_repo.find_rate_at_time(conn, "nbu", "PLN", "UAH", T)

    assert row is not None
    assert row.rate_mid == Decimal("4.0")


# ---------------------------------------------------------------------------
# 4.1.3  NULL valid_to is open-ended (covers any future time)
# ---------------------------------------------------------------------------


async def test_null_valid_to_is_open_ended(
    conn: asyncpg.Connection, rate_repo: CurrencyRateRepo
) -> None:
    await insert_source_config(conn, source="nbu")
    await insert_rate(
        conn,
        source="nbu",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=4.0,
        valid_from=T - timedelta(hours=1),
        valid_to=None,
    )

    row = await rate_repo.find_rate_at_time(
        conn, "nbu", "PLN", "UAH", T + timedelta(days=30)
    )

    assert row is not None
    assert row.rate_mid == Decimal("4.0")


# ---------------------------------------------------------------------------
# 4.1.4  Finite valid_to excludes time after the window
# ---------------------------------------------------------------------------


async def test_finite_valid_to_excludes_time_after(
    conn: asyncpg.Connection, rate_repo: CurrencyRateRepo
) -> None:
    await insert_source_config(conn, source="nbu")
    await insert_rate(
        conn,
        source="nbu",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=4.0,
        valid_from=T - timedelta(hours=2),
        valid_to=T - timedelta(minutes=30),
    )

    row = await rate_repo.find_rate_at_time(conn, "nbu", "PLN", "UAH", T)

    assert row is None


# ---------------------------------------------------------------------------
# 4.1.5  Returns None for nonexistent pair
# ---------------------------------------------------------------------------


async def test_returns_none_for_nonexistent_pair(
    conn: asyncpg.Connection, rate_repo: CurrencyRateRepo
) -> None:
    await insert_source_config(conn, source="nbu")

    row = await rate_repo.find_rate_at_time(conn, "nbu", "PLN", "UAH", T)

    assert row is None


# ---------------------------------------------------------------------------
# 4.1.6  Returns rate_buy and rate_sell
# ---------------------------------------------------------------------------


async def test_returns_rate_buy_and_sell(
    conn: asyncpg.Connection, rate_repo: CurrencyRateRepo
) -> None:
    await insert_source_config(conn, source="monobank")
    await insert_rate(
        conn,
        source="monobank",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=4.0,
        rate_buy=3.9,
        rate_sell=4.1,
        valid_from=T - timedelta(hours=1),
    )

    row = await rate_repo.find_rate_at_time(conn, "monobank", "PLN", "UAH", T)

    assert row is not None
    assert row.rate_buy == Decimal("3.9")
    assert row.rate_sell == Decimal("4.1")


# ---------------------------------------------------------------------------
# 4.1.7  NULL rate_buy and rate_sell returned as None
# ---------------------------------------------------------------------------


async def test_null_rate_buy_and_sell_returned_as_none(
    conn: asyncpg.Connection, rate_repo: CurrencyRateRepo
) -> None:
    await insert_source_config(conn, source="nbu")
    await insert_rate(
        conn,
        source="nbu",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=4.0,
        rate_buy=None,
        rate_sell=None,
        valid_from=T - timedelta(hours=1),
    )

    row = await rate_repo.find_rate_at_time(conn, "nbu", "PLN", "UAH", T)

    assert row is not None
    assert row.rate_buy is None
    assert row.rate_sell is None


# ---------------------------------------------------------------------------
# 4.1.8  Returns last_polled_at
# ---------------------------------------------------------------------------


async def test_returns_last_polled_at(
    conn: asyncpg.Connection, rate_repo: CurrencyRateRepo
) -> None:
    polled = T - timedelta(seconds=60)
    await insert_source_config(conn, source="nbu")
    await insert_rate(
        conn,
        source="nbu",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=4.0,
        valid_from=T - timedelta(hours=1),
        last_polled_at=polled,
    )

    row = await rate_repo.find_rate_at_time(conn, "nbu", "PLN", "UAH", T)

    assert row is not None
    assert row.last_polled_at is not None
    # Compare as UTC-aware datetimes; asyncpg returns tz-aware values
    assert row.last_polled_at.replace(tzinfo=UTC) == polled


# ---------------------------------------------------------------------------
# 4.1.9  Filters by source correctly — same pair in different sources
# ---------------------------------------------------------------------------


async def test_filters_by_source(
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
        valid_from=T - timedelta(hours=1),
    )
    await insert_rate(
        conn,
        source="nbu",
        currency_from="PLN",
        currency_to="UAH",
        rate_mid=4.5,
        valid_from=T - timedelta(hours=1),
    )

    row = await rate_repo.find_rate_at_time(conn, "nbu", "PLN", "UAH", T)

    assert row is not None
    assert row.source == "nbu"
    assert row.rate_mid == Decimal("4.5")
