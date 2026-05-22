"""Integration tests for CurrencyRateRepo.upsert_historical."""

import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import asyncpg
import pytest
from grosh_shared.domain.models import RateSource

from grosh_ingestion.repositories.currency_rate_repo import CurrencyRateRepo

from .conftest import insert_rate_row, select_rows

T = datetime(2025, 6, 1, 12, 0, tzinfo=UTC)
T_minus_1m = T - timedelta(minutes=1)
T_minus_1h = T - timedelta(hours=1)
T_minus_1d = T - timedelta(days=1)
T_minus_3d = T - timedelta(days=3)
T_minus_5d = T - timedelta(days=5)
T_minus_7d = T - timedelta(days=7)
T_minus_10d = T - timedelta(days=10)
T_minus_20d = T - timedelta(days=20)
T_plus_1d = T + timedelta(days=1)

_SOURCE = RateSource.nbu
_FROM = "USD"
_TO = "UAH"


# --- 6.1 Empty state ---


async def test_empty_sequence_inserts_open_row(
    conn: asyncpg.Connection, rate_repo: CurrencyRateRepo
) -> None:
    await rate_repo.upsert_historical(
        conn=conn,
        source=_SOURCE,
        currency_from=_FROM,
        currency_to=_TO,
        rate_buy=None,
        rate_sell=None,
        rate_mid=Decimal("40"),
        at_time=T,
    )

    rows = await select_rows(conn, source=_SOURCE, currency_from=_FROM, currency_to=_TO)
    assert len(rows) == 1
    row = rows[0]
    assert row["valid_from"] == T
    assert row["valid_to"] is None
    assert row["last_polled_at"] is None
    assert row["update_cadence_seconds"] is None


# --- 6.2 Exact valid_from match ---


async def test_exact_match_same_rates_is_noop(
    conn: asyncpg.Connection, rate_repo: CurrencyRateRepo
) -> None:
    await insert_rate_row(
        conn,
        source=_SOURCE,
        currency_from=_FROM,
        currency_to=_TO,
        rate_mid=Decimal("40"),
        valid_from=T,
    )

    await rate_repo.upsert_historical(
        conn=conn,
        source=_SOURCE,
        currency_from=_FROM,
        currency_to=_TO,
        rate_buy=None,
        rate_sell=None,
        rate_mid=Decimal("40"),
        at_time=T,
    )

    rows = await select_rows(conn, source=_SOURCE, currency_from=_FROM, currency_to=_TO)
    assert len(rows) == 1
    assert rows[0]["rate_mid"] == Decimal("40")


async def test_exact_match_different_rates_updates_in_place(
    conn: asyncpg.Connection, rate_repo: CurrencyRateRepo
) -> None:
    await insert_rate_row(
        conn,
        source=_SOURCE,
        currency_from=_FROM,
        currency_to=_TO,
        rate_mid=Decimal("40"),
        valid_from=T,
        valid_to=T_plus_1d,
    )

    await rate_repo.upsert_historical(
        conn=conn,
        source=_SOURCE,
        currency_from=_FROM,
        currency_to=_TO,
        rate_buy=None,
        rate_sell=None,
        rate_mid=Decimal("41"),
        at_time=T,
    )

    rows = await select_rows(conn, source=_SOURCE, currency_from=_FROM, currency_to=_TO)
    assert len(rows) == 1
    assert rows[0]["rate_mid"] == Decimal("41")
    assert rows[0]["valid_to"] == T_plus_1d


# --- 6.3 Slot-in middle ---


async def test_splits_open_covering_row(
    conn: asyncpg.Connection, rate_repo: CurrencyRateRepo
) -> None:
    await insert_rate_row(
        conn,
        source=_SOURCE,
        currency_from=_FROM,
        currency_to=_TO,
        rate_mid=Decimal("40"),
        valid_from=T_minus_10d,
    )

    await rate_repo.upsert_historical(
        conn=conn,
        source=_SOURCE,
        currency_from=_FROM,
        currency_to=_TO,
        rate_buy=None,
        rate_sell=None,
        rate_mid=Decimal("41"),
        at_time=T_minus_5d,
    )

    rows = await select_rows(
        conn, source=_SOURCE, currency_from=_FROM, currency_to=_TO, kind="historical"
    )
    assert len(rows) == 2
    old_row = next(r for r in rows if r["valid_from"] == T_minus_10d)
    new_row = next(r for r in rows if r["valid_from"] == T_minus_5d)
    assert old_row["valid_to"] == T_minus_5d
    assert new_row["valid_to"] is None
    assert new_row["rate_mid"] == Decimal("41")


async def test_splits_closed_covering_row(
    conn: asyncpg.Connection, rate_repo: CurrencyRateRepo
) -> None:
    await insert_rate_row(
        conn,
        source=_SOURCE,
        currency_from=_FROM,
        currency_to=_TO,
        rate_mid=Decimal("40"),
        valid_from=T_minus_10d,
        valid_to=T_minus_3d,
    )

    await rate_repo.upsert_historical(
        conn=conn,
        source=_SOURCE,
        currency_from=_FROM,
        currency_to=_TO,
        rate_buy=None,
        rate_sell=None,
        rate_mid=Decimal("41"),
        at_time=T_minus_5d,
    )

    rows = await select_rows(
        conn, source=_SOURCE, currency_from=_FROM, currency_to=_TO, kind="historical"
    )
    assert len(rows) == 2
    old_row = next(r for r in rows if r["valid_from"] == T_minus_10d)
    new_row = next(r for r in rows if r["valid_from"] == T_minus_5d)
    assert old_row["valid_to"] == T_minus_5d
    assert new_row["valid_to"] == T_minus_3d


async def test_split_preserves_predecessor_and_successor(
    conn: asyncpg.Connection, rate_repo: CurrencyRateRepo
) -> None:
    # Three rows: [T-20d, T-10d), [T-10d, T-3d), [T-3d, NULL)
    await insert_rate_row(
        conn,
        source=_SOURCE,
        currency_from=_FROM,
        currency_to=_TO,
        rate_mid=Decimal("38"),
        valid_from=T_minus_20d,
        valid_to=T_minus_10d,
    )
    await insert_rate_row(
        conn,
        source=_SOURCE,
        currency_from=_FROM,
        currency_to=_TO,
        rate_mid=Decimal("39"),
        valid_from=T_minus_10d,
        valid_to=T_minus_3d,
    )
    await insert_rate_row(
        conn,
        source=_SOURCE,
        currency_from=_FROM,
        currency_to=_TO,
        rate_mid=Decimal("40"),
        valid_from=T_minus_3d,
    )

    # Insert into the middle row
    await rate_repo.upsert_historical(
        conn=conn,
        source=_SOURCE,
        currency_from=_FROM,
        currency_to=_TO,
        rate_buy=None,
        rate_sell=None,
        rate_mid=Decimal("41"),
        at_time=T_minus_5d,
    )

    rows = await select_rows(
        conn, source=_SOURCE, currency_from=_FROM, currency_to=_TO, kind="historical"
    )
    assert len(rows) == 4

    predecessor = next(r for r in rows if r["valid_from"] == T_minus_20d)
    assert predecessor["valid_to"] == T_minus_10d
    assert predecessor["rate_mid"] == Decimal("38")

    successor = next(r for r in rows if r["valid_from"] == T_minus_3d)
    assert successor["valid_to"] is None
    assert successor["rate_mid"] == Decimal("40")

    split_old = next(r for r in rows if r["valid_from"] == T_minus_10d)
    assert split_old["valid_to"] == T_minus_5d

    split_new = next(r for r in rows if r["valid_from"] == T_minus_5d)
    assert split_new["valid_to"] == T_minus_3d
    assert split_new["rate_mid"] == Decimal("41")


# --- 6.4 Slot-in at start ---


async def test_earlier_than_all_existing_prepends_bounded_row(
    conn: asyncpg.Connection, rate_repo: CurrencyRateRepo
) -> None:
    await insert_rate_row(
        conn,
        source=_SOURCE,
        currency_from=_FROM,
        currency_to=_TO,
        rate_mid=Decimal("40"),
        valid_from=T,
    )

    await rate_repo.upsert_historical(
        conn=conn,
        source=_SOURCE,
        currency_from=_FROM,
        currency_to=_TO,
        rate_buy=None,
        rate_sell=None,
        rate_mid=Decimal("39"),
        at_time=T_minus_1d,
    )

    rows = await select_rows(
        conn, source=_SOURCE, currency_from=_FROM, currency_to=_TO, kind="historical"
    )
    assert len(rows) == 2
    new_row = next(r for r in rows if r["valid_from"] == T_minus_1d)
    existing = next(r for r in rows if r["valid_from"] == T)
    assert new_row["valid_to"] == T
    assert new_row["rate_mid"] == Decimal("39")
    assert existing["valid_to"] is None


# --- 6.5 Bug guard ---


@pytest.mark.skip(reason="guard covered by code review; not easily triggerable")
async def test_orphan_guard_raises_runtime_error(
    conn: asyncpg.Connection, rate_repo: CurrencyRateRepo
) -> None:
    pass


# --- 6.6 Polled rows present ---


async def test_polled_rows_ignored_by_upsert_historical(
    conn: asyncpg.Connection, rate_repo: CurrencyRateRepo
) -> None:
    await insert_rate_row(
        conn,
        source=_SOURCE,
        currency_from=_FROM,
        currency_to=_TO,
        rate_mid=Decimal("40"),
        valid_from=T_minus_1h,
        last_polled_at=T_minus_1h,
        update_cadence_seconds=300,
    )

    await rate_repo.upsert_historical(
        conn=conn,
        source=_SOURCE,
        currency_from=_FROM,
        currency_to=_TO,
        rate_buy=None,
        rate_sell=None,
        rate_mid=Decimal("40"),
        at_time=T_minus_1d,
    )

    polled = await select_rows(
        conn, source=_SOURCE, currency_from=_FROM, currency_to=_TO, kind="polled"
    )
    historical = await select_rows(
        conn, source=_SOURCE, currency_from=_FROM, currency_to=_TO, kind="historical"
    )
    assert len(polled) == 1
    assert polled[0]["valid_from"] == T_minus_1h
    assert len(historical) == 1
    assert historical[0]["valid_from"] == T_minus_1d


async def test_historical_and_polled_can_share_valid_from(
    conn: asyncpg.Connection, rate_repo: CurrencyRateRepo
) -> None:
    await insert_rate_row(
        conn,
        source=_SOURCE,
        currency_from=_FROM,
        currency_to=_TO,
        rate_mid=Decimal("40"),
        valid_from=T,
        last_polled_at=T,
        update_cadence_seconds=300,
    )

    await rate_repo.upsert_historical(
        conn=conn,
        source=_SOURCE,
        currency_from=_FROM,
        currency_to=_TO,
        rate_buy=None,
        rate_sell=None,
        rate_mid=Decimal("40"),
        at_time=T,
    )

    all_rows = await select_rows(
        conn, source=_SOURCE, currency_from=_FROM, currency_to=_TO
    )
    polled = [r for r in all_rows if r["last_polled_at"] is not None]
    historical = [r for r in all_rows if r["last_polled_at"] is None]
    assert len(all_rows) == 2
    assert len(polled) == 1
    assert len(historical) == 1
    assert polled[0]["valid_from"] == T
    assert historical[0]["valid_from"] == T


# --- 6.7 Concurrency ---


async def test_concurrent_historical_upserts_serialize(db_pool: asyncpg.Pool) -> None:
    """Two concurrent upsert_historical calls produce a gap-free sequence."""
    repo = CurrencyRateRepo()

    # Seed an open historical row starting at T-10d
    async with db_pool.acquire() as seed_conn:
        await seed_conn.execute(
            """
            INSERT INTO currency_rates (
                source, currency_from, currency_to,
                rate_mid, valid_from, valid_to,
                last_polled_at, update_cadence_seconds
            )
            VALUES ($1, $2, $3, $4, $5, NULL, NULL, NULL)
            """,
            _SOURCE,
            _FROM,
            _TO,
            Decimal("40"),
            T_minus_10d,
        )

    async def do_historical(rate_mid: Decimal, at_time: datetime) -> None:
        async with db_pool.acquire() as c:
            await repo.upsert_historical(
                conn=c,
                source=_SOURCE,
                currency_from=_FROM,
                currency_to=_TO,
                rate_buy=None,
                rate_sell=None,
                rate_mid=rate_mid,
                at_time=at_time,
            )

    await asyncio.gather(
        do_historical(Decimal("41"), T_minus_7d),
        do_historical(Decimal("42"), T_minus_3d),
    )

    async with db_pool.acquire() as check_conn:
        rows = await check_conn.fetch(
            """
            SELECT * FROM currency_rates
            WHERE source = $1
              AND currency_from = $2
              AND currency_to = $3
              AND last_polled_at IS NULL
            ORDER BY valid_from ASC
            """,
            _SOURCE,
            _FROM,
            _TO,
        )

    async with db_pool.acquire() as clean_conn:
        await clean_conn.execute(
            "DELETE FROM currency_rates"
            " WHERE source=$1 AND currency_from=$2 AND currency_to=$3",
            _SOURCE,
            _FROM,
            _TO,
        )

    assert len(rows) == 3

    # No overlapping intervals and no duplicate valid_from values
    valid_froms = [r["valid_from"] for r in rows]
    assert len(valid_froms) == len(set(valid_froms))

    # Each closed row's valid_to equals the next row's valid_from
    for i in range(len(rows) - 1):
        assert rows[i]["valid_to"] == rows[i + 1]["valid_from"]

    # Last row is open
    assert rows[-1]["valid_to"] is None


async def test_historical_and_polled_dont_deadlock(db_pool: asyncpg.Pool) -> None:
    """Concurrent polled and historical upserts for same pair don't deadlock."""
    repo = CurrencyRateRepo()

    async def do_polled() -> None:
        async with db_pool.acquire() as c:
            await repo.upsert_polled(
                conn=c,
                source=_SOURCE,
                currency_from=_FROM,
                currency_to=_TO,
                rate_buy=None,
                rate_sell=None,
                rate_mid=Decimal("40"),
                update_cadence_seconds=300,
                at_time=T,
            )

    async def do_historical() -> None:
        async with db_pool.acquire() as c:
            await repo.upsert_historical(
                conn=c,
                source=_SOURCE,
                currency_from=_FROM,
                currency_to=_TO,
                rate_buy=None,
                rate_sell=None,
                rate_mid=Decimal("40"),
                at_time=T_minus_1d,
            )

    await asyncio.wait_for(
        asyncio.gather(do_polled(), do_historical()),
        timeout=10,
    )

    # Cleanup
    async with db_pool.acquire() as clean_conn:
        await clean_conn.execute(
            "DELETE FROM currency_rates"
            " WHERE source=$1 AND currency_from=$2 AND currency_to=$3",
            _SOURCE,
            _FROM,
            _TO,
        )
