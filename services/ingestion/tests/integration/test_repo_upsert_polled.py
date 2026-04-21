"""Integration tests for CurrencyRateRepo.upsert_polled."""

import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import asyncpg
from grosh_shared.models import RateSource

from grosh_ingestion.repositories.currency_rate_repo import CurrencyRateRepo

from .conftest import insert_rate_row, select_rows

T = datetime(2025, 6, 1, 12, 0, tzinfo=UTC)
T_minus_1m = T - timedelta(minutes=1)
T_minus_1h = T - timedelta(hours=1)
T_minus_1d = T - timedelta(days=1)
T_plus_1d = T + timedelta(days=1)

_SOURCE = RateSource.monobank
_FROM = "USD"
_TO = "UAH"
_CADENCE = 300


async def test_empty_sequence_inserts_open_row(
    conn: asyncpg.Connection, rate_repo: CurrencyRateRepo
):
    await rate_repo.upsert_polled(
        conn=conn,
        source=_SOURCE,
        currency_from=_FROM,
        currency_to=_TO,
        rate_buy=Decimal("39"),
        rate_sell=Decimal("41"),
        rate_mid=Decimal("40"),
        update_cadence_seconds=_CADENCE,
        at_time=T,
    )

    rows = await select_rows(conn, source=_SOURCE, currency_from=_FROM, currency_to=_TO)
    assert len(rows) == 1
    row = rows[0]
    assert row["valid_from"] == T
    assert row["valid_to"] is None
    assert row["last_polled_at"] == T
    assert row["update_cadence_seconds"] == _CADENCE


async def test_inserted_row_has_correct_rates(
    conn: asyncpg.Connection, rate_repo: CurrencyRateRepo
):
    await rate_repo.upsert_polled(
        conn=conn,
        source=_SOURCE,
        currency_from=_FROM,
        currency_to=_TO,
        rate_buy=Decimal("39"),
        rate_sell=Decimal("41"),
        rate_mid=Decimal("40"),
        update_cadence_seconds=_CADENCE,
        at_time=T,
    )

    rows = await select_rows(conn, source=_SOURCE, currency_from=_FROM, currency_to=_TO)
    assert rows[0]["rate_buy"] == Decimal("39")
    assert rows[0]["rate_sell"] == Decimal("41")
    assert rows[0]["rate_mid"] == Decimal("40")


async def test_rates_match_bumps_last_polled_at_only(
    conn: asyncpg.Connection, rate_repo: CurrencyRateRepo
):
    await insert_rate_row(
        conn,
        source=_SOURCE,
        currency_from=_FROM,
        currency_to=_TO,
        rate_buy=Decimal("39"),
        rate_sell=Decimal("41"),
        rate_mid=Decimal("40"),
        valid_from=T_minus_1m,
        last_polled_at=T_minus_1m,
        update_cadence_seconds=_CADENCE,
    )

    await rate_repo.upsert_polled(
        conn=conn,
        source=_SOURCE,
        currency_from=_FROM,
        currency_to=_TO,
        rate_buy=Decimal("39"),
        rate_sell=Decimal("41"),
        rate_mid=Decimal("40"),
        update_cadence_seconds=_CADENCE,
        at_time=T,
    )

    rows = await select_rows(conn, source=_SOURCE, currency_from=_FROM, currency_to=_TO)
    assert len(rows) == 1
    assert rows[0]["last_polled_at"] == T
    assert rows[0]["valid_from"] == T_minus_1m
    assert rows[0]["valid_to"] is None


async def test_rates_match_refreshes_update_cadence(
    conn: asyncpg.Connection, rate_repo: CurrencyRateRepo
):
    await insert_rate_row(
        conn,
        source=_SOURCE,
        currency_from=_FROM,
        currency_to=_TO,
        rate_mid=Decimal("40"),
        valid_from=T_minus_1m,
        last_polled_at=T_minus_1m,
        update_cadence_seconds=300,
    )

    await rate_repo.upsert_polled(
        conn=conn,
        source=_SOURCE,
        currency_from=_FROM,
        currency_to=_TO,
        rate_buy=None,
        rate_sell=None,
        rate_mid=Decimal("40"),
        update_cadence_seconds=600,
        at_time=T,
    )

    rows = await select_rows(conn, source=_SOURCE, currency_from=_FROM, currency_to=_TO)
    assert rows[0]["update_cadence_seconds"] == 600


async def test_rates_differ_closes_current_opens_new(
    conn: asyncpg.Connection, rate_repo: CurrencyRateRepo
):
    await insert_rate_row(
        conn,
        source=_SOURCE,
        currency_from=_FROM,
        currency_to=_TO,
        rate_mid=Decimal("40"),
        valid_from=T_minus_1m,
        last_polled_at=T_minus_1m,
        update_cadence_seconds=_CADENCE,
    )

    await rate_repo.upsert_polled(
        conn=conn,
        source=_SOURCE,
        currency_from=_FROM,
        currency_to=_TO,
        rate_buy=None,
        rate_sell=None,
        rate_mid=Decimal("41"),
        update_cadence_seconds=_CADENCE,
        at_time=T,
    )

    rows = await select_rows(conn, source=_SOURCE, currency_from=_FROM, currency_to=_TO)
    assert len(rows) == 2
    old_row = next(r for r in rows if r["valid_from"] == T_minus_1m)
    new_row = next(r for r in rows if r["valid_from"] == T)
    assert old_row["valid_to"] == T
    assert new_row["valid_to"] is None
    assert new_row["rate_mid"] == Decimal("41")


async def test_only_rate_buy_changes_triggers_close_open(
    conn: asyncpg.Connection, rate_repo: CurrencyRateRepo
):
    await insert_rate_row(
        conn,
        source=_SOURCE,
        currency_from=_FROM,
        currency_to=_TO,
        rate_buy=Decimal("39"),
        rate_sell=Decimal("41"),
        rate_mid=Decimal("40"),
        valid_from=T_minus_1m,
        last_polled_at=T_minus_1m,
        update_cadence_seconds=_CADENCE,
    )

    await rate_repo.upsert_polled(
        conn=conn,
        source=_SOURCE,
        currency_from=_FROM,
        currency_to=_TO,
        rate_buy=Decimal("39.5"),
        rate_sell=Decimal("41"),
        rate_mid=Decimal("40"),
        update_cadence_seconds=_CADENCE,
        at_time=T,
    )

    rows = await select_rows(conn, source=_SOURCE, currency_from=_FROM, currency_to=_TO)
    assert len(rows) == 2


async def test_null_vs_set_rate_buy_is_a_change(
    conn: asyncpg.Connection, rate_repo: CurrencyRateRepo
):
    await insert_rate_row(
        conn,
        source=_SOURCE,
        currency_from=_FROM,
        currency_to=_TO,
        rate_buy=Decimal("39"),
        rate_sell=Decimal("41"),
        rate_mid=Decimal("40"),
        valid_from=T_minus_1m,
        last_polled_at=T_minus_1m,
        update_cadence_seconds=_CADENCE,
    )

    await rate_repo.upsert_polled(
        conn=conn,
        source=_SOURCE,
        currency_from=_FROM,
        currency_to=_TO,
        rate_buy=None,
        rate_sell=Decimal("41"),
        rate_mid=Decimal("40"),
        update_cadence_seconds=_CADENCE,
        at_time=T,
    )

    rows = await select_rows(conn, source=_SOURCE, currency_from=_FROM, currency_to=_TO)
    assert len(rows) == 2


async def test_historical_rows_ignored_by_upsert_polled(
    conn: asyncpg.Connection, rate_repo: CurrencyRateRepo
):
    # Seed a historical row (no last_polled_at)
    await insert_rate_row(
        conn,
        source=_SOURCE,
        currency_from=_FROM,
        currency_to=_TO,
        rate_mid=Decimal("40"),
        valid_from=T_minus_1d,
    )

    await rate_repo.upsert_polled(
        conn=conn,
        source=_SOURCE,
        currency_from=_FROM,
        currency_to=_TO,
        rate_buy=None,
        rate_sell=None,
        rate_mid=Decimal("40"),
        update_cadence_seconds=_CADENCE,
        at_time=T,
    )

    polled = await select_rows(
        conn, source=_SOURCE, currency_from=_FROM, currency_to=_TO, kind="polled"
    )
    historical = await select_rows(
        conn, source=_SOURCE, currency_from=_FROM, currency_to=_TO, kind="historical"
    )
    assert len(polled) == 1
    assert len(historical) == 1
    assert historical[0]["valid_from"] == T_minus_1d


async def test_polled_and_historical_rows_coexist(
    conn: asyncpg.Connection, rate_repo: CurrencyRateRepo
):
    # Seed historical open row
    await insert_rate_row(
        conn,
        source=_SOURCE,
        currency_from=_FROM,
        currency_to=_TO,
        rate_mid=Decimal("40"),
        valid_from=T_minus_1d,
    )
    # Seed polled open row
    await insert_rate_row(
        conn,
        source=_SOURCE,
        currency_from=_FROM,
        currency_to=_TO,
        rate_mid=Decimal("40"),
        valid_from=T_minus_1h,
        last_polled_at=T_minus_1h,
        update_cadence_seconds=_CADENCE,
    )

    await rate_repo.upsert_polled(
        conn=conn,
        source=_SOURCE,
        currency_from=_FROM,
        currency_to=_TO,
        rate_buy=None,
        rate_sell=None,
        rate_mid=Decimal("40"),
        update_cadence_seconds=_CADENCE,
        at_time=T,
    )

    polled = await select_rows(
        conn, source=_SOURCE, currency_from=_FROM, currency_to=_TO, kind="polled"
    )
    historical = await select_rows(
        conn, source=_SOURCE, currency_from=_FROM, currency_to=_TO, kind="historical"
    )
    assert len(polled) == 1
    assert polled[0]["last_polled_at"] == T
    assert len(historical) == 1
    assert historical[0]["valid_from"] == T_minus_1d


async def test_concurrent_pollers_serialize(db_pool: asyncpg.Pool):
    """Two concurrent upsert_polled calls for the same pair with unchanged rates.

    FOR UPDATE serializes concurrent access to the same open polled row.
    When both callers observe the same rates, only bumps to last_polled_at
    occur — no spurious close+open.
    """
    repo = CurrencyRateRepo()

    # Seed an open polled row with rate_mid=40
    async with db_pool.acquire() as seed_conn:
        await seed_conn.execute(
            """
            INSERT INTO currency_rates (
                source, currency_from, currency_to,
                rate_mid, valid_from, last_polled_at, update_cadence_seconds
            )
            VALUES ($1, $2, $3, $4, $5, $5, $6)
            """,
            _SOURCE,
            _FROM,
            _TO,
            Decimal("40"),
            T_minus_1m,
            _CADENCE,
        )

    async def do_upsert(at_time: datetime) -> None:
        async with db_pool.acquire() as c:
            await repo.upsert_polled(
                conn=c,
                source=_SOURCE,
                currency_from=_FROM,
                currency_to=_TO,
                rate_buy=None,
                rate_sell=None,
                rate_mid=Decimal("40"),
                update_cadence_seconds=_CADENCE,
                at_time=at_time,
            )

    # Both callers observe the same rates — concurrent bumps
    await asyncio.gather(
        do_upsert(T),
        do_upsert(T + timedelta(seconds=1)),
    )

    async with db_pool.acquire() as check_conn:
        rows = await check_conn.fetch(
            """
            SELECT * FROM currency_rates
            WHERE source = $1 AND currency_from = $2 AND currency_to = $3
            ORDER BY valid_from ASC
            """,
            _SOURCE,
            _FROM,
            _TO,
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

    # Both callers bumped last_polled_at on the same row — no new rows created.
    assert len(rows) == 1
    assert rows[0]["valid_to"] is None
    # last_polled_at is the later of the two bumps
    assert rows[0]["last_polled_at"] in (T, T + timedelta(seconds=1))
