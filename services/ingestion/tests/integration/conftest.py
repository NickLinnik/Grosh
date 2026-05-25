"""Integration test fixtures for grosh-ingestion.

Each test session gets a fresh database, migrated from scratch, dropped on
teardown. Each test gets its own asyncpg connection wrapped in a rolled-back
transaction, so tests are fully isolated.
"""

import os
from collections.abc import AsyncGenerator
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import asyncpg
import pytest
import pytest_asyncio
from dotenv import load_dotenv

_ENV_FILE = Path(__file__).resolve().parents[4] / "infra" / ".env"
if _ENV_FILE.exists():
    load_dotenv(_ENV_FILE, override=False)

# test_db.py runs alembic in a subprocess which inherits os.environ.
# alembic's env.py uses DATABASE_URL_ADMIN when present, but that URL has
# @postgres: which doesn't resolve from the host machine. Remove it so alembic
# falls back to DATABASE_URL (set by test_db to @localhost: for the subprocess).
# If DATABASE_URL is not yet set, promote DATABASE_URL_ADMIN as the base so
# test_db.py can perform its @postgres: → @localhost: substitution.
_db_admin_url = os.environ.pop("DATABASE_URL_ADMIN", None)
if "DATABASE_URL" not in os.environ and _db_admin_url is not None:
    os.environ["DATABASE_URL"] = _db_admin_url

from grosh_shared.db.testing import create_test_db, drop_test_db  # noqa: E402

from grosh_ingestion.repositories.currency_rate_repo import (  # noqa: E402
    CurrencyRateRepo,
)
from grosh_ingestion.services.currency_rate_service import (  # noqa: E402
    CurrencyRateService,
)


@pytest_asyncio.fixture(loop_scope="session", scope="session")
async def db_pool() -> AsyncGenerator[asyncpg.Pool, None]:
    pool, db_name = await create_test_db("ingestion")
    yield pool
    await pool.close()
    await drop_test_db(db_name)


@pytest_asyncio.fixture(loop_scope="session", scope="session")
async def ingestion_pool(db_pool: asyncpg.Pool) -> AsyncGenerator[asyncpg.Pool, None]:
    """Connect as the `grosh_ingestion` role to exercise the production RLS regime.

    Tests using this pool see the same RLS enforcement that runs in prod,
    unlike the admin-table-owner `db_pool` which bypasses RLS by default.
    The pool points at the same throwaway test DB as `db_pool` (which
    created and migrated it).
    """
    async with db_pool.acquire() as admin_conn:
        db_name = await admin_conn.fetchval("SELECT current_database()")

    # Reuse the same host:port the admin pool uses by parsing the DSN that
    # test_db built. asyncpg doesn't expose port directly so we read it
    # from the env DSN.
    base_dsn = os.environ["DATABASE_URL"].replace("@postgres:", "@localhost:")
    # Replace user:password and database name.
    from urllib.parse import urlparse, urlunparse

    parsed = urlparse(base_dsn.replace("+asyncpg", ""))
    password = os.environ.get(
        "GROSH_INGESTION_DB_PASSWORD",
        "changeme_ingestion",
    )
    new_netloc = f"grosh_ingestion:{password}@{parsed.hostname}:{parsed.port}"
    ingestion_dsn = urlunparse(parsed._replace(netloc=new_netloc, path=f"/{db_name}"))

    pool = await asyncpg.create_pool(ingestion_dsn)
    try:
        yield pool
    finally:
        await pool.close()


@pytest_asyncio.fixture(loop_scope="session", scope="function")
async def conn(db_pool: asyncpg.Pool) -> AsyncGenerator[asyncpg.Connection, None]:
    async with db_pool.acquire() as connection:
        tx = connection.transaction()
        await tx.start()
        yield connection
        await tx.rollback()


@pytest.fixture(scope="function")
def rate_repo() -> CurrencyRateRepo:
    return CurrencyRateRepo()


@pytest.fixture(scope="function")
def rate_service(rate_repo: CurrencyRateRepo) -> CurrencyRateService:
    return CurrencyRateService(rate_repo)


async def insert_rate_row(
    conn: asyncpg.Connection,
    *,
    source: str,
    currency_from: str,
    currency_to: str,
    rate_mid: Decimal | int | str,
    rate_buy: Decimal | int | str | None = None,
    rate_sell: Decimal | int | str | None = None,
    valid_from: datetime,
    valid_to: datetime | None = None,
    last_polled_at: datetime | None = None,
    update_cadence_seconds: int | None = None,
) -> int:
    """Insert a currency_rates row directly and return its id."""
    row = await conn.fetchrow(
        """
        INSERT INTO currency_rates (
            source, currency_from, currency_to,
            rate_buy, rate_sell, rate_mid,
            valid_from, valid_to,
            last_polled_at, update_cadence_seconds
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        RETURNING id
        """,
        source,
        currency_from,
        currency_to,
        Decimal(str(rate_buy)) if rate_buy is not None else None,
        Decimal(str(rate_sell)) if rate_sell is not None else None,
        Decimal(str(rate_mid)),
        valid_from,
        valid_to,
        last_polled_at,
        update_cadence_seconds,
    )
    return row["id"]


async def select_rows(
    conn: asyncpg.Connection,
    *,
    source: str,
    currency_from: str,
    currency_to: str,
    kind: str | None = None,
) -> list[asyncpg.Record]:
    """Select all currency_rates rows for a pair, ordered by valid_from ASC.

    kind: 'polled' filters to rows with last_polled_at IS NOT NULL,
          'historical' filters to rows with last_polled_at IS NULL,
          None returns all rows.
    """
    if kind == "polled":
        kind_filter = "AND last_polled_at IS NOT NULL"
    elif kind == "historical":
        kind_filter = "AND last_polled_at IS NULL"
    else:
        kind_filter = ""

    return await conn.fetch(
        f"""
        SELECT *
        FROM currency_rates
        WHERE source = $1
          AND currency_from = $2
          AND currency_to = $3
          {kind_filter}
        ORDER BY valid_from ASC
        """,
        source,
        currency_from,
        currency_to,
    )
