"""Integration test fixtures for grosh-consumer.

Each test session gets a fresh database, migrated from scratch, dropped on
teardown. Each test gets its own asyncpg connection wrapped in a rolled-back
transaction, so tests are fully isolated.
"""

from collections.abc import AsyncGenerator
from pathlib import Path

import asyncpg
import pytest
import pytest_asyncio
from dotenv import load_dotenv

_ENV_FILE = Path(__file__).resolve().parents[4] / "infra" / ".env"
if _ENV_FILE.exists():
    load_dotenv(_ENV_FILE, override=False)

from grosh_shared.test_db import create_test_db, drop_test_db  # noqa: E402

from grosh_consumer.repositories.currency_rate_repo import (  # noqa: E402
    CurrencyRateRepo,
)
from grosh_consumer.services.currency_conversion_service import (  # noqa: E402
    CurrencyConversionService,
)


@pytest_asyncio.fixture(loop_scope="session", scope="session")
async def db_pool() -> AsyncGenerator[asyncpg.Pool, None]:
    pool, db_name = await create_test_db("consumer")
    yield pool
    await pool.close()
    await drop_test_db(db_name)


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
def service(rate_repo: CurrencyRateRepo) -> CurrencyConversionService:
    return CurrencyConversionService(rate_repo)
