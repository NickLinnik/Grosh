"""Integration test fixtures for grosh-normalizer.

Each test session gets a fresh database, migrated from scratch, dropped on
teardown. Each test gets its own asyncpg connection wrapped in a rolled-back
transaction, so tests are fully isolated.
"""

import os
from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from uuid import UUID, uuid4

import asyncpg
import pytest_asyncio
from dotenv import load_dotenv

_ENV_FILE = Path(__file__).resolve().parents[4] / "infra" / ".env"
if _ENV_FILE.exists():
    load_dotenv(_ENV_FILE, override=False)

_db_admin_url = os.environ.pop("DATABASE_URL_ADMIN", None)
if "DATABASE_URL" not in os.environ and _db_admin_url is not None:
    os.environ["DATABASE_URL"] = _db_admin_url

from grosh_shared.test_db import create_test_db, drop_test_db  # noqa: E402

# ---------------------------------------------------------------------------
# Test pool helpers
# ---------------------------------------------------------------------------


class SingleConnectionPool:
    """Test helper: duck-typed pool that always yields a single shared connection."""

    def __init__(self, conn: asyncpg.Connection) -> None:
        self._conn = conn

    @asynccontextmanager
    async def acquire(self) -> AsyncIterator[asyncpg.Connection]:
        yield self._conn


@pytest_asyncio.fixture(loop_scope="session", scope="session")
async def db_pool() -> AsyncGenerator[asyncpg.Pool, None]:
    pool, db_name = await create_test_db("normalizer")
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


# ---------------------------------------------------------------------------
# DB seed helpers
# ---------------------------------------------------------------------------


async def insert_user(
    conn: asyncpg.Connection,
    user_id: UUID | None = None,
) -> UUID:
    uid = user_id or uuid4()
    await conn.execute(
        """
        INSERT INTO users (id, email, password_hash, display_name, role)
        VALUES ($1, $2, 'hash', 'Test User', 'member')
        """,
        uid,
        f"test-{uid}@example.com",
    )
    return uid
