"""Integration test fixtures for grosh-api.

Each test session gets a fresh database, migrated from scratch, dropped on
teardown. Each test gets its own asyncpg connection wrapped in a rolled-back
transaction, so tests are fully isolated.
"""

import os
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any

import asyncpg
import pytest_asyncio
from dotenv import load_dotenv
from httpx import ASGITransport, AsyncClient

_ENV_FILE = Path(__file__).resolve().parents[4] / "infra" / ".env"
if _ENV_FILE.exists():
    load_dotenv(_ENV_FILE, override=False)

# test_db.py expects DATABASE_URL (no suffix). Use the admin role for test DB
# lifecycle (CREATE/DROP DATABASE) if available; fall back to the API role.
if "DATABASE_URL" not in os.environ:
    os.environ["DATABASE_URL"] = os.environ.get(
        "DATABASE_URL_ADMIN",
        os.environ.get("DATABASE_URL_API", ""),
    )

from grosh_shared.test_db import create_test_db, drop_test_db  # noqa: E402

from grosh_api.main import app  # noqa: E402


class _SingleConnPool:
    """Wraps a single asyncpg connection to look like a pool for tests.

    The app's ``get_db_conn`` dependency calls ``pool.acquire()`` as an async
    context manager. During tests we want every request to reuse the same
    open transaction so that the rollback at the end of each test wipes all
    data written by that test. This shim satisfies the pool interface while
    always returning the one test connection.
    """

    def __init__(self, conn: asyncpg.Connection) -> None:
        self._conn = conn

    def acquire(self) -> "_SingleConnPool":
        return self

    async def __aenter__(self) -> asyncpg.Connection:
        return self._conn

    async def __aexit__(self, *_: Any) -> None:
        pass


@pytest_asyncio.fixture(loop_scope="session", scope="session")
async def db_pool() -> AsyncGenerator[asyncpg.Pool, None]:
    pool, db_name = await create_test_db("api")
    yield pool
    await pool.close()
    await drop_test_db(db_name)


@pytest_asyncio.fixture(loop_scope="session")
async def conn(db_pool: asyncpg.Pool) -> AsyncGenerator[asyncpg.Connection, None]:
    async with db_pool.acquire() as connection:
        tx = connection.transaction()
        await tx.start()
        yield connection
        await tx.rollback()


@pytest_asyncio.fixture(loop_scope="session")
async def client(conn: asyncpg.Connection) -> AsyncGenerator[AsyncClient, None]:
    app.state.pool = _SingleConnPool(conn)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="https://test"
    ) as c:
        yield c


@pytest_asyncio.fixture(loop_scope="session")
async def admin_token(client: AsyncClient) -> str:
    resp = await client.post(
        "/auth/login",
        json={
            "email": os.environ["ADMIN_EMAIL"],
            "password": os.environ["ADMIN_PASSWORD"],
        },
    )
    assert resp.status_code == 200, resp.text
    return resp.json()["access_token"]
