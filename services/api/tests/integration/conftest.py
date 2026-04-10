import os
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any

import asyncpg
import pytest_asyncio
from dotenv import load_dotenv
from httpx import ASGITransport, AsyncClient

# Auto-load infra/.env so tests run from any context (PyCharm, terminal, CI)
# without requiring the shell to export variables first. Real environment
# values always win — override=False keeps CI/shell exports authoritative.
_ENV_FILE = Path(__file__).resolve().parents[4] / "infra" / ".env"
if _ENV_FILE.exists():
    load_dotenv(_ENV_FILE, override=False)

from grosh_api.main import app  # noqa: E402 — must come after load_dotenv
from grosh_api.utils.db_url import for_asyncpg  # noqa: E402


class _SingleConnPool:
    """Wraps a single asyncpg connection to look like a pool for tests.

    > **What is this pattern?**
    > The app's `get_db_conn` dependency calls `pool.acquire()` as an async
    > context manager. During tests we want every request to reuse the same
    > open transaction so that the rollback at the end of each test wipes all
    > data written by that test. This shim satisfies the pool interface while
    > always returning the one test connection.
    """

    def __init__(self, conn: asyncpg.Connection) -> None:
        self._conn = conn

    def acquire(self) -> "_SingleConnPool":
        return self

    async def __aenter__(self) -> asyncpg.Connection:
        return self._conn

    async def __aexit__(self, *_: Any) -> None:
        pass  # do not release — the test owns the connection lifecycle


@pytest_asyncio.fixture(loop_scope="session", scope="session")
async def db_pool() -> AsyncGenerator[asyncpg.Pool, None]:
    # .env uses the Docker service name "timescaledb" which only resolves
    # inside the compose network. Tests always run on the host, so rewrite
    # to localhost unless an explicit override is present.
    raw = os.environ["DATABASE_URL"].replace("@timescaledb:", "@localhost:")
    dsn = for_asyncpg(raw)
    pool = await asyncpg.create_pool(dsn)
    yield pool
    await pool.close()


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
    # base_url must be https:// so httpx's cookie jar accepts Secure cookies.
    # The ASGI transport doesn't care about the scheme — it's in-process.
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
