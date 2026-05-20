"""Integration tests for POST /reprocess router.

Uses real Postgres for lock_exists / last_reprocess_at checks.
Mocks ReprocessDispatcher to avoid spawning K8s Jobs.
"""

import os
from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import MagicMock
from uuid import UUID, uuid4

import asyncpg
import jwt
import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from grosh_ingestion.deps import get_db_conn, get_reprocess_dispatcher
from grosh_ingestion.errors import K8sDispatchError
from grosh_ingestion.main import app
from grosh_ingestion.services.reprocess_dispatcher import ReprocessDispatcher

_JWT_SECRET = os.environ.get("JWT_SECRET", "super-secret-dev-jwt-key-change-in-prod")
_MOCK_JOB_NAME = "grosh-reprocess-abc12345-1234567890"


# ---------------------------------------------------------------------------
# Pool shim — makes app.state.pool return the test connection
# ---------------------------------------------------------------------------


class _SingleConnPool:
    """Wraps one asyncpg connection to satisfy pool.acquire() calls."""

    def __init__(self, conn: asyncpg.Connection) -> None:
        self._conn = conn

    def acquire(self) -> "_SingleConnPool":
        return self

    async def __aenter__(self) -> asyncpg.Connection:
        return self._conn

    async def __aexit__(self, *_: Any) -> None:
        pass


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_token(user_id: UUID) -> str:
    payload = {
        "sub": str(user_id),
        "exp": datetime.now(UTC) + timedelta(minutes=15),
    }
    return jwt.encode(payload, _JWT_SECRET, algorithm="HS256")


async def _insert_user(conn: asyncpg.Connection, user_id: UUID) -> None:
    await conn.execute(
        """
        INSERT INTO users
            (id, email, password_hash, display_name, role)
        VALUES
            ($1, $2, 'x', 'Test User', 'member')
        """,
        user_id,
        f"test-{user_id}@example.com",
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="function")
def mock_dispatcher() -> MagicMock:
    dispatcher = MagicMock(spec=ReprocessDispatcher)
    dispatcher.trigger_reprocess.return_value = _MOCK_JOB_NAME
    return dispatcher


@pytest_asyncio.fixture(loop_scope="session", scope="function")
async def client(
    conn: asyncpg.Connection, mock_dispatcher: MagicMock
) -> AsyncGenerator[AsyncClient, None]:
    app.state.pool = _SingleConnPool(conn)

    async def _override_db_conn() -> AsyncGenerator[asyncpg.Connection, None]:
        async with conn.transaction():
            yield conn

    prev_db_conn = app.dependency_overrides.get(get_db_conn)
    prev_dispatcher = app.dependency_overrides.get(get_reprocess_dispatcher)

    app.dependency_overrides[get_db_conn] = _override_db_conn
    app.dependency_overrides[get_reprocess_dispatcher] = lambda: mock_dispatcher

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as c:
        yield c

    if prev_db_conn is None:
        app.dependency_overrides.pop(get_db_conn, None)
    else:
        app.dependency_overrides[get_db_conn] = prev_db_conn

    if prev_dispatcher is None:
        app.dependency_overrides.pop(get_reprocess_dispatcher, None)
    else:
        app.dependency_overrides[get_reprocess_dispatcher] = prev_dispatcher


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_202_happy_path(
    conn: asyncpg.Connection,
    client: AsyncClient,
    mock_dispatcher: MagicMock,
) -> None:
    """Valid JWT, no lock, no recent backup → 202 with expected body."""
    user_id = uuid4()
    await _insert_user(conn, user_id)
    token = _make_token(user_id)

    resp = await client.post(
        "/v1/reprocess", headers={"Authorization": f"Bearer {token}"}
    )

    assert resp.status_code == 202, resp.text
    body = resp.json()
    assert body["status"] == "started"
    assert body["user_id"] == str(user_id)
    assert body["job_name"] == _MOCK_JOB_NAME
    mock_dispatcher.trigger_reprocess.assert_called_once_with(user_id)


@pytest.mark.asyncio
async def test_409_lock_conflict(
    conn: asyncpg.Connection,
    client: AsyncClient,
    mock_dispatcher: MagicMock,
) -> None:
    """A reprocessing_locks row for the user → 409."""
    user_id = uuid4()
    await _insert_user(conn, user_id)
    await conn.execute(
        "INSERT INTO reprocessing_locks (user_id) VALUES ($1)",
        user_id,
    )
    token = _make_token(user_id)

    resp = await client.post(
        "/v1/reprocess", headers={"Authorization": f"Bearer {token}"}
    )

    assert resp.status_code == 409, resp.text
    mock_dispatcher.trigger_reprocess.assert_not_called()


@pytest.mark.asyncio
async def test_429_cooldown(
    conn: asyncpg.Connection,
    client: AsyncClient,
    mock_dispatcher: MagicMock,
) -> None:
    """A recent reprocessing_backups row (30 min ago) → 429 cooldown."""
    user_id = uuid4()
    await _insert_user(conn, user_id)
    recent = datetime.now(UTC) - timedelta(minutes=30)
    await conn.execute(
        "INSERT INTO reprocessing_backups (user_id, data, created_at)"
        " VALUES ($1, $2::jsonb, $3)",
        user_id,
        "[]",
        recent,
    )
    token = _make_token(user_id)

    resp = await client.post(
        "/v1/reprocess", headers={"Authorization": f"Bearer {token}"}
    )

    assert resp.status_code == 429, resp.text
    mock_dispatcher.trigger_reprocess.assert_not_called()


@pytest.mark.asyncio
async def test_401_no_auth(client: AsyncClient) -> None:
    """No Authorization header → 401."""
    resp = await client.post("/v1/reprocess")
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_502_k8s_failure(
    conn: asyncpg.Connection,
    client: AsyncClient,
    mock_dispatcher: MagicMock,
) -> None:
    """Dispatcher raises K8sDispatchError → 502 with JOB_SUBMISSION_FAILED envelope."""
    user_id = uuid4()
    await _insert_user(conn, user_id)
    mock_dispatcher.trigger_reprocess.side_effect = K8sDispatchError(
        "Cannot create reprocess job: K8s API error"
    )
    token = _make_token(user_id)

    resp = await client.post(
        "/v1/reprocess", headers={"Authorization": f"Bearer {token}"}
    )

    assert resp.status_code == 502, resp.text
    body = resp.json()
    assert body["code"] == "JOB_SUBMISSION_FAILED"
    mock_dispatcher.trigger_reprocess.reset_mock(side_effect=True)
    mock_dispatcher.trigger_reprocess.return_value = _MOCK_JOB_NAME
