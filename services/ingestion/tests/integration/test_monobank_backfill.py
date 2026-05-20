"""Integration tests for backfill window validation (Slice 25).

Cases:
  (a) from=2025-01-01 & to=2025-02-01 (exactly 31 days, half-open) → 202 accepted.
  (b) from=2025-01-01 & to=2025-02-02 (32 days) → 422 BACKFILL_WINDOW_TOO_LARGE.
  (c) from=2025-02-01 & to=2025-01-01 (inverted) → 422 INVALID_DATE_RANGE.
  (d) from=2025-01-01 & to=2025-01-01 (zero-day window) → 422 INVALID_DATE_RANGE.

The K8s dispatcher (BackfillService) is mocked to avoid real cluster calls.
Postgres is real (via the session-scoped db_pool fixture).
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

from grosh_ingestion.deps import get_backfill_service, get_db_conn
from grosh_ingestion.main import app
from grosh_ingestion.services.backfill_service import BackfillService

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_JWT_SECRET = os.environ.get("JWT_SECRET", "super-secret-dev-jwt-key-change-in-prod")
_ENCRYPTION_KEY = "test-encryption-key-backfill"
_MOCK_JOB_NAME = "monobank-backfill-20250101-000000-abcd1234"


# ---------------------------------------------------------------------------
# Pool shim — identical to other integration test files
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


def _make_token(user_id: UUID, role: str = "member") -> str:
    payload = {
        "sub": str(user_id),
        "exp": datetime.now(UTC) + timedelta(minutes=15),
    }
    return jwt.encode(payload, _JWT_SECRET, algorithm="HS256")


async def _insert_user(
    conn: asyncpg.Connection, user_id: UUID, role: str = "member"
) -> None:
    await conn.execute(
        """
        INSERT INTO users
            (id, email, password_hash, display_name, role)
        VALUES
            ($1, $2, 'x', 'Test User', $3)
        """,
        user_id,
        f"backfill-{user_id}@example.com",
        role,
    )


async def _insert_integration(
    conn: asyncpg.Connection,
    user_id: UUID,
    integration_id: UUID,
) -> None:
    await conn.execute(
        """
        INSERT INTO bank_integrations
            (id, user_id, bank, status, config)
        VALUES
            ($1, $2, 'monobank', 'active', '{}')
        """,
        integration_id,
        user_id,
    )


async def _insert_account(
    conn: asyncpg.Connection,
    user_id: UUID,
    account_id: UUID,
    integration_id: UUID,
    external_id: str,
) -> None:
    await conn.execute(
        """
        INSERT INTO accounts
            (id, user_id, integration_id, source, external_id,
             type, currency_code, name)
        VALUES
            ($1, $2, $3, 'monobank', $4, 'black', 'UAH', 'Test Card')
        """,
        account_id,
        user_id,
        integration_id,
        external_id,
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="function")
def mock_backfill_service() -> MagicMock:
    service = MagicMock(spec=BackfillService)
    service.trigger_transactions_backfill.return_value = _MOCK_JOB_NAME
    return service


@pytest_asyncio.fixture(loop_scope="session", scope="function")
async def http_client(
    conn: asyncpg.Connection,
    mock_backfill_service: MagicMock,
) -> AsyncGenerator[AsyncClient, None]:
    """ASGI test client wired to the rolled-back test connection."""
    app.state.pool = _SingleConnPool(conn)

    os.environ.setdefault("JWT_SECRET", _JWT_SECRET)
    os.environ.setdefault("ENCRYPTION_KEY", _ENCRYPTION_KEY)

    async def _override_db_conn() -> AsyncGenerator[asyncpg.Connection, None]:
        async with conn.transaction():
            yield conn

    prev_db_conn = app.dependency_overrides.get(get_db_conn)
    prev_backfill = app.dependency_overrides.get(get_backfill_service)

    app.dependency_overrides[get_db_conn] = _override_db_conn
    app.dependency_overrides[get_backfill_service] = lambda: mock_backfill_service

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as c:
        yield c

    if prev_db_conn is None:
        app.dependency_overrides.pop(get_db_conn, None)
    else:
        app.dependency_overrides[get_db_conn] = prev_db_conn

    if prev_backfill is None:
        app.dependency_overrides.pop(get_backfill_service, None)
    else:
        app.dependency_overrides[get_backfill_service] = prev_backfill


# ---------------------------------------------------------------------------
# Case (a): exactly 31 days (half-open) is accepted with 202
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_backfill_31_days_accepted(
    conn: asyncpg.Connection,
    http_client: AsyncClient,
    mock_backfill_service: MagicMock,
) -> None:
    """A 31-day window [2025-01-01, 2025-02-01) is exactly within the cap → 202."""
    user_id = uuid4()
    account_id = uuid4()
    integration_id = uuid4()

    await _insert_user(conn, user_id)
    await _insert_integration(conn, user_id, integration_id)
    await _insert_account(conn, user_id, account_id, integration_id, "mono-ext-1")

    token = _make_token(user_id)
    resp = await http_client.post(
        f"/v1/monobank/accounts/{account_id}/backfill",
        params={"from": "2025-01-01", "to": "2025-02-01"},
        headers={"Authorization": f"Bearer {token}"},
    )

    assert resp.status_code == 202, resp.text
    body = resp.json()
    assert body["job_id"] == _MOCK_JOB_NAME
    assert f"/accounts/{account_id}/backfill/{_MOCK_JOB_NAME}" in body["status_url"]
    mock_backfill_service.trigger_transactions_backfill.assert_called_once()


# ---------------------------------------------------------------------------
# Case (b): 32 days → 422 BACKFILL_WINDOW_TOO_LARGE
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_backfill_32_days_rejected(
    conn: asyncpg.Connection,
    http_client: AsyncClient,
    mock_backfill_service: MagicMock,
) -> None:
    """A 32-day window is one day over the cap → 422 BACKFILL_WINDOW_TOO_LARGE."""
    user_id = uuid4()
    await _insert_user(conn, user_id)

    token = _make_token(user_id)
    resp = await http_client.post(
        f"/v1/monobank/accounts/{uuid4()}/backfill",
        params={"from": "2025-01-01", "to": "2025-02-02"},
        headers={"Authorization": f"Bearer {token}"},
    )

    assert resp.status_code == 422, resp.text
    body = resp.json()
    assert body["code"] == "BACKFILL_WINDOW_TOO_LARGE"
    assert "32 days" in body["detail"]
    mock_backfill_service.trigger_transactions_backfill.assert_not_called()


# ---------------------------------------------------------------------------
# Case (c): inverted range → 422 INVALID_DATE_RANGE
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_backfill_inverted_range_rejected(
    conn: asyncpg.Connection,
    http_client: AsyncClient,
    mock_backfill_service: MagicMock,
) -> None:
    """from > to is invalid → 422 INVALID_DATE_RANGE."""
    user_id = uuid4()
    await _insert_user(conn, user_id)

    token = _make_token(user_id)
    resp = await http_client.post(
        f"/v1/monobank/accounts/{uuid4()}/backfill",
        params={"from": "2025-02-01", "to": "2025-01-01"},
        headers={"Authorization": f"Bearer {token}"},
    )

    assert resp.status_code == 422, resp.text
    body = resp.json()
    assert body["code"] == "INVALID_DATE_RANGE"
    mock_backfill_service.trigger_transactions_backfill.assert_not_called()


# ---------------------------------------------------------------------------
# Case (d): zero-day window (from == to) → 422 INVALID_DATE_RANGE
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_backfill_zero_day_window_rejected(
    conn: asyncpg.Connection,
    http_client: AsyncClient,
    mock_backfill_service: MagicMock,
) -> None:
    """from == to is a zero-day window → 422 INVALID_DATE_RANGE."""
    user_id = uuid4()
    await _insert_user(conn, user_id)

    token = _make_token(user_id)
    resp = await http_client.post(
        f"/v1/monobank/accounts/{uuid4()}/backfill",
        params={"from": "2025-01-01", "to": "2025-01-01"},
        headers={"Authorization": f"Bearer {token}"},
    )

    assert resp.status_code == 422, resp.text
    body = resp.json()
    assert body["code"] == "INVALID_DATE_RANGE"
    mock_backfill_service.trigger_transactions_backfill.assert_not_called()
