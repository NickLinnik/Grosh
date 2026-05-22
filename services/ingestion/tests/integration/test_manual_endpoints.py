"""Integration tests for the manual-entry endpoints (Slice 32 schema tightening).

Covers the Pydantic-level rejection of invalid `type` and `direction` values.
Before Slice 32 these were rejected by hand-rolled `if` guards in the handler;
now they are rejected by `Literal[...]` annotations at parse time, before the
handler runs.
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

from grosh_ingestion.deps import get_db_conn, get_producer
from grosh_ingestion.main import app

_JWT_SECRET = os.environ.get("JWT_SECRET", "super-secret-dev-jwt-key-change-in-prod")


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


def _make_token(user_id: UUID) -> str:
    payload = {
        "sub": str(user_id),
        "exp": datetime.now(UTC) + timedelta(minutes=15),
    }
    return jwt.encode(payload, _JWT_SECRET, algorithm="HS256")


async def _insert_user(conn: asyncpg.Connection, user_id: UUID) -> None:
    await conn.execute(
        """
        INSERT INTO users (id, email, password_hash, display_name, role)
        VALUES ($1, $2, 'x', 'Test User', 'member')
        """,
        user_id,
        f"test-{user_id}@example.com",
    )


@pytest_asyncio.fixture(loop_scope="session", scope="function")
async def client(
    conn: asyncpg.Connection,
) -> AsyncGenerator[AsyncClient, None]:
    app.state.pool = _SingleConnPool(conn)

    async def _override_db_conn() -> AsyncGenerator[asyncpg.Connection, None]:
        async with conn.transaction():
            yield conn

    prev_db_conn = app.dependency_overrides.get(get_db_conn)
    prev_producer = app.dependency_overrides.get(get_producer)
    app.dependency_overrides[get_db_conn] = _override_db_conn
    app.dependency_overrides[get_producer] = lambda: MagicMock()

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as c:
        yield c

    if prev_db_conn is None:
        app.dependency_overrides.pop(get_db_conn, None)
    else:
        app.dependency_overrides[get_db_conn] = prev_db_conn

    if prev_producer is None:
        app.dependency_overrides.pop(get_producer, None)
    else:
        app.dependency_overrides[get_producer] = prev_producer


@pytest.mark.asyncio
async def test_create_account_rejects_non_cash_type(
    conn: asyncpg.Connection,
    client: AsyncClient,
) -> None:
    """type='checking' is rejected by Pydantic Literal['cash'] before handler runs."""
    user_id = uuid4()
    await _insert_user(conn, user_id)
    token = _make_token(user_id)

    resp = await client.post(
        "/v1/manual/accounts",
        headers={"Authorization": f"Bearer {token}"},
        json={"type": "checking", "currency_code": "UAH", "name": "Test"},
    )

    assert resp.status_code == 422
    body = resp.json()
    # RFC-7807 envelope
    assert body["status"] == 422
    errors = body["validation_errors"]
    assert any(
        err["loc"] == ["body", "type"] and err["type"] == "literal_error"
        for err in errors
    ), f"Expected literal_error on body.type, got {errors}"


@pytest.mark.asyncio
async def test_create_transaction_rejects_zero_direction(
    conn: asyncpg.Connection,
    client: AsyncClient,
) -> None:
    """direction='zero' is rejected by Pydantic Literal[income, expense] at parse."""
    user_id = uuid4()
    await _insert_user(conn, user_id)
    token = _make_token(user_id)

    resp = await client.post(
        "/v1/manual/transactions",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "account_id": str(uuid4()),
            "amount_cents": 100,
            "operation_currency_code": "UAH",
            "time": "2026-01-01T00:00:00Z",
            "direction": "zero",
        },
    )

    assert resp.status_code == 422
    body = resp.json()
    assert body["status"] == 422
    errors = body["validation_errors"]
    assert any(
        err["loc"] == ["body", "direction"] and err["type"] == "literal_error"
        for err in errors
    ), f"Expected literal_error on body.direction, got {errors}"
