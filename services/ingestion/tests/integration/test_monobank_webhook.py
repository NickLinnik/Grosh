"""Integration tests for the Monobank webhook receiver endpoint.

Cases:
  (a) Happy path — valid payload → producer.produce called once with
      correct topic/key/value.
  (b) Unknown webhook_secret → 404 INTEGRATION_NOT_FOUND, zero produce calls.
  (c) Unknown account (valid secret, unknown account id) →
      404 ACCOUNT_NOT_FOUND, zero produce calls.
  (d) Malformed statementItem (outer payload valid, inner dict missing
      required fields) → 422 VALIDATION_ERROR, zero produce calls.

Postgres is real (session-scoped db_pool). Producer is a MagicMock.
"""

import json
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
from grosh_shared.envelope import TransactionEnvelope
from grosh_shared.models import Topic
from httpx import ASGITransport, AsyncClient

from grosh_ingestion.deps import get_db_conn, get_producer
from grosh_ingestion.main import app
from grosh_ingestion.sources.monobank.router import get_monobank_repo

_JWT_SECRET = os.environ.get("JWT_SECRET", "super-secret-dev-jwt-key-change-in-prod")
_ENCRYPTION_KEY = os.environ.get("ENCRYPTION_KEY", "test-encryption-key-webhook")

# A valid MonobankStatementItem in its wire (camelCase) form.
_VALID_STATEMENT_ITEM: dict[str, Any] = {
    "id": "stmt-abc-123",
    "time": 1700000000,
    "description": "Coffee shop",
    "mcc": 5812,
    "originalMcc": 5812,
    "hold": False,
    "amount": -5000,
    "operationAmount": -5000,
    "currencyCode": 980,
    "cashbackAmount": 0,
    "balance": 100000,
}


# ---------------------------------------------------------------------------
# Pool shim — identical to other test modules
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
        f"webhook-{user_id}@example.com",
    )


async def _insert_integration(
    conn: asyncpg.Connection,
    user_id: UUID,
    webhook_secret: str,
) -> UUID:
    """Seed a minimal active Monobank bank_integrations row."""
    row = await conn.fetchrow(
        """
        INSERT INTO bank_integrations
            (user_id, bank, config, status)
        VALUES
            ($1, 'monobank', $2::jsonb, 'active')
        RETURNING id
        """,
        user_id,
        json.dumps(
            {
                "webhook_secret": webhook_secret,
                "webhook_url": f"https://test.example.com/monobank/webhook/{webhook_secret}",
                "monobank_client_id": f"client-{user_id}",
                "encrypted_token": "deadbeef",
                "key_version": 1,
            }
        ),
    )
    return row["id"]


async def _insert_account(
    conn: asyncpg.Connection,
    user_id: UUID,
    integration_id: UUID,
    external_id: str,
) -> UUID:
    """Seed a minimal account row linked to the given integration."""
    row = await conn.fetchrow(
        """
        INSERT INTO accounts
            (user_id, integration_id, external_id, source, type,
             currency_code, config)
        VALUES
            ($1, $2, $3, 'monobank', 'black', 'UAH', '{}')
        RETURNING id
        """,
        user_id,
        integration_id,
        external_id,
    )
    return row["id"]


# ---------------------------------------------------------------------------
# Fixture
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture(loop_scope="session", scope="function")
async def webhook_client(
    conn: asyncpg.Connection,
) -> AsyncGenerator[tuple[AsyncClient, MagicMock], None]:
    """ASGI test client with a shared producer MagicMock."""
    app.state.pool = _SingleConnPool(conn)

    producer_mock = MagicMock()

    async def _override_db_conn() -> AsyncGenerator[asyncpg.Connection, None]:
        async with conn.transaction():
            yield conn

    prev_db_conn = app.dependency_overrides.get(get_db_conn)
    prev_producer = app.dependency_overrides.get(get_producer)
    prev_monobank_repo = app.dependency_overrides.get(get_monobank_repo)
    app.dependency_overrides[get_db_conn] = _override_db_conn
    app.dependency_overrides[get_producer] = lambda: producer_mock
    # Remove any unit-test mock for get_monobank_repo so the real repo is used.
    app.dependency_overrides.pop(get_monobank_repo, None)

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as c:
        yield c, producer_mock

    if prev_db_conn is None:
        app.dependency_overrides.pop(get_db_conn, None)
    else:
        app.dependency_overrides[get_db_conn] = prev_db_conn

    if prev_producer is None:
        app.dependency_overrides.pop(get_producer, None)
    else:
        app.dependency_overrides[get_producer] = prev_producer

    if prev_monobank_repo is None:
        app.dependency_overrides.pop(get_monobank_repo, None)
    else:
        app.dependency_overrides[get_monobank_repo] = prev_monobank_repo


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_webhook_happy_path_publishes_envelope(
    conn: asyncpg.Connection,
    webhook_client: tuple[AsyncClient, MagicMock],
) -> None:
    """Valid payload → producer.produce called once with correct topic/key/envelope."""
    client, producer = webhook_client

    user_id = uuid4()
    await _insert_user(conn, user_id)

    secret = "validhexsecret0000000001"
    integration_id = await _insert_integration(conn, user_id, secret)

    external_account_id = "mono-account-happy"
    account_id = await _insert_account(
        conn, user_id, integration_id, external_account_id
    )

    resp = await client.post(
        f"/monobank/webhook/{secret}",
        json={
            "type": "StatementItem",
            "data": {
                "account": external_account_id,
                "statementItem": _VALID_STATEMENT_ITEM,
            },
        },
    )

    assert resp.status_code == 200, resp.text

    # Producer must have been called exactly once.
    assert producer.produce.call_count == 1

    call_kwargs = producer.produce.call_args.kwargs
    assert call_kwargs["topic"] == Topic.raw_transactions_monobank
    assert call_kwargs["key"] == str(user_id).encode()

    # Decode the envelope and verify fields.
    envelope = TransactionEnvelope.model_validate_json(call_kwargs["value"])
    assert envelope.user_id == user_id
    assert envelope.account_id == account_id
    assert envelope.source == "monobank"
    assert envelope.payload == _VALID_STATEMENT_ITEM

    # poll(0) must also have been called (fire-and-forget delivery callback trigger).
    assert producer.poll.call_count == 1


@pytest.mark.asyncio
async def test_webhook_unknown_secret_returns_404(
    conn: asyncpg.Connection,
    webhook_client: tuple[AsyncClient, MagicMock],
) -> None:
    """Unknown webhook_secret → 404 INTEGRATION_NOT_FOUND, zero produce calls."""
    client, producer = webhook_client

    resp = await client.post(
        "/monobank/webhook/nonexistent-secret-xyz",
        json={
            "type": "StatementItem",
            "data": {
                "account": "some-account",
                "statementItem": _VALID_STATEMENT_ITEM,
            },
        },
    )

    assert resp.status_code == 404, resp.text
    body = resp.json()
    assert body["code"] == "INTEGRATION_NOT_FOUND"
    assert producer.produce.call_count == 0


@pytest.mark.asyncio
async def test_webhook_unknown_account_returns_404(
    conn: asyncpg.Connection,
    webhook_client: tuple[AsyncClient, MagicMock],
) -> None:
    """Valid secret but unknown account_id → 404 ACCOUNT_NOT_FOUND,
    zero produce calls."""
    client, producer = webhook_client

    user_id = uuid4()
    await _insert_user(conn, user_id)

    secret = "validhexsecret0000000002"
    await _insert_integration(conn, user_id, secret)
    # Deliberately do NOT insert an account row for this integration.

    resp = await client.post(
        f"/monobank/webhook/{secret}",
        json={
            "type": "StatementItem",
            "data": {
                "account": "account-that-does-not-exist",
                "statementItem": _VALID_STATEMENT_ITEM,
            },
        },
    )

    assert resp.status_code == 404, resp.text
    body = resp.json()
    assert body["code"] == "ACCOUNT_NOT_FOUND"
    assert producer.produce.call_count == 0


@pytest.mark.asyncio
async def test_webhook_malformed_statement_returns_422(
    conn: asyncpg.Connection,
    webhook_client: tuple[AsyncClient, MagicMock],
) -> None:
    """Outer payload valid (statementItem is dict[str, Any]) but inner dict missing
    required fields → 422 VALIDATION_ERROR, zero produce calls.

    MonobankWebhookData.statement_item is typed dict[str, Any], so the outer
    Pydantic validation accepts any dict. The receive_webhook handler calls
    MonobankStatementItem.model_validate(payload.data.statement_item) which is
    the gate that rejects the malformed inner dict with a 422.
    """
    client, producer = webhook_client

    user_id = uuid4()
    await _insert_user(conn, user_id)

    secret = "validhexsecret0000000003"
    integration_id = await _insert_integration(conn, user_id, secret)

    external_account_id = "mono-account-malformed"
    await _insert_account(conn, user_id, integration_id, external_account_id)

    # statementItem is missing all required fields (id, time, mcc, etc.)
    resp = await client.post(
        f"/monobank/webhook/{secret}",
        json={
            "type": "StatementItem",
            "data": {
                "account": external_account_id,
                "statementItem": {
                    "junk": "payload",
                    "missing_all_required_fields": True,
                },
            },
        },
    )

    assert resp.status_code == 422, resp.text
    body = resp.json()
    assert body["code"] == "VALIDATION_ERROR"
    assert producer.produce.call_count == 0
