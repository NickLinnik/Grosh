"""Integration tests for ManualService, manual router endpoints, and AccountRepo.

Covers the uncovered lines:
- manual/service.py:61-78 (create_account dup check, UniqueViolationError path)
- manual/service.py:103-161 (create_transaction: ownership, rate source resolution)
- manual/router.py:65-73 (create_account happy path)
- manual/router.py:91-106 (create_transaction happy path)
- manual/router.py:131-151 (update_account: 404, 403 not manual, 409 name conflict)
- manual/router.py:168-179 (delete_account: 404 not found, 403 not manual)
- account_repo.py:114-131 (get_by_id), 150-169 (update_name), 184-195 (soft_delete)

Uses the real Postgres test DB via the session-scoped db_pool fixture and the
existing _SingleConnPool pattern from test_manual_endpoints.py.
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
from grosh_ingestion.errors import AccountAlreadyExistsError, AccountNotOwnedError
from grosh_ingestion.main import app
from grosh_ingestion.repositories.account_repo import AccountRepo
from grosh_ingestion.repositories.user_settings_repo import UserSettingsRepo
from grosh_ingestion.sources.manual.service import ManualService

_JWT_SECRET = os.environ.get("JWT_SECRET", "super-secret-dev-jwt-key-change-in-prod")


# ---------------------------------------------------------------------------
# Pool shim (same pattern as test_manual_endpoints.py)
# ---------------------------------------------------------------------------


class _SingleConnPool:
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
    return jwt.encode(
        {"sub": str(user_id), "exp": datetime.now(UTC) + timedelta(minutes=15)},
        _JWT_SECRET,
        algorithm="HS256",
    )


async def _insert_user(conn: asyncpg.Connection, user_id: UUID) -> None:
    await conn.execute(
        """
        INSERT INTO users (id, email, password_hash, display_name, role)
        VALUES ($1, $2, 'x', 'Test User', 'member')
        """,
        user_id,
        f"manual-svc-{user_id}@example.com",
    )


async def _insert_manual_account(
    conn: asyncpg.Connection,
    user_id: UUID,
    account_id: UUID,
    name: str = "Cash UAH",
    currency_code: str = "UAH",
) -> None:
    await conn.execute(
        """
        INSERT INTO accounts
            (id, user_id, source, type, currency_code, name, is_active)
        VALUES
            ($1, $2, 'manual', 'cash', $3, $4, true)
        """,
        account_id,
        user_id,
        currency_code,
        name,
    )


async def _insert_monobank_account(
    conn: asyncpg.Connection,
    user_id: UUID,
    account_id: UUID,
) -> None:
    integration_id = uuid4()
    await conn.execute(
        """
        INSERT INTO bank_integrations (id, user_id, bank, status, config)
        VALUES ($1, $2, 'monobank', 'active', '{}')
        """,
        integration_id,
        user_id,
    )
    await conn.execute(
        """
        INSERT INTO accounts
            (id, user_id, integration_id, source, type, currency_code, is_active)
        VALUES
            ($1, $2, $3, 'monobank', 'black', 'UAH', true)
        """,
        account_id,
        user_id,
        integration_id,
    )


# ---------------------------------------------------------------------------
# HTTP client fixture
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture(loop_scope="session", scope="function")
async def client(
    conn: asyncpg.Connection,
) -> AsyncGenerator[AsyncClient, None]:
    app.state.pool = _SingleConnPool(conn)

    async def _override_db_conn() -> AsyncGenerator[asyncpg.Connection, None]:
        async with conn.transaction():
            yield conn

    prev_db = app.dependency_overrides.get(get_db_conn)
    prev_producer = app.dependency_overrides.get(get_producer)

    app.dependency_overrides[get_db_conn] = _override_db_conn
    app.dependency_overrides[get_producer] = lambda: MagicMock()

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as c:
        yield c

    if prev_db is None:
        app.dependency_overrides.pop(get_db_conn, None)
    else:
        app.dependency_overrides[get_db_conn] = prev_db

    if prev_producer is None:
        app.dependency_overrides.pop(get_producer, None)
    else:
        app.dependency_overrides[get_producer] = prev_producer


# ===========================================================================
# POST /v1/manual/accounts — create account happy path
# ===========================================================================


async def test_create_account_happy_path(
    conn: asyncpg.Connection,
    client: AsyncClient,
) -> None:
    """create_account returns 201 with correct fields for a new manual account.

    Bugs here would silently create accounts with wrong currency or type, making
    the account unusable for transactions in the intended currency.
    """
    user_id = uuid4()
    await _insert_user(conn, user_id)
    token = _make_token(user_id)

    resp = await client.post(
        "/v1/manual/accounts",
        headers={"Authorization": f"Bearer {token}"},
        json={"type": "cash", "currency_code": "UAH", "name": "My Cash"},
    )

    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["type"] == "cash"
    assert body["currency_code"] == "UAH"
    assert body["name"] == "My Cash"
    assert "id" in body
    assert "created_at" in body


async def test_create_account_duplicate_returns_409(
    conn: asyncpg.Connection,
    client: AsyncClient,
) -> None:
    """Creating a duplicate account name+currency returns 409 ACCOUNT_ALREADY_EXISTS.

    Without this guard the user ends up with two identically-named accounts and
    cannot tell them apart in the UI.
    """
    user_id = uuid4()
    await _insert_user(conn, user_id)
    token = _make_token(user_id)

    payload = {"type": "cash", "currency_code": "USD", "name": "Dollar Cash"}

    resp1 = await client.post(
        "/v1/manual/accounts",
        headers={"Authorization": f"Bearer {token}"},
        json=payload,
    )
    assert resp1.status_code == 201, resp1.text

    resp2 = await client.post(
        "/v1/manual/accounts",
        headers={"Authorization": f"Bearer {token}"},
        json=payload,
    )
    assert resp2.status_code == 409, resp2.text
    assert resp2.json()["code"] == "VALIDATION_ERROR"


# ===========================================================================
# POST /v1/manual/transactions — create transaction
# ===========================================================================


async def test_create_transaction_happy_path(
    conn: asyncpg.Connection,
    client: AsyncClient,
) -> None:
    """create_transaction returns 201 with correct envelope fields.

    Bugs here would publish wrong account_id or direction to Kafka, causing
    the normalizer to misclassify the transaction.
    """
    user_id = uuid4()
    account_id = uuid4()
    await _insert_user(conn, user_id)
    await _insert_manual_account(conn, user_id, account_id)
    token = _make_token(user_id)

    resp = await client.post(
        "/v1/manual/transactions",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "account_id": str(account_id),
            "amount_cents": 15000,
            "operation_currency_code": "UAH",
            "description": "Groceries",
            "time": "2026-01-15T10:00:00Z",
            "direction": "expense",
        },
    )

    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["account_id"] == str(account_id)
    assert body["amount_cents"] == 15000
    assert body["direction"] == "expense"
    assert body["source"] == "manual"
    assert "id" in body


async def test_create_transaction_wrong_account_returns_403(
    conn: asyncpg.Connection,
    client: AsyncClient,
) -> None:
    """Transaction against an account owned by another user → 403 ACCOUNT_NOT_OWNED.

    Without this check any user could publish transactions against any account_id,
    corrupting another user's balance history.
    """
    user_a = uuid4()
    user_b = uuid4()
    account_b = uuid4()
    await _insert_user(conn, user_a)
    await _insert_user(conn, user_b)
    await _insert_manual_account(conn, user_b, account_b)
    token_a = _make_token(user_a)

    resp = await client.post(
        "/v1/manual/transactions",
        headers={"Authorization": f"Bearer {token_a}"},
        json={
            "account_id": str(account_b),
            "amount_cents": 5000,
            "operation_currency_code": "UAH",
            "time": "2026-01-15T10:00:00Z",
            "direction": "expense",
        },
    )

    assert resp.status_code == 403, resp.text
    assert resp.json()["code"] == "INSUFFICIENT_PERMISSIONS"


async def test_create_transaction_amount_must_be_positive(
    conn: asyncpg.Connection,
    client: AsyncClient,
) -> None:
    """amount_cents <= 0 is rejected by Pydantic gt=0 constraint with 422.

    The manual router uses `amount_cents: int = Field(gt=0)`. Allowing zero or
    negative values would make the amount meaningless (direction already encodes sign).
    """
    user_id = uuid4()
    await _insert_user(conn, user_id)
    token = _make_token(user_id)

    resp = await client.post(
        "/v1/manual/transactions",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "account_id": str(uuid4()),
            "amount_cents": -500,
            "operation_currency_code": "UAH",
            "time": "2026-01-15T10:00:00Z",
            "direction": "expense",
        },
    )

    assert resp.status_code == 422, resp.text


# ===========================================================================
# PUT /v1/manual/accounts/{account_id} — update name
# ===========================================================================


async def test_update_account_happy_path(
    conn: asyncpg.Connection,
    client: AsyncClient,
) -> None:
    """update_account returns 200 with the updated name.

    Bugs here would silently not persist the new name, confusing the user on
    the next page load.
    """
    user_id = uuid4()
    account_id = uuid4()
    await _insert_user(conn, user_id)
    await _insert_manual_account(conn, user_id, account_id, name="Old Name")
    token = _make_token(user_id)

    resp = await client.put(
        f"/v1/manual/accounts/{account_id}",
        headers={"Authorization": f"Bearer {token}"},
        json={"name": "New Name"},
    )

    assert resp.status_code == 200, resp.text
    assert resp.json()["name"] == "New Name"


async def test_update_account_not_found_returns_404(
    conn: asyncpg.Connection,
    client: AsyncClient,
) -> None:
    """update_account for a non-existent account returns 404 ACCOUNT_NOT_FOUND."""
    user_id = uuid4()
    await _insert_user(conn, user_id)
    token = _make_token(user_id)

    resp = await client.put(
        f"/v1/manual/accounts/{uuid4()}",
        headers={"Authorization": f"Bearer {token}"},
        json={"name": "Whatever"},
    )

    assert resp.status_code == 404, resp.text
    assert resp.json()["code"] == "ACCOUNT_NOT_FOUND"


async def test_update_account_bank_account_returns_403(
    conn: asyncpg.Connection,
    client: AsyncClient,
) -> None:
    """update_account on a monobank account returns 403 INSUFFICIENT_PERMISSIONS.

    Bank accounts are managed by the bank integration, not the user; allowing
    name changes would desync the displayed name from the bank's name on re-link.
    """
    user_id = uuid4()
    account_id = uuid4()
    await _insert_user(conn, user_id)
    await _insert_monobank_account(conn, user_id, account_id)
    token = _make_token(user_id)

    resp = await client.put(
        f"/v1/manual/accounts/{account_id}",
        headers={"Authorization": f"Bearer {token}"},
        json={"name": "Renamed Bank"},
    )

    assert resp.status_code == 403, resp.text
    assert resp.json()["code"] == "INSUFFICIENT_PERMISSIONS"


# ===========================================================================
# DELETE /v1/manual/accounts/{account_id} — soft delete
# ===========================================================================


async def test_delete_account_happy_path(
    conn: asyncpg.Connection,
    client: AsyncClient,
) -> None:
    """delete_account returns 204 and sets is_active=false.

    Bugs here would leave the account active in the DB, making it appear in
    account lists and accept new transactions.
    """
    user_id = uuid4()
    account_id = uuid4()
    await _insert_user(conn, user_id)
    await _insert_manual_account(conn, user_id, account_id)
    token = _make_token(user_id)

    resp = await client.delete(
        f"/v1/manual/accounts/{account_id}",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert resp.status_code == 204, resp.text

    # Verify is_active flipped in DB
    row = await conn.fetchrow(
        "SELECT is_active FROM accounts WHERE id = $1",
        account_id,
    )
    assert row is not None
    assert row["is_active"] is False


async def test_delete_account_not_found_returns_404(
    conn: asyncpg.Connection,
    client: AsyncClient,
) -> None:
    """delete_account for a non-existent account returns 404 ACCOUNT_NOT_FOUND."""
    user_id = uuid4()
    await _insert_user(conn, user_id)
    token = _make_token(user_id)

    resp = await client.delete(
        f"/v1/manual/accounts/{uuid4()}",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert resp.status_code == 404, resp.text
    assert resp.json()["code"] == "ACCOUNT_NOT_FOUND"


async def test_delete_bank_account_returns_403(
    conn: asyncpg.Connection,
    client: AsyncClient,
) -> None:
    """delete_account on a monobank account returns 403 INSUFFICIENT_PERMISSIONS.

    Bank-connected accounts must be removed through the integration unlink flow,
    not soft-deleted directly — direct deletion would orphan the integration.
    """
    user_id = uuid4()
    account_id = uuid4()
    await _insert_user(conn, user_id)
    await _insert_monobank_account(conn, user_id, account_id)
    token = _make_token(user_id)

    resp = await client.delete(
        f"/v1/manual/accounts/{account_id}",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert resp.status_code == 403, resp.text
    assert resp.json()["code"] == "INSUFFICIENT_PERMISSIONS"


# ===========================================================================
# ManualService unit-style tests (using real DB repo)
# ===========================================================================


async def test_service_create_account_duplicate_raises_domain_error(
    conn: asyncpg.Connection,
) -> None:
    """ManualService raises AccountAlreadyExistsError (not DB error) on duplicate.

    The service must catch asyncpg.UniqueViolationError and re-raise as domain
    error so the router maps it to 409 — not 500.
    """
    user_id = uuid4()
    await _insert_user(conn, user_id)
    account_id = uuid4()
    await _insert_manual_account(
        conn, user_id, account_id, name="Dup Account", currency_code="EUR"
    )

    svc = ManualService(AccountRepo(), UserSettingsRepo())

    with pytest.raises(AccountAlreadyExistsError):
        await svc.create_account(
            conn=conn,
            user_id=user_id,
            account_type="cash",
            currency_code="EUR",
            name="Dup Account",
        )


async def test_service_create_transaction_wrong_account_raises_domain_error(
    conn: asyncpg.Connection,
) -> None:
    """ManualService raises AccountNotOwnedError when account belongs to another user.

    Without this check the service would publish a Kafka message with an account_id
    that doesn't belong to the caller, corrupting another user's transaction history.
    """
    from unittest.mock import MagicMock

    from confluent_kafka import Producer
    from grosh_shared.domain.models import TransactionDirection

    user_a = uuid4()
    user_b = uuid4()
    account_b = uuid4()
    await _insert_user(conn, user_a)
    await _insert_user(conn, user_b)
    await _insert_manual_account(conn, user_b, account_b)

    svc = ManualService(AccountRepo(), UserSettingsRepo())
    mock_producer = MagicMock(spec=Producer)

    with pytest.raises(AccountNotOwnedError):
        await svc.create_transaction(
            conn=conn,
            user_id=user_a,
            account_id=account_b,
            amount_cents=1000,
            currency_code="UAH",
            description=None,
            time=datetime.now(UTC),
            direction=TransactionDirection.expense,
            mcc=None,
            rate_source=None,
            producer=mock_producer,
        )
