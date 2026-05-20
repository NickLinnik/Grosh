"""Integration tests for the Monobank lifecycle endpoints.

Cases:
  (a) Idempotency — link twice with same token → second call 200 is_new=False.
  (b) Conflict — link as client A then client B → 409 INTEGRATION_ALREADY_LINKED.
  (c) Rebind continuity — link, simulate a transaction, delete, re-link →
      accounts rebound with was_rebound=True and transaction still attached.

MonobankClient is patched at the module where it is instantiated so no real
HTTP calls are made. Postgres is real (via the session-scoped db_pool fixture).
"""

import os
from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID, uuid4

import asyncpg
import jwt
import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from grosh_ingestion.deps import get_db_conn
from grosh_ingestion.main import app

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_JWT_SECRET = os.environ.get("JWT_SECRET", "super-secret-dev-jwt-key-change-in-prod")
_ENCRYPTION_KEY = "test-encryption-key-lifecycle"
_WEBHOOK_BASE_URL = "https://test.example.com"

_CLIENT_ID_A = "stable-client-A"
_CLIENT_ID_B = "stable-client-B"

_MONO_ACCOUNT_1 = {
    "id": "mono-acc-1",
    "sendId": "send-1",
    "balance": 100000,
    "creditLimit": 0,
    "type": "black",
    "currencyCode": 980,  # UAH
    "cashbackType": "UAH",
    "maskedPan": ["537541XXXXXX1234"],
    "iban": "UA123456789012345678901234567",
}

_MONO_ACCOUNT_2 = {
    "id": "mono-acc-2",
    "sendId": "send-2",
    "balance": 200000,
    "creditLimit": 0,
    "type": "black",
    "currencyCode": 840,  # USD
    "cashbackType": None,
    "maskedPan": [],
    "iban": "UA987654321098765432109876543",
}


def _client_info(client_id: str, accounts: list[dict[str, Any]]) -> MagicMock:
    """Build a fake MonobankClientInfo-like object."""
    info = MagicMock()
    info.client_id = client_id
    info.accounts = [_mono_account(a) for a in accounts]
    return info


def _mono_account(data: dict[str, Any]) -> MagicMock:
    acc = MagicMock()
    acc.id = data["id"]
    acc.currency_code = data["currencyCode"]
    acc.type = data["type"]
    acc.cashback_type = data.get("cashbackType")
    acc.masked_pan = data.get("maskedPan", [])
    acc.iban = data["iban"]
    return acc


def _make_client_mock(client_id: str, accounts: list[dict[str, Any]]) -> MagicMock:
    """Return a MonobankClient mock that get_client_info returns the given shape."""
    client_mock = AsyncMock()
    client_mock.get_client_info = AsyncMock(
        return_value=_client_info(client_id, accounts)
    )
    client_mock.set_webhook = AsyncMock()
    client_mock.__aenter__ = AsyncMock(return_value=client_mock)
    client_mock.__aexit__ = AsyncMock(return_value=False)
    return client_mock


# ---------------------------------------------------------------------------
# Pool shim — identical to test_reprocess_router.py
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
        f"lifecycle-{user_id}@example.com",
    )


async def _insert_minimal_transaction(
    conn: asyncpg.Connection,
    user_id: UUID,
    account_id: UUID,
) -> UUID:
    """Insert a minimal transaction row for a given account and return its id."""
    row = await conn.fetchrow(
        """
        INSERT INTO transactions (
            source_id,
            user_id,
            account_id,
            time,
            amount_cents,
            currency_code,
            direction,
            source,
            origin
        )
        VALUES (
            $1, $2, $3, $4, $5, $6,
            'expense'::transaction_direction,
            'monobank'::transaction_source,
            'bank'::transaction_origin
        )
        RETURNING id
        """,
        f"tx-lifecycle-{uuid4()}",
        user_id,
        account_id,
        datetime.now(UTC),
        -10000,
        "UAH",
    )
    return row["id"]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture(loop_scope="session", scope="function")
async def http_client(
    conn: asyncpg.Connection,
) -> AsyncGenerator[AsyncClient, None]:
    """ASGI test client wired to the rolled-back test connection."""
    app.state.pool = _SingleConnPool(conn)

    # Patch required env vars for the lifecycle router
    os.environ.setdefault("WEBHOOK_BASE_URL", _WEBHOOK_BASE_URL)
    os.environ.setdefault("ENCRYPTION_KEY", _ENCRYPTION_KEY)
    os.environ.setdefault("JWT_SECRET", _JWT_SECRET)

    async def _override_db_conn() -> AsyncGenerator[asyncpg.Connection, None]:
        async with conn.transaction():
            yield conn

    prev_db_conn = app.dependency_overrides.get(get_db_conn)
    app.dependency_overrides[get_db_conn] = _override_db_conn

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as c:
        yield c

    if prev_db_conn is None:
        app.dependency_overrides.pop(get_db_conn, None)
    else:
        app.dependency_overrides[get_db_conn] = prev_db_conn


# ---------------------------------------------------------------------------
# Case (a): idempotency
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_link_idempotency(
    conn: asyncpg.Connection,
    http_client: AsyncClient,
) -> None:
    """Second link with the same token returns 200 is_new=False; no new DB rows."""
    user_id = uuid4()
    await _insert_user(conn, user_id)
    token = _make_token(user_id)

    client_mock = _make_client_mock(_CLIENT_ID_A, [_MONO_ACCOUNT_1, _MONO_ACCOUNT_2])

    with patch(
        "grosh_ingestion.sources.monobank.linking_service.MonobankClient",
        return_value=client_mock,
    ):
        # First link — must be 201 with is_new=True
        resp1 = await http_client.post(
            "/v1/monobank/link",
            json={"token": "fake-token-A"},
            headers={"Authorization": f"Bearer {token}"},
        )
        assert resp1.status_code == 201, resp1.text
        body1 = resp1.json()
        assert body1["is_new"] is True
        assert len(body1["accounts"]) == 2

        account_ids_after_first = {a["account_id"] for a in body1["accounts"]}

        # Capture integration count and account count after first link
        integration_count_after_first = await conn.fetchval(
            """
            SELECT COUNT(*)
            FROM bank_integrations
            WHERE user_id = $1
                AND bank = 'monobank'
                AND status = 'active'
            """,
            user_id,
        )
        account_count_after_first = await conn.fetchval(
            "SELECT COUNT(*) FROM accounts WHERE user_id = $1",
            user_id,
        )

        # Second link with the same token — must be 200 with is_new=False
        resp2 = await http_client.post(
            "/v1/monobank/link",
            json={"token": "fake-token-A"},
            headers={"Authorization": f"Bearer {token}"},
        )
        assert resp2.status_code == 200, resp2.text
        body2 = resp2.json()
        assert body2["is_new"] is False
        assert body2["integration_id"] == body1["integration_id"]

        # Accounts list must match (same IDs, same count)
        account_ids_after_second = {a["account_id"] for a in body2["accounts"]}
        assert account_ids_after_second == account_ids_after_first

    # DB: still exactly 1 active integration, same account count
    integration_count_final = await conn.fetchval(
        """
        SELECT COUNT(*)
        FROM bank_integrations
        WHERE user_id = $1
            AND bank = 'monobank'
            AND status = 'active'
        """,
        user_id,
    )
    account_count_final = await conn.fetchval(
        "SELECT COUNT(*) FROM accounts WHERE user_id = $1",
        user_id,
    )
    assert integration_count_final == 1
    assert integration_count_after_first == 1
    assert account_count_final == account_count_after_first


# ---------------------------------------------------------------------------
# Case (b): different-client-id conflict
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_link_different_client_id_returns_409(
    conn: asyncpg.Connection,
    http_client: AsyncClient,
) -> None:
    """Linking a different Monobank account while one is already linked returns 409."""
    user_id = uuid4()
    await _insert_user(conn, user_id)
    token = _make_token(user_id)

    client_mock_a = _make_client_mock(_CLIENT_ID_A, [_MONO_ACCOUNT_1])
    client_mock_b = _make_client_mock(_CLIENT_ID_B, [_MONO_ACCOUNT_2])

    # Link client A first
    with patch(
        "grosh_ingestion.sources.monobank.linking_service.MonobankClient",
        return_value=client_mock_a,
    ):
        resp_a = await http_client.post(
            "/v1/monobank/link",
            json={"token": "fake-token-A"},
            headers={"Authorization": f"Bearer {token}"},
        )
        assert resp_a.status_code == 201, resp_a.text
        body_a = resp_a.json()
        original_integration_id = body_a["integration_id"]

    # Attempt to link client B — must be rejected
    with patch(
        "grosh_ingestion.sources.monobank.linking_service.MonobankClient",
        return_value=client_mock_b,
    ):
        resp_b = await http_client.post(
            "/v1/monobank/link",
            json={"token": "fake-token-B"},
            headers={"Authorization": f"Bearer {token}"},
        )
        assert resp_b.status_code == 409, resp_b.text
        body_b = resp_b.json()
        assert body_b["code"] == "INTEGRATION_ALREADY_LINKED"

    # DB: integration still belongs to client A
    row = await conn.fetchrow(
        """
        SELECT config->>'monobank_client_id' AS client_id
        FROM bank_integrations
        WHERE id = $1
        """,
        UUID(original_integration_id),
    )
    assert row is not None
    assert row["client_id"] == _CLIENT_ID_A


# ---------------------------------------------------------------------------
# Case (c): rebind continuity
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_link_rebind_continuity(
    conn: asyncpg.Connection,
    http_client: AsyncClient,
) -> None:
    """After DELETE + re-link with same client_id, accounts are rebound and
    historical transactions remain attached to the original account_id.
    """
    user_id = uuid4()
    await _insert_user(conn, user_id)
    token = _make_token(user_id)

    client_mock = _make_client_mock(_CLIENT_ID_A, [_MONO_ACCOUNT_1])

    # Step 1: initial link
    with patch(
        "grosh_ingestion.sources.monobank.linking_service.MonobankClient",
        return_value=client_mock,
    ):
        resp1 = await http_client.post(
            "/v1/monobank/link",
            json={"token": "fake-token-A"},
            headers={"Authorization": f"Bearer {token}"},
        )
        assert resp1.status_code == 201, resp1.text
        body1 = resp1.json()
        integration_id_v1 = UUID(body1["integration_id"])
        account_id_original = UUID(body1["accounts"][0]["account_id"])

    # Step 2: simulate a transaction landing on the account
    # RLS requires app.current_user_id to be set for INSERT into transactions.
    await conn.execute(
        "SELECT set_config('app.current_user_id', $1, true)",
        str(user_id),
    )
    tx_id = await _insert_minimal_transaction(conn, user_id, account_id_original)

    # Step 3: delete the integration
    with patch(
        "grosh_ingestion.sources.monobank.linking_service.MonobankClient",
        return_value=client_mock,
    ):
        resp_del = await http_client.delete(
            f"/v1/monobank/integrations/{integration_id_v1}",
            headers={"Authorization": f"Bearer {token}"},
        )
        assert resp_del.status_code == 204, resp_del.text

    # Confirm integration is gone and account is now orphaned (integration_id IS NULL)
    orphan_integration_id = await conn.fetchval(
        "SELECT integration_id FROM accounts WHERE id = $1",
        account_id_original,
    )
    assert orphan_integration_id is None

    # Step 4: re-link with the same client A
    with patch(
        "grosh_ingestion.sources.monobank.linking_service.MonobankClient",
        return_value=client_mock,
    ):
        resp2 = await http_client.post(
            "/v1/monobank/link",
            json={"token": "fake-token-A"},
            headers={"Authorization": f"Bearer {token}"},
        )
        assert resp2.status_code == 201, resp2.text
        body2 = resp2.json()
        integration_id_v2 = UUID(body2["integration_id"])

    # A new integration row was created (different id from v1)
    assert integration_id_v2 != integration_id_v1

    # All returned accounts must be flagged was_rebound=True
    assert len(body2["accounts"]) == 1
    for acc in body2["accounts"]:
        assert acc["was_rebound"] is True, f"Expected was_rebound=True for {acc}"

    # The original account_id UUID is unchanged (rebind = same row, new FK)
    rebound_account_ids = {UUID(a["account_id"]) for a in body2["accounts"]}
    assert account_id_original in rebound_account_ids

    # DB: account now points at the new integration
    new_integration_fk = await conn.fetchval(
        "SELECT integration_id FROM accounts WHERE id = $1",
        account_id_original,
    )
    assert new_integration_fk == integration_id_v2

    # The historical transaction is still attached to the original account_id
    tx_account_id = await conn.fetchval(
        "SELECT account_id FROM transactions WHERE id = $1",
        tx_id,
    )
    assert tx_account_id == account_id_original
