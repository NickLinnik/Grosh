"""Integration tests for MonobankLinkingService.reregister_webhooks.

Cases:
  (a) Happy path — DB config updated with new webhook_secret/url AND client.set_webhook
      called once with the new URL.
  (b) Client failure — set_webhook raises; result status='webhook_failed', BUT DB config
      already shows the new webhook_url (DB-before-network ordering invariant).
  (c) Non-monobank rows skipped — list_active returns a mix; only the monobank row
      produces a result entry.
  (d) Decrypt failure — decrypt_token_value returns None (mocked); result has
      status='error', detail='token decryption failed', no client calls.

Postgres is real (session-scoped db_pool). MonobankClient is stubbed via monkeypatch
at the import site: grosh_ingestion.sources.monobank.linking_service.MonobankClient.
"""

import json
import os
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID, uuid4

import asyncpg
import pytest

from grosh_ingestion.repositories.account_repo import AccountRepo
from grosh_ingestion.repositories.integration_repo import IntegrationRepo
from grosh_ingestion.sources.monobank.linking_service import MonobankLinkingService
from grosh_ingestion.sources.monobank.repo import MonobankRepo

_ENCRYPTION_KEY = os.environ.get("ENCRYPTION_KEY", "test-encryption-key-reregister")
_WEBHOOK_BASE_URL = "https://new.example.com"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _insert_user(conn: asyncpg.Connection, user_id: UUID) -> None:
    await conn.execute(
        """
        INSERT INTO users
            (id, email, password_hash, display_name, role)
        VALUES
            ($1, $2, 'x', 'Test User', 'member')
        """,
        user_id,
        f"reregister-{user_id}@example.com",
    )


async def _insert_monobank_integration(
    conn: asyncpg.Connection,
    user_id: UUID,
    *,
    webhook_secret: str = "oldsecret0000000000001",
    webhook_url: str = "https://old.example.com/monobank/webhook/oldsecret0000000000001",
    plaintext_token: str = "real-monobank-token",
    encryption_key: str = _ENCRYPTION_KEY,
) -> UUID:
    """Seed a Monobank integration row with a properly encrypted token."""
    # Encrypt the token via pgcrypto so decrypt_token_value can recover it.
    ciphertext_bytes = await conn.fetchval(
        "SELECT pgp_sym_encrypt($1, $2)",
        plaintext_token,
        encryption_key,
    )
    encrypted_token_hex = bytes(ciphertext_bytes).hex()

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
                "encrypted_token": encrypted_token_hex,
                "key_version": 1,
                "webhook_secret": webhook_secret,
                "webhook_url": webhook_url,
                "monobank_client_id": f"client-{user_id}",
            }
        ),
    )
    return row["id"]


def _make_client_stub(*, raises_on_set_webhook: bool = False) -> MagicMock:
    """Return an async-context-manager stub for MonobankClient."""
    stub = AsyncMock()
    if raises_on_set_webhook:
        stub.set_webhook = AsyncMock(side_effect=Exception("Monobank network error"))
    else:
        stub.set_webhook = AsyncMock()
    stub.__aenter__ = AsyncMock(return_value=stub)
    stub.__aexit__ = AsyncMock(return_value=False)
    return stub


def _make_service() -> MonobankLinkingService:
    return MonobankLinkingService(
        integration_repo=IntegrationRepo(),
        account_repo=AccountRepo(),
        monobank_repo=MonobankRepo(),
    )


# ---------------------------------------------------------------------------
# (a) Happy path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_reregister_happy_path_updates_db_and_calls_client(
    conn: asyncpg.Connection,
) -> None:
    """DB config gets a new webhook_secret/url AND client.set_webhook called once."""
    user_id = uuid4()
    await _insert_user(conn, user_id)
    integration_id = await _insert_monobank_integration(conn, user_id)

    stub = _make_client_stub()

    with patch(
        "grosh_ingestion.sources.monobank.linking_service.MonobankClient",
        return_value=stub,
    ):
        service = _make_service()
        results = await service.reregister_webhooks(
            conn,
            webhook_base_url=_WEBHOOK_BASE_URL,
            encryption_key=_ENCRYPTION_KEY,
        )

    # Exactly one result entry for the one monobank integration.
    assert len(results) == 1
    result = results[0]
    assert result["integration_id"] == integration_id
    assert result["status"] == "ok"
    new_webhook_url: str = result["webhook_url"]
    # Webhook router is mounted unversioned (no `/v1` prefix) per Slice 20.
    # Production constructs the URL as f"{base}/monobank/webhook/{secret}".
    assert new_webhook_url.startswith(f"{_WEBHOOK_BASE_URL}/monobank/webhook/")

    # DB config must reflect the new secret and url.
    row = await conn.fetchrow(
        "SELECT config FROM bank_integrations WHERE id = $1",
        integration_id,
    )
    assert row is not None
    config = (
        json.loads(row["config"])
        if isinstance(row["config"], str)
        else dict(row["config"])
    )
    assert config["webhook_url"] == new_webhook_url
    # The new secret must be embedded in the url.
    stored_secret = config["webhook_secret"]
    assert stored_secret in new_webhook_url

    # Client's set_webhook must have been called once with the new url.
    stub.set_webhook.assert_awaited_once_with(new_webhook_url)


# ---------------------------------------------------------------------------
# (b) Client failure — DB updated before network (invariant)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_reregister_client_failure_still_updates_db(
    conn: asyncpg.Connection,
) -> None:
    """set_webhook raises → result status='webhook_failed', DB already shows new url.

    This tests the DB-before-network ordering invariant: the config is written
    before the Monobank API call, so even a network failure leaves the DB in a
    consistent state with the new secret ready for a future retry.
    """
    user_id = uuid4()
    await _insert_user(conn, user_id)
    integration_id = await _insert_monobank_integration(
        conn,
        user_id,
        webhook_secret="oldsecret0000000000002",
        webhook_url="https://old.example.com/monobank/webhook/oldsecret0000000000002",
    )

    stub = _make_client_stub(raises_on_set_webhook=True)

    with patch(
        "grosh_ingestion.sources.monobank.linking_service.MonobankClient",
        return_value=stub,
    ):
        service = _make_service()
        results = await service.reregister_webhooks(
            conn,
            webhook_base_url=_WEBHOOK_BASE_URL,
            encryption_key=_ENCRYPTION_KEY,
        )

    assert len(results) == 1
    result = results[0]
    assert result["integration_id"] == integration_id
    assert result["status"] == "webhook_failed"
    failed_webhook_url: str = result["webhook_url"]
    assert failed_webhook_url.startswith(f"{_WEBHOOK_BASE_URL}/monobank/webhook/")

    # DB-before-network invariant: config ALREADY has the new url even though
    # the Monobank API call failed.
    row = await conn.fetchrow(
        "SELECT config FROM bank_integrations WHERE id = $1",
        integration_id,
    )
    assert row is not None
    config = (
        json.loads(row["config"])
        if isinstance(row["config"], str)
        else dict(row["config"])
    )
    assert config["webhook_url"] == failed_webhook_url
    new_secret = config["webhook_secret"]
    assert new_secret not in ("oldsecret0000000000002",)  # old secret must be replaced


# ---------------------------------------------------------------------------
# (c) Non-monobank rows skipped
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_reregister_skips_non_monobank_rows(
    conn: asyncpg.Connection,
) -> None:
    """Rows with bank != 'monobank' returned by list_active are silently skipped.

    The bank_source DB enum currently only contains 'monobank', so we cannot seed
    a non-monobank row directly. Instead we patch IntegrationRepo.list_active to
    inject a fake non-monobank record alongside the real monobank one, exercising
    the filtering branch in reregister_webhooks without a schema change.
    """
    user_id = uuid4()
    await _insert_user(conn, user_id)
    integration_id = await _insert_monobank_integration(
        conn,
        user_id,
        webhook_secret="oldsecret0000000000003",
        webhook_url="https://old.example.com/monobank/webhook/oldsecret0000000000003",
    )

    # Fetch the real monobank row so we can inject it alongside a fake one.
    real_row = await conn.fetchrow(
        "SELECT id, user_id, bank, config FROM bank_integrations WHERE id = $1",
        integration_id,
    )
    assert real_row is not None

    # Build a fake non-monobank record that list_active might return in future.
    fake_non_mono: dict[str, Any] = {
        "id": uuid4(),
        "user_id": user_id,
        "bank": "pumb",  # not in enum today — injected as a raw dict to bypass DB
        "config": json.dumps({"encrypted_token": "irrelevant"}),
    }

    stub = _make_client_stub()

    with (
        patch(
            "grosh_ingestion.sources.monobank.linking_service.MonobankClient",
            return_value=stub,
        ),
        patch.object(
            IntegrationRepo,
            "list_active",
            new_callable=AsyncMock,
            return_value=[real_row, fake_non_mono],
        ),
    ):
        service = _make_service()
        results = await service.reregister_webhooks(
            conn,
            webhook_base_url=_WEBHOOK_BASE_URL,
            encryption_key=_ENCRYPTION_KEY,
        )

    # Only the monobank row should produce a result; the fake 'pumb' row is skipped.
    assert len(results) == 1
    assert results[0]["integration_id"] == integration_id
    assert results[0]["status"] == "ok"


# ---------------------------------------------------------------------------
# (d) Decrypt failure
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_reregister_decrypt_failure_returns_error_entry(
    conn: asyncpg.Connection,
) -> None:
    """decrypt_token_value returns None → status='error',
    detail='token decryption failed', no client calls.

    In production, decrypt_token_value raises a PostgresError on bad ciphertext rather
    than returning None (as confirmed by test_monobank_repo.py). The None-returning
    path is a defensive guard in the service. We patch the repo method to return None
    to exercise that guard without relying on production cryptographic failure modes.
    """
    user_id = uuid4()
    await _insert_user(conn, user_id)
    integration_id = await _insert_monobank_integration(conn, user_id)

    stub = _make_client_stub()

    with (
        patch(
            "grosh_ingestion.sources.monobank.linking_service.MonobankClient",
            return_value=stub,
        ),
        patch.object(
            MonobankRepo,
            "decrypt_token_value",
            new_callable=AsyncMock,
            return_value=None,
        ),
    ):
        service = _make_service()
        results = await service.reregister_webhooks(
            conn,
            webhook_base_url=_WEBHOOK_BASE_URL,
            encryption_key=_ENCRYPTION_KEY,
        )

    assert len(results) == 1
    result = results[0]
    assert result["integration_id"] == integration_id
    assert result["status"] == "error"
    assert result["detail"] == "token decryption failed"

    # No client calls should have been made.
    stub.set_webhook.assert_not_awaited()
