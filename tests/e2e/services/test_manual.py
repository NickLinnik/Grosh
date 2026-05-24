"""E2E tests for /v1/manual/* endpoints."""

import secrets
from uuid import uuid4

import httpx
import pytest

from helpers.factories import build_account, build_seed_admin, build_user
from helpers.http import INGESTION_BASE, bearer, log_in

pytestmark = pytest.mark.asyncio


async def test_create_manual_cash_account(pg, client: httpx.AsyncClient) -> None:
    _, email, password = await build_seed_admin(pg)
    token = await log_in(client, email, password)
    r = await client.post(
        f"{INGESTION_BASE}/v1/manual/accounts",
        headers=bearer(token),
        json={"type": "cash", "currency_code": "USD", "name": "My USD"},
    )
    assert r.status_code == 201
    body = r.json()
    assert body["type"] == "cash"
    assert body["currency_code"] == "USD"
    assert body["name"] == "My USD"


async def test_create_manual_transaction_on_own_manual_account(
    pg, client: httpx.AsyncClient
) -> None:
    uid, email, password = await build_seed_admin(pg)
    aid = await build_account(
        pg,
        user_id=uid,
        source="manual",
        account_type="cash",
        currency_code="USD",
        name="Cash",
    )
    token = await log_in(client, email, password)
    r = await client.post(
        f"{INGESTION_BASE}/v1/manual/transactions",
        headers=bearer(token),
        json={
            "account_id": str(aid),
            "amount_cents": 1000,
            "operation_currency_code": "USD",
            "description": "test",
            "time": "2026-05-15T12:00:00Z",
            "direction": "income",
            "idempotency_key": f"e2e-{secrets.token_hex(8)}",
        },
    )
    assert r.status_code == 201


async def test_create_manual_transaction_on_bank_account_rejected(
    pg, client: httpx.AsyncClient
) -> None:
    """D-002: must reject manual tx on a Monobank account.

    Currently accepts it (returns 201). This test pins the fix.
    """
    uid, email, password = await build_seed_admin(pg)
    bank_account = await build_account(
        pg,
        user_id=uid,
        source="monobank",
        account_type="black",
        currency_code="UAH",
    )
    token = await log_in(client, email, password)
    r = await client.post(
        f"{INGESTION_BASE}/v1/manual/transactions",
        headers=bearer(token),
        json={
            "account_id": str(bank_account),
            "amount_cents": 1000,
            "operation_currency_code": "UAH",
            "description": "should not be allowed",
            "time": "2026-05-15T12:00:00Z",
            "direction": "income",
            "idempotency_key": f"e2e-{secrets.token_hex(8)}",
        },
    )
    assert (
        r.status_code in (400, 403, 422)
    ), f"manual tx on bank account should be rejected, got {r.status_code}: {r.text[:300]}"


async def test_update_manual_account_name(pg, client: httpx.AsyncClient) -> None:
    uid, email, password = await build_seed_admin(pg)
    aid = await build_account(
        pg,
        user_id=uid,
        source="manual",
        account_type="cash",
        currency_code="USD",
        name="Old Name",
    )
    token = await log_in(client, email, password)
    r = await client.put(
        f"{INGESTION_BASE}/v1/manual/accounts/{aid}",
        headers=bearer(token),
        json={"name": "New Name"},
    )
    assert r.status_code == 200
    assert r.json()["name"] == "New Name"


async def test_update_bank_account_via_manual_rejected(
    pg, client: httpx.AsyncClient
) -> None:
    uid, email, password = await build_seed_admin(pg)
    bank_account = await build_account(pg, user_id=uid, source="monobank")
    token = await log_in(client, email, password)
    r = await client.put(
        f"{INGESTION_BASE}/v1/manual/accounts/{bank_account}",
        headers=bearer(token),
        json={"name": "should fail"},
    )
    assert r.status_code in (403, 404)


async def test_member_cannot_update_admins_manual_account(
    pg, client: httpx.AsyncClient
) -> None:
    admin_uid, _, _ = await build_seed_admin(pg)
    admin_account = await build_account(
        pg,
        user_id=admin_uid,
        source="manual",
        account_type="cash",
        currency_code="USD",
        name="Admins",
    )
    _, member_email, member_password = await build_user(pg)
    token = await log_in(client, member_email, member_password)
    r = await client.put(
        f"{INGESTION_BASE}/v1/manual/accounts/{admin_account}",
        headers=bearer(token),
        json={"name": "Hijacked"},
    )
    assert r.status_code == 404


async def test_delete_manual_account(pg, client: httpx.AsyncClient) -> None:
    uid, email, password = await build_seed_admin(pg)
    aid = await build_account(
        pg,
        user_id=uid,
        source="manual",
        account_type="cash",
        currency_code="USD",
        name="ToDelete",
    )
    token = await log_in(client, email, password)
    r = await client.delete(
        f"{INGESTION_BASE}/v1/manual/accounts/{aid}", headers=bearer(token)
    )
    assert r.status_code == 204


async def test_delete_nonexistent_manual_account_returns_404(
    pg, client: httpx.AsyncClient
) -> None:
    _, email, password = await build_seed_admin(pg)
    token = await log_in(client, email, password)
    r = await client.delete(
        f"{INGESTION_BASE}/v1/manual/accounts/{uuid4()}", headers=bearer(token)
    )
    assert r.status_code == 404
