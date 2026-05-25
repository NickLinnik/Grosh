"""E2E tests for /v1/accounts and /v1/accounts/{id}."""

from uuid import uuid4

import httpx
import pytest

from helpers.factories import build_account, build_seed_admin, build_user
from helpers.http import API_BASE, bearer, log_in

pytestmark = pytest.mark.asyncio


async def test_list_accounts_empty(pg, client: httpx.AsyncClient) -> None:
    _, email, password = await build_seed_admin(pg)
    token = await log_in(client, email, password)
    r = await client.get(f"{API_BASE}/v1/accounts", headers=bearer(token))
    assert r.status_code == 200
    assert r.json() == []


async def test_list_accounts_returns_user_accounts(
    pg, client: httpx.AsyncClient
) -> None:
    uid, email, password = await build_seed_admin(pg)
    await build_account(pg, user_id=uid, currency_code="UAH", name="UAH Cash")
    await build_account(pg, user_id=uid, currency_code="USD", name="USD Card")
    token = await log_in(client, email, password)
    r = await client.get(f"{API_BASE}/v1/accounts", headers=bearer(token))
    assert r.status_code == 200
    assert len(r.json()) == 2


async def test_list_accounts_filter_by_currency(pg, client: httpx.AsyncClient) -> None:
    uid, email, password = await build_seed_admin(pg)
    await build_account(pg, user_id=uid, currency_code="UAH")
    await build_account(pg, user_id=uid, currency_code="USD")
    token = await log_in(client, email, password)
    r = await client.get(
        f"{API_BASE}/v1/accounts?currency_code=USD", headers=bearer(token)
    )
    assert r.status_code == 200
    items = r.json()
    assert len(items) == 1
    assert items[0]["currency_code"] == "USD"


async def test_list_accounts_filter_by_source(pg, client: httpx.AsyncClient) -> None:
    uid, email, password = await build_seed_admin(pg)
    await build_account(pg, user_id=uid, source="monobank")
    await build_account(pg, user_id=uid, source="manual", account_type="cash")
    token = await log_in(client, email, password)
    r = await client.get(f"{API_BASE}/v1/accounts?source=manual", headers=bearer(token))
    assert r.status_code == 200
    items = r.json()
    assert len(items) == 1
    assert items[0]["source"] == "manual"


async def test_get_account_by_id(pg, client: httpx.AsyncClient) -> None:
    uid, email, password = await build_seed_admin(pg)
    aid = await build_account(pg, user_id=uid)
    token = await log_in(client, email, password)
    r = await client.get(f"{API_BASE}/v1/accounts/{aid}", headers=bearer(token))
    assert r.status_code == 200
    assert r.json()["id"] == str(aid)


async def test_get_account_nonexistent_returns_404(
    pg, client: httpx.AsyncClient
) -> None:
    _, email, password = await build_seed_admin(pg)
    token = await log_in(client, email, password)
    r = await client.get(f"{API_BASE}/v1/accounts/{uuid4()}", headers=bearer(token))
    assert r.status_code == 404


async def test_get_other_users_account_returns_404_not_403(
    pg, client: httpx.AsyncClient
) -> None:
    """RLS isolation: other user's account must look like it doesn't exist."""
    admin_uid, admin_email, admin_password = await build_seed_admin(pg)
    other_uid, other_email, other_password = await build_user(pg)
    other_account = await build_account(pg, user_id=other_uid)

    token = await log_in(client, admin_email, admin_password)
    r = await client.get(
        f"{API_BASE}/v1/accounts/{other_account}", headers=bearer(token)
    )
    # 404 (info leak guard), not 403 — admin must not learn the account exists
    assert r.status_code == 404


async def test_list_accounts_does_not_leak_other_users(
    pg, client: httpx.AsyncClient
) -> None:
    admin_uid, admin_email, admin_password = await build_seed_admin(pg)
    other_uid, _, _ = await build_user(pg)
    await build_account(pg, user_id=other_uid, currency_code="EUR", name="Other's")
    # admin has no accounts of their own
    token = await log_in(client, admin_email, admin_password)
    r = await client.get(f"{API_BASE}/v1/accounts", headers=bearer(token))
    assert r.status_code == 200
    assert r.json() == []


async def test_list_accounts_without_auth_returns_401(
    client: httpx.AsyncClient,
) -> None:
    r = await client.get(f"{API_BASE}/v1/accounts")
    assert r.status_code == 401
