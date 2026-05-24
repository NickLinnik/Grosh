"""E2E tests for /v1/admin/users."""

from uuid import uuid4

import httpx
import pytest

from helpers.factories import build_seed_admin, build_user
from helpers.http import API_BASE, bearer, log_in

pytestmark = pytest.mark.asyncio


async def test_admin_creates_user(pg, client: httpx.AsyncClient) -> None:
    _, email, password = await build_seed_admin(pg)
    token = await log_in(client, email, password)
    r = await client.post(
        f"{API_BASE}/v1/admin/users",
        headers=bearer(token),
        json={
            "email": "newuser@example.com",
            "password": "NewUserPass123!",
            "display_name": "New User",
            "role": "member",
        },
    )
    assert r.status_code == 201
    body = r.json()
    assert body["email"] == "newuser@example.com"
    assert body["role"] == "member"


async def test_admin_creates_user_duplicate_email_rejected(
    pg, client: httpx.AsyncClient
) -> None:
    _, email, password = await build_seed_admin(pg)
    await build_user(pg, email="dup@example.com")
    token = await log_in(client, email, password)
    r = await client.post(
        f"{API_BASE}/v1/admin/users",
        headers=bearer(token),
        json={
            "email": "dup@example.com",
            "password": "AnotherPass123!",
            "display_name": "Dup",
            "role": "member",
        },
    )
    assert r.status_code in (409, 400)


async def test_admin_lists_users(pg, client: httpx.AsyncClient) -> None:
    _, email, password = await build_seed_admin(pg)
    await build_user(pg, email="a@example.com")
    await build_user(pg, email="b@example.com")
    token = await log_in(client, email, password)
    r = await client.get(f"{API_BASE}/v1/admin/users", headers=bearer(token))
    assert r.status_code == 200
    items = r.json()["items"]
    # admin (seed) + 2 created = 3
    assert len(items) == 3


async def test_non_admin_cannot_create_user(pg, client: httpx.AsyncClient) -> None:
    _, member_email, member_password = await build_user(pg, role="member")
    token = await log_in(client, member_email, member_password)
    r = await client.post(
        f"{API_BASE}/v1/admin/users",
        headers=bearer(token),
        json={
            "email": "nope@example.com",
            "password": "WontWork123!",
            "display_name": "Nope",
            "role": "member",
        },
    )
    assert r.status_code == 403


async def test_non_admin_cannot_list_users(pg, client: httpx.AsyncClient) -> None:
    _, member_email, member_password = await build_user(pg, role="member")
    token = await log_in(client, member_email, member_password)
    r = await client.get(f"{API_BASE}/v1/admin/users", headers=bearer(token))
    assert r.status_code == 403


async def test_admin_cannot_delete_self(pg, client: httpx.AsyncClient) -> None:
    uid, email, password = await build_seed_admin(pg)
    token = await log_in(client, email, password)
    r = await client.delete(f"{API_BASE}/v1/admin/users/{uid}", headers=bearer(token))
    assert r.status_code in (400, 403, 409)


async def test_admin_can_delete_other_user(pg, client: httpx.AsyncClient) -> None:
    _, email, password = await build_seed_admin(pg)
    other_uid, _, _ = await build_user(pg)
    token = await log_in(client, email, password)
    r = await client.delete(
        f"{API_BASE}/v1/admin/users/{other_uid}", headers=bearer(token)
    )
    assert r.status_code == 204


async def test_admin_delete_nonexistent_user_returns_404(
    pg, client: httpx.AsyncClient
) -> None:
    _, email, password = await build_seed_admin(pg)
    token = await log_in(client, email, password)
    r = await client.delete(
        f"{API_BASE}/v1/admin/users/{uuid4()}", headers=bearer(token)
    )
    assert r.status_code == 404
