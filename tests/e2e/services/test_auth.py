"""E2E tests for /v1/auth/* endpoints."""

import httpx
import pytest

from helpers.factories import build_seed_admin
from helpers.http import API_BASE, bearer, log_in

pytestmark = pytest.mark.asyncio


async def test_login_with_correct_credentials_returns_token(
    pg, client: httpx.AsyncClient
) -> None:
    _, email, password = await build_seed_admin(pg)
    r = await client.post(
        f"{API_BASE}/v1/auth/login", json={"email": email, "password": password}
    )
    assert r.status_code == 200
    body = r.json()
    assert "access_token" in body
    assert body["token_type"] == "bearer"


async def test_login_with_wrong_password_returns_401(
    pg, client: httpx.AsyncClient
) -> None:
    _, email, _ = await build_seed_admin(pg)
    r = await client.post(
        f"{API_BASE}/v1/auth/login", json={"email": email, "password": "wrong"}
    )
    assert r.status_code == 401


async def test_login_with_unknown_email_returns_401(client: httpx.AsyncClient) -> None:
    r = await client.post(
        f"{API_BASE}/v1/auth/login",
        json={"email": "nobody@example.com", "password": "irrelevant"},
    )
    assert r.status_code == 401


async def test_login_with_missing_password_returns_422(
    client: httpx.AsyncClient,
) -> None:
    r = await client.post(f"{API_BASE}/v1/auth/login", json={"email": "x@y.com"})
    assert r.status_code == 422


async def test_me_with_valid_token_returns_user(pg, client: httpx.AsyncClient) -> None:
    uid, email, password = await build_seed_admin(pg)
    token = await log_in(client, email, password)
    r = await client.get(f"{API_BASE}/v1/auth/me", headers=bearer(token))
    assert r.status_code == 200
    body = r.json()
    assert body["id"] == str(uid)
    assert body["email"] == email
    assert body["role"] == "admin"


async def test_me_without_token_returns_401(client: httpx.AsyncClient) -> None:
    r = await client.get(f"{API_BASE}/v1/auth/me")
    assert r.status_code == 401


async def test_me_with_garbage_token_returns_401(client: httpx.AsyncClient) -> None:
    r = await client.get(
        f"{API_BASE}/v1/auth/me", headers=bearer("garbage.token.value")
    )
    assert r.status_code == 401


async def test_refresh_with_valid_cookie_returns_new_token(
    pg, client: httpx.AsyncClient
) -> None:
    _, email, password = await build_seed_admin(pg)
    login_resp = await client.post(
        f"{API_BASE}/v1/auth/login", json={"email": email, "password": password}
    )
    # The cookie has Secure flag; httpx.AsyncClient's cookie jar won't send it
    # over http://. Extract the value and re-send it explicitly.
    cookie_value = None
    for cookie in login_resp.cookies.jar:
        if cookie.name == "refresh_token":
            cookie_value = cookie.value
    assert cookie_value is not None

    r = await client.post(
        f"{API_BASE}/v1/auth/refresh", cookies={"refresh_token": cookie_value}
    )
    assert r.status_code == 200
    assert "access_token" in r.json()


async def test_refresh_without_cookie_returns_401(client: httpx.AsyncClient) -> None:
    r = await client.post(f"{API_BASE}/v1/auth/refresh")
    assert r.status_code == 401


async def test_logout_returns_204(pg, client: httpx.AsyncClient) -> None:
    _, email, password = await build_seed_admin(pg)
    token = await log_in(client, email, password)
    r = await client.post(f"{API_BASE}/v1/auth/logout", headers=bearer(token))
    assert r.status_code == 204


async def test_logout_is_idempotent(pg, client: httpx.AsyncClient) -> None:
    """D-001: calling /v1/auth/logout twice MUST be idempotent.

    Currently the second call returns 500 due to UniqueViolationError on
    revoked_tokens.jti. Once fixed, this test pins the contract.
    """
    _, email, password = await build_seed_admin(pg)
    token = await log_in(client, email, password)
    r1 = await client.post(f"{API_BASE}/v1/auth/logout", headers=bearer(token))
    assert r1.status_code == 204
    r2 = await client.post(f"{API_BASE}/v1/auth/logout", headers=bearer(token))
    assert (
        r2.status_code == 204
    ), f"Second logout returned {r2.status_code}; expected idempotent 204. Body: {r2.text}"


async def test_logout_all_returns_204(pg, client: httpx.AsyncClient) -> None:
    _, email, password = await build_seed_admin(pg)
    token = await log_in(client, email, password)
    r = await client.post(f"{API_BASE}/v1/auth/logout-all", headers=bearer(token))
    assert r.status_code == 204
