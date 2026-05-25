"""E2E tests for /v1/settings."""

import httpx
import pytest

from helpers.factories import build_seed_admin
from helpers.http import API_BASE, bearer, log_in

pytestmark = pytest.mark.asyncio


async def test_get_settings_returns_defaults_for_fresh_user(
    pg, client: httpx.AsyncClient
) -> None:
    _, email, password = await build_seed_admin(pg)
    token = await log_in(client, email, password)
    r = await client.get(f"{API_BASE}/v1/settings", headers=bearer(token))
    assert r.status_code == 200
    body = r.json()
    assert body["timezone"] == "UTC"
    assert body["default_rate_source"] is None


async def test_update_settings_persists_values(pg, client: httpx.AsyncClient) -> None:
    _, email, password = await build_seed_admin(pg)
    token = await log_in(client, email, password)
    r = await client.put(
        f"{API_BASE}/v1/settings",
        headers=bearer(token),
        json={"timezone": "Europe/Kyiv", "default_rate_source": "monobank"},
    )
    assert r.status_code == 200
    assert r.json()["timezone"] == "Europe/Kyiv"
    assert r.json()["default_rate_source"] == "monobank"

    # Re-read should return the same
    r2 = await client.get(f"{API_BASE}/v1/settings", headers=bearer(token))
    assert r2.json()["timezone"] == "Europe/Kyiv"
    assert r2.json()["default_rate_source"] == "monobank"


async def test_update_settings_rejects_invalid_timezone(
    pg, client: httpx.AsyncClient
) -> None:
    _, email, password = await build_seed_admin(pg)
    token = await log_in(client, email, password)
    r = await client.put(
        f"{API_BASE}/v1/settings",
        headers=bearer(token),
        json={"timezone": "Mars/Olympus"},
    )
    assert r.status_code == 422


async def test_update_settings_rejects_unknown_rate_source(
    pg, client: httpx.AsyncClient
) -> None:
    _, email, password = await build_seed_admin(pg)
    token = await log_in(client, email, password)
    r = await client.put(
        f"{API_BASE}/v1/settings",
        headers=bearer(token),
        json={"default_rate_source": "bitcoin-cabal"},
    )
    assert r.status_code == 422


async def test_get_settings_without_auth_returns_401(client: httpx.AsyncClient) -> None:
    r = await client.get(f"{API_BASE}/v1/settings")
    assert r.status_code == 401
