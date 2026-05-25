"""E2E tests for /v1/monobank/* and /monobank/webhook/* lifecycle endpoints.

Excludes /backfill — those require K8s. Excludes /link — that requires live
Monobank API. See test_webhook_pipeline.py for the webhook→consumer flow.
"""

import secrets

import httpx
import pytest

from helpers.factories import (
    build_bank_integration,
    build_seed_admin,
    build_user,
)
from helpers.http import INGESTION_BASE, bearer, log_in

pytestmark = pytest.mark.asyncio


async def test_list_integrations_empty(pg, client: httpx.AsyncClient) -> None:
    _, email, password = await build_seed_admin(pg)
    token = await log_in(client, email, password)
    r = await client.get(
        f"{INGESTION_BASE}/v1/monobank/integrations", headers=bearer(token)
    )
    assert r.status_code == 200
    assert r.json() == []


async def test_list_integrations_returns_user_integrations(
    pg, client: httpx.AsyncClient
) -> None:
    uid, email, password = await build_seed_admin(pg)
    await build_bank_integration(pg, user_id=uid)
    token = await log_in(client, email, password)
    r = await client.get(
        f"{INGESTION_BASE}/v1/monobank/integrations", headers=bearer(token)
    )
    assert r.status_code == 200
    assert len(r.json()) == 1


async def test_list_integrations_rls_isolation(pg, client: httpx.AsyncClient) -> None:
    admin_uid, _, _ = await build_seed_admin(pg)
    await build_bank_integration(pg, user_id=admin_uid)

    _, member_email, member_password = await build_user(pg)
    token = await log_in(client, member_email, member_password)
    r = await client.get(
        f"{INGESTION_BASE}/v1/monobank/integrations", headers=bearer(token)
    )
    assert r.status_code == 200
    assert r.json() == []


async def test_webhook_verify_get_with_known_secret(
    pg, client: httpx.AsyncClient
) -> None:
    uid, _, _ = await build_seed_admin(pg)
    _, secret = await build_bank_integration(pg, user_id=uid)
    r = await client.get(f"{INGESTION_BASE}/monobank/webhook/{secret}")
    assert r.status_code == 200


async def test_webhook_verify_get_with_unknown_secret_returns_200(
    client: httpx.AsyncClient,
) -> None:
    """The GET handler does not validate the secret — it just confirms reachability."""
    r = await client.get(f"{INGESTION_BASE}/monobank/webhook/{secrets.token_hex(16)}")
    assert r.status_code == 200


async def test_webhook_post_with_unknown_secret_returns_404(
    client: httpx.AsyncClient,
) -> None:
    r = await client.post(
        f"{INGESTION_BASE}/monobank/webhook/{secrets.token_hex(16)}",
        json={
            "type": "StatementItem",
            "data": {"account": "x", "statementItem": {}},
        },
    )
    assert r.status_code == 404
