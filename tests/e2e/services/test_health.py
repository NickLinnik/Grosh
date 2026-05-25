"""Smoke test — confirms session lifecycle works and services are reachable."""

import httpx
import pytest

from helpers.http import API_BASE, INGESTION_BASE

pytestmark = pytest.mark.asyncio


async def test_api_health(client: httpx.AsyncClient) -> None:
    r = await client.get(f"{API_BASE}/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}


async def test_ingestion_health(client: httpx.AsyncClient) -> None:
    r = await client.get(f"{INGESTION_BASE}/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}
