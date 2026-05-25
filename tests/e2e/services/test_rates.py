"""E2E tests for /v1/rates and /v1/rates/at."""

from datetime import UTC, datetime

import httpx
import pytest

from helpers.factories import build_rate, build_seed_admin
from helpers.http import API_BASE, bearer, log_in

pytestmark = pytest.mark.asyncio


async def test_list_rates_empty(pg, client: httpx.AsyncClient) -> None:
    _, email, password = await build_seed_admin(pg)
    token = await log_in(client, email, password)
    r = await client.get(f"{API_BASE}/v1/rates", headers=bearer(token))
    assert r.status_code == 200
    assert r.json()["total"] == 0


async def test_list_rates_returns_inserted(pg, client: httpx.AsyncClient) -> None:
    _, email, password = await build_seed_admin(pg)
    await build_rate(
        pg,
        currency_from="USD",
        currency_to="UAH",
        rate_mid=40.0,
        valid_from=datetime(2026, 5, 1, tzinfo=UTC),
        valid_to=datetime(2026, 5, 2, tzinfo=UTC),
    )
    token = await log_in(client, email, password)
    r = await client.get(f"{API_BASE}/v1/rates", headers=bearer(token))
    assert r.status_code == 200
    assert r.json()["total"] == 1


async def test_list_rates_filter_source(pg, client: httpx.AsyncClient) -> None:
    _, email, password = await build_seed_admin(pg)
    await build_rate(pg, source="nbu", rate_mid=40.0)
    await build_rate(pg, source="monobank", rate_mid=41.0)
    token = await log_in(client, email, password)
    r = await client.get(f"{API_BASE}/v1/rates?source=nbu", headers=bearer(token))
    assert r.status_code == 200
    assert r.json()["total"] == 1


async def test_list_rates_filter_currency(pg, client: httpx.AsyncClient) -> None:
    _, email, password = await build_seed_admin(pg)
    await build_rate(pg, currency_from="USD", currency_to="UAH", rate_mid=40.0)
    await build_rate(pg, currency_from="EUR", currency_to="UAH", rate_mid=45.0)
    token = await log_in(client, email, password)
    r = await client.get(
        f"{API_BASE}/v1/rates?currency_from=USD", headers=bearer(token)
    )
    assert r.status_code == 200
    items = r.json()["items"]
    assert len(items) == 1
    assert items[0]["currency_from"] == "USD"


async def test_rates_at_scd2_lookup(pg, client: httpx.AsyncClient) -> None:
    _, email, password = await build_seed_admin(pg)
    # Three SCD2 slices
    await build_rate(
        pg,
        source="nbu",
        currency_from="USD",
        currency_to="UAH",
        rate_mid=38.0,
        valid_from=datetime(2026, 4, 1, tzinfo=UTC),
        valid_to=datetime(2026, 4, 30, tzinfo=UTC),
    )
    await build_rate(
        pg,
        source="nbu",
        currency_from="USD",
        currency_to="UAH",
        rate_mid=40.0,
        valid_from=datetime(2026, 4, 30, tzinfo=UTC),
        valid_to=datetime(2026, 5, 31, tzinfo=UTC),
    )
    token = await log_in(client, email, password)
    # April 15 → first slice
    r1 = await client.get(
        f"{API_BASE}/v1/rates/at"
        "?source=nbu&currency_from=USD&currency_to=UAH&at=2026-04-15T00:00:00Z",
        headers=bearer(token),
    )
    assert r1.status_code == 200
    items1 = r1.json()
    assert len(items1) == 1
    assert items1[0]["rate_mid"] == "38.00000000"

    # May 15 → second slice
    r2 = await client.get(
        f"{API_BASE}/v1/rates/at"
        "?source=nbu&currency_from=USD&currency_to=UAH&at=2026-05-15T00:00:00Z",
        headers=bearer(token),
    )
    assert r2.status_code == 200
    items2 = r2.json()
    assert len(items2) == 1
    assert items2[0]["rate_mid"] == "40.00000000"


async def test_rates_at_half_open_boundary(pg, client: httpx.AsyncClient) -> None:
    """At the boundary (valid_to of one slice = valid_from of next), the new slice wins."""
    _, email, password = await build_seed_admin(pg)
    await build_rate(
        pg,
        source="nbu",
        currency_from="USD",
        currency_to="UAH",
        rate_mid=38.0,
        valid_from=datetime(2026, 4, 1, tzinfo=UTC),
        valid_to=datetime(2026, 5, 1, tzinfo=UTC),
    )
    await build_rate(
        pg,
        source="nbu",
        currency_from="USD",
        currency_to="UAH",
        rate_mid=40.0,
        valid_from=datetime(2026, 5, 1, tzinfo=UTC),
        valid_to=datetime(2026, 6, 1, tzinfo=UTC),
    )
    token = await log_in(client, email, password)
    # Exact boundary: 2026-05-01 00:00:00 → second slice (valid_from inclusive)
    r = await client.get(
        f"{API_BASE}/v1/rates/at"
        "?source=nbu&currency_from=USD&currency_to=UAH&at=2026-05-01T00:00:00Z",
        headers=bearer(token),
    )
    assert r.status_code == 200
    items = r.json()
    assert len(items) == 1
    assert items[0]["rate_mid"] == "40.00000000"


async def test_rates_at_before_any_data_returns_empty(
    pg, client: httpx.AsyncClient
) -> None:
    _, email, password = await build_seed_admin(pg)
    await build_rate(
        pg,
        source="nbu",
        rate_mid=40.0,
        valid_from=datetime(2026, 5, 1, tzinfo=UTC),
    )
    token = await log_in(client, email, password)
    r = await client.get(
        f"{API_BASE}/v1/rates/at"
        "?source=nbu&currency_from=USD&currency_to=UAH&at=1990-01-01T00:00:00Z",
        headers=bearer(token),
    )
    assert r.status_code == 200
    assert r.json() == []


async def test_rates_without_auth_returns_401(client: httpx.AsyncClient) -> None:
    r = await client.get(f"{API_BASE}/v1/rates")
    assert r.status_code == 401
