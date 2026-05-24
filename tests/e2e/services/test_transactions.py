"""E2E tests for /v1/transactions and /v1/transactions/aggregates."""

from datetime import UTC, datetime

import httpx
import pytest

from helpers.factories import (
    build_account,
    build_seed_admin,
    build_transaction,
    build_user,
)
from helpers.http import API_BASE, bearer, log_in

pytestmark = pytest.mark.asyncio


async def test_list_transactions_empty(pg, client: httpx.AsyncClient) -> None:
    _, email, password = await build_seed_admin(pg)
    token = await log_in(client, email, password)
    r = await client.get(f"{API_BASE}/v1/transactions", headers=bearer(token))
    assert r.status_code == 200
    body = r.json()
    assert body["items"] == []
    assert body["total"] == 0


async def test_list_transactions_returns_user_data(
    pg, client: httpx.AsyncClient
) -> None:
    uid, email, password = await build_seed_admin(pg)
    aid = await build_account(pg, user_id=uid)
    for i in range(3):
        await build_transaction(
            pg,
            user_id=uid,
            account_id=aid,
            time=datetime(2026, 5, i + 1, 12, 0, tzinfo=UTC),
            source_id=f"tx-{i}",
        )
    token = await log_in(client, email, password)
    r = await client.get(f"{API_BASE}/v1/transactions", headers=bearer(token))
    assert r.status_code == 200
    body = r.json()
    assert body["total"] == 3
    assert len(body["items"]) == 3


async def test_list_transactions_limit(pg, client: httpx.AsyncClient) -> None:
    uid, email, password = await build_seed_admin(pg)
    aid = await build_account(pg, user_id=uid)
    for i in range(5):
        await build_transaction(
            pg,
            user_id=uid,
            account_id=aid,
            time=datetime(2026, 5, i + 1, 12, 0, tzinfo=UTC),
            source_id=f"tx-{i}",
        )
    token = await log_in(client, email, password)
    r = await client.get(f"{API_BASE}/v1/transactions?limit=2", headers=bearer(token))
    assert r.status_code == 200
    body = r.json()
    assert len(body["items"]) == 2
    assert body["next_cursor"] is not None


async def test_list_transactions_half_open_time_range(
    pg, client: httpx.AsyncClient
) -> None:
    uid, email, password = await build_seed_admin(pg)
    aid = await build_account(pg, user_id=uid)
    # 1 tx inside, 1 at the lower bound (included), 1 at the upper bound (excluded)
    await build_transaction(
        pg,
        user_id=uid,
        account_id=aid,
        time=datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
        source_id="at-from",
    )
    await build_transaction(
        pg,
        user_id=uid,
        account_id=aid,
        time=datetime(2026, 5, 15, 12, 0, tzinfo=UTC),
        source_id="middle",
    )
    await build_transaction(
        pg,
        user_id=uid,
        account_id=aid,
        time=datetime(2026, 6, 1, 0, 0, tzinfo=UTC),
        source_id="at-to",
    )
    token = await log_in(client, email, password)
    r = await client.get(
        f"{API_BASE}/v1/transactions"
        "?from=2026-05-01T00:00:00Z&to=2026-06-01T00:00:00Z",
        headers=bearer(token),
    )
    assert r.status_code == 200
    body = r.json()
    assert body["total"] == 2  # at-from included, at-to excluded


async def test_list_transactions_account_filter(pg, client: httpx.AsyncClient) -> None:
    uid, email, password = await build_seed_admin(pg)
    a1 = await build_account(pg, user_id=uid, currency_code="UAH")
    a2 = await build_account(pg, user_id=uid, currency_code="USD")
    await build_transaction(pg, user_id=uid, account_id=a1, source_id="a1-tx")
    await build_transaction(pg, user_id=uid, account_id=a2, source_id="a2-tx")
    token = await log_in(client, email, password)
    r = await client.get(
        f"{API_BASE}/v1/transactions?account_id={a1}", headers=bearer(token)
    )
    assert r.status_code == 200
    body = r.json()
    assert body["total"] == 1
    assert body["items"][0]["account_id"] == str(a1)


async def test_list_transactions_exclude_category_transfer(
    pg, client: httpx.AsyncClient
) -> None:
    uid, email, password = await build_seed_admin(pg)
    aid = await build_account(pg, user_id=uid)
    await build_transaction(pg, user_id=uid, account_id=aid, source_id="ordinary")
    await build_transaction(
        pg,
        user_id=uid,
        account_id=aid,
        source_id="transfer-out",
        special_category="transfer",
    )
    token = await log_in(client, email, password)
    r = await client.get(
        f"{API_BASE}/v1/transactions?exclude_category=transfer", headers=bearer(token)
    )
    assert r.status_code == 200
    body = r.json()
    assert body["total"] == 1
    assert body["items"][0]["special_category"] is None


async def test_list_transactions_only_category_transfer(
    pg, client: httpx.AsyncClient
) -> None:
    uid, email, password = await build_seed_admin(pg)
    aid = await build_account(pg, user_id=uid)
    await build_transaction(pg, user_id=uid, account_id=aid, source_id="ordinary")
    await build_transaction(
        pg,
        user_id=uid,
        account_id=aid,
        source_id="transfer",
        special_category="transfer",
    )
    token = await log_in(client, email, password)
    r = await client.get(
        f"{API_BASE}/v1/transactions?category=transfer", headers=bearer(token)
    )
    assert r.status_code == 200
    assert r.json()["total"] == 1


async def test_list_transactions_conflicting_category_filters_returns_422(
    pg, client: httpx.AsyncClient
) -> None:
    _, email, password = await build_seed_admin(pg)
    token = await log_in(client, email, password)
    r = await client.get(
        f"{API_BASE}/v1/transactions?category=transfer&exclude_category=transfer",
        headers=bearer(token),
    )
    assert r.status_code == 422


async def test_list_transactions_bad_cursor_returns_400(
    pg, client: httpx.AsyncClient
) -> None:
    _, email, password = await build_seed_admin(pg)
    token = await log_in(client, email, password)
    r = await client.get(
        f"{API_BASE}/v1/transactions?cursor=NOTBASE64", headers=bearer(token)
    )
    assert r.status_code == 400


async def test_list_transactions_rls_isolation(pg, client: httpx.AsyncClient) -> None:
    """Member cannot see admin's transactions, even via account_id filter."""
    admin_uid, admin_email, admin_password = await build_seed_admin(pg)
    admin_acc = await build_account(pg, user_id=admin_uid)
    await build_transaction(
        pg, user_id=admin_uid, account_id=admin_acc, source_id="adm"
    )

    member_uid, member_email, member_password = await build_user(pg)
    token = await log_in(client, member_email, member_password)

    # No filter: empty
    r = await client.get(f"{API_BASE}/v1/transactions", headers=bearer(token))
    assert r.status_code == 200
    assert r.json()["total"] == 0

    # With admin's account_id: still empty (RLS scopes BEFORE the filter)
    r2 = await client.get(
        f"{API_BASE}/v1/transactions?account_id={admin_acc}", headers=bearer(token)
    )
    assert r2.status_code == 200
    assert r2.json()["total"] == 0


async def test_aggregates_default_month(pg, client: httpx.AsyncClient) -> None:
    uid, email, password = await build_seed_admin(pg)
    aid = await build_account(pg, user_id=uid)
    await build_transaction(
        pg,
        user_id=uid,
        account_id=aid,
        amount_cents=-1000,
        direction="expense",
        time=datetime(2026, 5, 15, tzinfo=UTC),
        source_id="t1",
    )
    await build_transaction(
        pg,
        user_id=uid,
        account_id=aid,
        amount_cents=5000,
        direction="income",
        time=datetime(2026, 5, 20, tzinfo=UTC),
        source_id="t2",
    )
    token = await log_in(client, email, password)
    r = await client.get(
        f"{API_BASE}/v1/transactions/aggregates", headers=bearer(token)
    )
    assert r.status_code == 200
    body = r.json()
    assert body["bucket"] == "month"


async def test_aggregates_day_bucket_with_range(pg, client: httpx.AsyncClient) -> None:
    _, email, password = await build_seed_admin(pg)
    token = await log_in(client, email, password)
    r = await client.get(
        f"{API_BASE}/v1/transactions/aggregates"
        "?bucket=day&from=2026-05-01T00:00:00Z&to=2026-05-02T00:00:00Z",
        headers=bearer(token),
    )
    assert r.status_code == 200
