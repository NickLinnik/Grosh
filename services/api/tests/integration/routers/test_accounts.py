"""Integration tests for GET /accounts and GET /accounts/{id}."""

import os
import uuid

import asyncpg
import pytest
from httpx import AsyncClient


async def _get_admin_id(conn: asyncpg.Connection) -> uuid.UUID:
    email = os.environ["ADMIN_EMAIL"].strip().lower()
    return await conn.fetchval(  # type: ignore[return-value]
        "SELECT id FROM users WHERE email = $1", email
    )


async def _set_rls(conn: asyncpg.Connection, user_id: uuid.UUID) -> None:
    await conn.execute(
        "SELECT set_config('app.current_user_id', $1::text, true)",
        str(user_id),
    )


async def _seed_account(
    conn: asyncpg.Connection,
    user_id: uuid.UUID,
    *,
    source: str = "manual",
    account_type: str = "cash",
    currency_code: str = "UAH",
    name: str = "Test Account",
) -> uuid.UUID:
    account_id: uuid.UUID = await conn.fetchval(
        """
        INSERT INTO accounts (user_id, source, type, currency_code, name)
        VALUES ($1, $2, $3, $4, $5)
        RETURNING id
        """,
        user_id,
        source,
        account_type,
        currency_code,
        name,
    )
    return account_id


# ---------------------------------------------------------------------------
# GET /accounts — list
# ---------------------------------------------------------------------------


@pytest.fixture
async def account_ids(conn: asyncpg.Connection) -> dict[str, uuid.UUID]:
    """Seed two accounts for the admin user with different currencies."""
    user_id = await _get_admin_id(conn)
    await _set_rls(conn, user_id)
    uah_id = await _seed_account(conn, user_id, currency_code="UAH", name="UAH Cash")
    usd_id = await _seed_account(conn, user_id, currency_code="USD", name="USD Savings")
    return {"uah": uah_id, "usd": usd_id, "user_id": user_id}


async def test_list_accounts_returns_seeded_rows(
    account_ids: dict[str, uuid.UUID],
    client: AsyncClient,
    admin_token: str,
) -> None:
    """list_by_user SQL returns correct rows — prevents column alias regression."""
    resp = await client.get(
        "/v1/accounts", headers={"Authorization": f"Bearer {admin_token}"}
    )

    assert resp.status_code == 200
    items = resp.json()
    returned_ids = {uuid.UUID(item["id"]) for item in items}
    assert account_ids["uah"] in returned_ids
    assert account_ids["usd"] in returned_ids


async def test_list_accounts_currency_filter(
    account_ids: dict[str, uuid.UUID],
    client: AsyncClient,
    admin_token: str,
) -> None:
    """currency_code filter passes through to SQL.

    Wrong filter wiring returns rows for the unfiltered currencies.
    """
    resp = await client.get(
        "/v1/accounts?currency_code=USD",
        headers={"Authorization": f"Bearer {admin_token}"},
    )

    assert resp.status_code == 200
    items = resp.json()
    returned_ids = {uuid.UUID(item["id"]) for item in items}
    assert account_ids["usd"] in returned_ids
    assert account_ids["uah"] not in returned_ids


async def test_list_accounts_name_filter(
    account_ids: dict[str, uuid.UUID],
    client: AsyncClient,
    admin_token: str,
) -> None:
    """name ILIKE filter works — ensures partial-match logic is wired correctly."""
    resp = await client.get(
        "/v1/accounts?name=UAH",
        headers={"Authorization": f"Bearer {admin_token}"},
    )

    assert resp.status_code == 200
    items = resp.json()
    returned_ids = {uuid.UUID(item["id"]) for item in items}
    assert account_ids["uah"] in returned_ids
    assert account_ids["usd"] not in returned_ids


# ---------------------------------------------------------------------------
# GET /accounts/{id} — single account
# ---------------------------------------------------------------------------


async def test_get_account_returns_correct_fields(
    account_ids: dict[str, uuid.UUID],
    client: AsyncClient,
    admin_token: str,
) -> None:
    """get_by_id SQL returns all required fields.

    Prevents regression where a new column is added to the model
    but the SELECT projection misses it.
    """
    resp = await client.get(
        f"/v1/accounts/{account_ids['uah']}",
        headers={"Authorization": f"Bearer {admin_token}"},
    )

    assert resp.status_code == 200
    body = resp.json()
    assert uuid.UUID(body["id"]) == account_ids["uah"]
    assert body["currency_code"] == "UAH"
    assert body["name"] == "UAH Cash"
    assert body["source"] == "manual"
    assert body["type"] == "cash"


async def test_get_account_not_found_returns_404(
    client: AsyncClient,
    admin_token: str,
) -> None:
    """Missing account returns 404 — prevents NoneType crash on absent row."""
    resp = await client.get(
        f"/v1/accounts/{uuid.uuid4()}",
        headers={"Authorization": f"Bearer {admin_token}"},
    )

    assert resp.status_code == 404
