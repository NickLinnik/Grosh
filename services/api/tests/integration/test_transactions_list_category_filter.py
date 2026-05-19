"""Integration tests for category / exclude_category filter params on GET /transactions."""  # noqa: E501

import os
import uuid
from datetime import UTC, datetime

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


async def _seed_account(conn: asyncpg.Connection, user_id: uuid.UUID) -> uuid.UUID:
    account_id: uuid.UUID = await conn.fetchval(
        """
        INSERT INTO accounts (user_id, source, type, currency_code, name)
        VALUES ($1, 'manual', 'cash', 'UAH', 'Filter Test Cash')
        RETURNING id
        """,
        user_id,
    )
    return account_id


async def _seed_transaction(
    conn: asyncpg.Connection,
    user_id: uuid.UUID,
    account_id: uuid.UUID,
    *,
    direction: str,
    special_category: str | None = None,
) -> uuid.UUID:
    tx_id: uuid.UUID = await conn.fetchval(
        """
        INSERT INTO transactions (
            source_id,
            user_id,
            account_id,
            time,
            amount_cents,
            currency_code,
            direction,
            special_category,
            source,
            origin
        ) VALUES (
            $1, $2, $3, $4, $5, 'UAH', $6, $7, 'manual', 'manual'
        )
        RETURNING id
        """,
        str(uuid.uuid4()),
        user_id,
        account_id,
        datetime(2026, 1, 15, 12, 0, tzinfo=UTC),
        1000,
        direction,
        special_category,
    )
    return tx_id


@pytest.fixture
async def fixture_ids(
    conn: asyncpg.Connection,
) -> dict[str, uuid.UUID]:
    """Insert 3 rows: 1 ordinary expense, 1 ordinary income, 1 transfer."""
    user_id = await _get_admin_id(conn)
    await _set_rls(conn, user_id)
    account_id = await _seed_account(conn, user_id)

    expense_id = await _seed_transaction(conn, user_id, account_id, direction="expense")
    income_id = await _seed_transaction(conn, user_id, account_id, direction="income")
    transfer_id = await _seed_transaction(
        conn, user_id, account_id, direction="expense", special_category="transfer"
    )
    return {"expense": expense_id, "income": income_id, "transfer": transfer_id}


async def test_asyncpg_special_category_array_cast(conn: asyncpg.Connection) -> None:
    result = await conn.fetchval("SELECT $1::special_category[]", ["transfer"])
    assert result == ["transfer"]


async def test_list_no_filter_returns_all(
    fixture_ids: dict[str, uuid.UUID],
    client: AsyncClient,
    admin_token: str,
) -> None:
    resp = await client.get(
        "/transactions",
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert resp.status_code == 200
    returned_ids = {uuid.UUID(item["id"]) for item in resp.json()["items"]}
    assert set(fixture_ids.values()) <= returned_ids


async def test_list_whitelist_transfer_returns_only_transfer(
    fixture_ids: dict[str, uuid.UUID],
    client: AsyncClient,
    admin_token: str,
) -> None:
    resp = await client.get(
        "/transactions?category=transfer",
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert resp.status_code == 200
    items = resp.json()["items"]
    returned_ids = {uuid.UUID(item["id"]) for item in items}
    assert fixture_ids["transfer"] in returned_ids
    assert fixture_ids["expense"] not in returned_ids
    assert fixture_ids["income"] not in returned_ids
    assert all(item["special_category"] == "transfer" for item in items)


async def test_list_blacklist_transfer_returns_ordinary(
    fixture_ids: dict[str, uuid.UUID],
    client: AsyncClient,
    admin_token: str,
) -> None:
    resp = await client.get(
        "/transactions?exclude_category=transfer",
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert resp.status_code == 200
    items = resp.json()["items"]
    returned_ids = {uuid.UUID(item["id"]) for item in items}
    assert fixture_ids["expense"] in returned_ids
    assert fixture_ids["income"] in returned_ids
    assert fixture_ids["transfer"] not in returned_ids
    assert all(item["special_category"] is None for item in items)


async def test_list_conflict_returns_422(
    client: AsyncClient,
    admin_token: str,
) -> None:
    resp = await client.get(
        "/transactions?category=transfer&exclude_category=transfer",
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert resp.status_code == 422
    assert "transfer" in resp.text.lower()


async def test_list_wrong_case_returns_422(
    client: AsyncClient,
    admin_token: str,
) -> None:
    resp = await client.get(
        "/transactions?category=Transfer",
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert resp.status_code == 422


async def test_list_duplicates_in_one_param_dedup(
    fixture_ids: dict[str, uuid.UUID],
    client: AsyncClient,
    admin_token: str,
) -> None:
    resp = await client.get(
        "/transactions?category=transfer&category=transfer",
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert resp.status_code == 200
    items = resp.json()["items"]
    returned_ids = {uuid.UUID(item["id"]) for item in items}
    assert fixture_ids["transfer"] in returned_ids
    assert fixture_ids["expense"] not in returned_ids
    assert fixture_ids["income"] not in returned_ids


async def test_list_empty_enum_value_returns_422(
    client: AsyncClient,
    admin_token: str,
) -> None:
    resp = await client.get(
        "/transactions?category=",
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert resp.status_code == 422
