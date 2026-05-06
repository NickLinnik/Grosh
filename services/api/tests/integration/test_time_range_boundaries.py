"""Regression tests for half-open [from, to) time-range semantics.

All three endpoints (transactions list, aggregates, rates list) must exclude
a row whose timestamp exactly equals the upper bound.  These tests are the
authoritative contract for that invariant.
"""

import os
import uuid
from datetime import UTC, datetime
from decimal import Decimal

import asyncpg
from httpx import AsyncClient


async def _get_admin_id(conn: asyncpg.Connection) -> uuid.UUID:
    email = os.environ["ADMIN_EMAIL"].strip().lower()
    return await conn.fetchval(  # type: ignore[return-value]
        "SELECT id FROM users WHERE email = $1",
        email,
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
        VALUES ($1, 'manual', 'cash', 'UAH', 'Boundary Test Cash')
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
    time: datetime,
    amount_cents: int = 100,
    amount_uah_cents: int = 100,
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
            amount_uah_cents,
            source,
            origin
        ) VALUES (
            $1, $2, $3, $4, $5, 'UAH', 'expense', $6, 'manual', 'manual'
        )
        RETURNING id
        """,
        str(uuid.uuid4()),
        user_id,
        account_id,
        time,
        amount_cents,
        amount_uah_cents,
    )
    return tx_id


async def _seed_rate(
    conn: asyncpg.Connection,
    *,
    valid_from: datetime,
    valid_to: datetime | None = None,
) -> uuid.UUID:
    rate_id: uuid.UUID = await conn.fetchval(
        """
        INSERT INTO currency_rates (
            source,
            currency_from,
            currency_to,
            rate_mid,
            valid_from,
            valid_to
        ) VALUES ('nbu', 'USD', 'UAH', $1, $2, $3)
        RETURNING id
        """,
        Decimal("40.0"),
        valid_from,
        valid_to,
    )
    return rate_id


# ---------------------------------------------------------------------------
# Transactions list
# ---------------------------------------------------------------------------


async def test_transactions_list_excludes_row_at_upper_bound(
    conn: asyncpg.Connection,
    client: AsyncClient,
    admin_token: str,
) -> None:
    user_id = await _get_admin_id(conn)
    await _set_rls(conn, user_id)
    account_id = await _seed_account(conn, user_id)

    inside = datetime(2099, 1, 31, 23, 59, 59, tzinfo=UTC)
    at_bound = datetime(2099, 2, 1, 0, 0, 0, tzinfo=UTC)
    id_inside = await _seed_transaction(conn, user_id, account_id, time=inside)
    await _seed_transaction(conn, user_id, account_id, time=at_bound)

    resp = await client.get(
        "/transactions?from=2099-01-01T00:00:00Z&to=2099-02-01T00:00:00Z",
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 1
    assert body["items"][0]["id"] == str(id_inside)


# ---------------------------------------------------------------------------
# Aggregates
# ---------------------------------------------------------------------------


async def test_aggregates_excludes_row_at_upper_bound(
    conn: asyncpg.Connection,
    client: AsyncClient,
    admin_token: str,
) -> None:
    user_id = await _get_admin_id(conn)
    await _set_rls(conn, user_id)
    account_id = await _seed_account(conn, user_id)

    inside = datetime(2099, 1, 31, 23, 59, 59, tzinfo=UTC)
    at_bound = datetime(2099, 2, 1, 0, 0, 0, tzinfo=UTC)
    await _seed_transaction(
        conn, user_id, account_id, time=inside, amount_cents=100, amount_uah_cents=100
    )
    # This row must NOT appear in the aggregate for January
    await _seed_transaction(
        conn,
        user_id,
        account_id,
        time=at_bound,
        amount_cents=99999,
        amount_uah_cents=99999,
    )

    resp = await client.get(
        "/transactions/aggregates?bucket=month&from=2099-01-01T00:00:00Z&to=2099-02-01T00:00:00Z&currency=UAH",
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["items"]) == 1
    assert body["items"][0]["currencies"]["UAH"]["total_expense_cents"] == 100


# ---------------------------------------------------------------------------
# Rates list
# ---------------------------------------------------------------------------


async def test_rates_list_excludes_row_at_upper_bound(
    conn: asyncpg.Connection,
    client: AsyncClient,
    admin_token: str,
) -> None:
    inside = datetime(2099, 1, 1, 0, 0, 0, tzinfo=UTC)
    at_bound = datetime(2099, 2, 1, 0, 0, 0, tzinfo=UTC)
    id_inside = await _seed_rate(conn, valid_from=inside, valid_to=at_bound)
    await _seed_rate(conn, valid_from=at_bound)

    resp = await client.get(
        "/rates?from=2099-01-01T00:00:00Z&to=2099-02-01T00:00:00Z",
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 1
    assert body["items"][0]["id"] == str(id_inside)


# ---------------------------------------------------------------------------
# /rates/at — SCD2 half-open window [valid_from, valid_to)
# ---------------------------------------------------------------------------


async def test_rates_at_scd2_half_open_window(
    conn: asyncpg.Connection,
    client: AsyncClient,
    admin_token: str,
) -> None:
    valid_from = datetime(2099, 1, 1, 0, 0, 0, tzinfo=UTC)
    valid_to = datetime(2099, 2, 1, 0, 0, 0, tzinfo=UTC)
    rate_id = await _seed_rate(conn, valid_from=valid_from, valid_to=valid_to)

    # at == valid_from: included (lower bound is closed)
    resp = await client.get(
        "/rates/at?at=2099-01-01T00:00:00Z&source=nbu&currency_from=USD&currency_to=UAH",
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert resp.status_code == 200
    assert any(r["id"] == str(rate_id) for r in resp.json())

    # at in the middle: included
    resp = await client.get(
        "/rates/at?at=2099-01-15T12:00:00Z&source=nbu&currency_from=USD&currency_to=UAH",
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert resp.status_code == 200
    assert any(r["id"] == str(rate_id) for r in resp.json())

    # at == valid_to: excluded (upper bound is open)
    resp = await client.get(
        "/rates/at?at=2099-02-01T00:00:00Z&source=nbu&currency_from=USD&currency_to=UAH",
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert resp.status_code == 200
    assert not any(r["id"] == str(rate_id) for r in resp.json())
