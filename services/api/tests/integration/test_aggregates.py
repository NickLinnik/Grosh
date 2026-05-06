"""Integration tests for GET /transactions/aggregates."""

import os
import uuid
from datetime import UTC, datetime

import asyncpg
from httpx import AsyncClient

# Admin user seeded by migration 0002 (fetched dynamically to avoid hardcoding)
_ADMIN_EMAIL_ENV = "ADMIN_EMAIL"


async def _get_admin_id(conn: asyncpg.Connection) -> uuid.UUID:
    email = os.environ[_ADMIN_EMAIL_ENV].strip().lower()
    return await conn.fetchval("SELECT id FROM users WHERE email = $1", email)  # type: ignore[return-value]


async def _seed_account(conn: asyncpg.Connection, user_id: uuid.UUID) -> uuid.UUID:
    """Insert a manual cash account for the given user and return its id."""
    account_id: uuid.UUID = await conn.fetchval(
        """
        INSERT INTO accounts (user_id, source, type, currency_code, name)
        VALUES ($1, 'manual', 'cash', 'UAH', 'Test Cash')
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
    amount_cents: int,
    direction: str = "expense",
    special_category: str | None = None,
    amount_uah_cents: int | None = None,
    amount_usd_cents: int | None = None,
    amount_eur_cents: int | None = None,
    currency_code: str = "UAH",
) -> uuid.UUID:
    """Insert a transaction row directly, bypassing the pipeline."""
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
            amount_uah_cents,
            amount_usd_cents,
            amount_eur_cents,
            source,
            origin
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, 'manual', 'manual'
        )
        RETURNING id
        """,
        str(uuid.uuid4()),
        user_id,
        account_id,
        time,
        amount_cents,
        currency_code,
        direction,
        special_category,
        amount_uah_cents,
        amount_usd_cents,
        amount_eur_cents,
    )
    return tx_id


# ---------------------------------------------------------------------------
# Fixtures: per-test user_id and account_id derived from the admin seed
# ---------------------------------------------------------------------------


async def _set_rls(conn: asyncpg.Connection, user_id: uuid.UUID) -> None:
    await conn.execute(
        "SELECT set_config('app.current_user_id', $1::text, true)",
        str(user_id),
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


async def test_aggregates_empty_result(
    conn: asyncpg.Connection,
    client: AsyncClient,
    admin_token: str,
) -> None:
    resp = await client.get(
        "/transactions/aggregates?bucket=month",
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["items"] == []


async def test_aggregates_basic_monthly_bucketing(
    conn: asyncpg.Connection,
    client: AsyncClient,
    admin_token: str,
) -> None:
    user_id = await _get_admin_id(conn)
    await _set_rls(conn, user_id)
    account_id = await _seed_account(conn, user_id)

    jan = datetime(2026, 1, 15, 12, 0, tzinfo=UTC)
    await _seed_transaction(
        conn,
        user_id,
        account_id,
        time=jan,
        amount_cents=1000,
        direction="expense",
        amount_uah_cents=1000,
    )
    await _seed_transaction(
        conn,
        user_id,
        account_id,
        time=jan,
        amount_cents=2000,
        direction="expense",
        amount_uah_cents=2000,
    )
    await _seed_transaction(
        conn,
        user_id,
        account_id,
        time=jan,
        amount_cents=500,
        direction="expense",
        amount_uah_cents=500,
    )
    await _seed_transaction(
        conn,
        user_id,
        account_id,
        time=jan,
        amount_cents=10000,
        direction="income",
        amount_uah_cents=10000,
    )
    await _seed_transaction(
        conn,
        user_id,
        account_id,
        time=jan,
        amount_cents=10000,
        direction="income",
        amount_uah_cents=10000,
    )

    resp = await client.get(
        "/transactions/aggregates?bucket=month&from=2026-01-01T00:00:00Z&to=2026-02-01T00:00:00Z&currency=UAH",
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["items"]) == 1
    uah = body["items"][0]["currencies"]["UAH"]
    assert uah["total_income_cents"] == 20000
    assert uah["total_expense_cents"] == 3500
    assert uah["delta_cents"] == 16500
    assert uah["converted_pct"] == 100.0


async def test_aggregates_currency_filter(
    conn: asyncpg.Connection,
    client: AsyncClient,
    admin_token: str,
) -> None:
    user_id = await _get_admin_id(conn)
    await _set_rls(conn, user_id)
    account_id = await _seed_account(conn, user_id)

    t = datetime(2026, 2, 10, 12, 0, tzinfo=UTC)
    await _seed_transaction(
        conn,
        user_id,
        account_id,
        time=t,
        amount_cents=500,
        direction="expense",
        amount_uah_cents=500,
        amount_usd_cents=10,
        amount_eur_cents=9,
    )

    resp = await client.get(
        "/transactions/aggregates?bucket=month&from=2026-02-01T00:00:00Z&to=2026-03-01T00:00:00Z&currency=UAH",
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["items"]) == 1
    currencies = body["items"][0]["currencies"]
    assert list(currencies.keys()) == ["UAH"]


async def test_aggregates_fields_delta_only(
    conn: asyncpg.Connection,
    client: AsyncClient,
    admin_token: str,
) -> None:
    user_id = await _get_admin_id(conn)
    await _set_rls(conn, user_id)
    account_id = await _seed_account(conn, user_id)

    t = datetime(2026, 3, 5, 12, 0, tzinfo=UTC)
    await _seed_transaction(
        conn,
        user_id,
        account_id,
        time=t,
        amount_cents=300,
        direction="expense",
        amount_uah_cents=300,
    )

    resp = await client.get(
        "/transactions/aggregates?bucket=month&from=2026-03-01T00:00:00Z&to=2026-04-01T00:00:00Z&currency=UAH&fields=delta",
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["items"]) == 1
    uah = body["items"][0]["currencies"]["UAH"]
    assert "delta_cents" in uah
    assert "converted_pct" in uah
    assert "total_income_cents" not in uah
    assert "total_expense_cents" not in uah


async def test_aggregates_transfers_excluded(
    conn: asyncpg.Connection,
    client: AsyncClient,
    admin_token: str,
) -> None:
    user_id = await _get_admin_id(conn)
    await _set_rls(conn, user_id)
    account_id = await _seed_account(conn, user_id)

    t = datetime(2026, 4, 10, 12, 0, tzinfo=UTC)
    await _seed_transaction(
        conn,
        user_id,
        account_id,
        time=t,
        amount_cents=1000,
        direction="expense",
        amount_uah_cents=1000,
    )
    # transfer row: should be excluded from aggregates
    await _seed_transaction(
        conn,
        user_id,
        account_id,
        time=t,
        amount_cents=99999,
        direction="expense",
        special_category="transfer",
        amount_uah_cents=99999,
    )

    resp = await client.get(
        "/transactions/aggregates?bucket=month&from=2026-04-01T00:00:00Z&to=2026-05-01T00:00:00Z&currency=UAH",
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["items"]) == 1
    assert body["items"][0]["currencies"]["UAH"]["total_expense_cents"] == 1000


async def test_aggregates_zero_direction_excluded(
    conn: asyncpg.Connection,
    client: AsyncClient,
    admin_token: str,
) -> None:
    user_id = await _get_admin_id(conn)
    await _set_rls(conn, user_id)
    account_id = await _seed_account(conn, user_id)

    t = datetime(2026, 5, 10, 12, 0, tzinfo=UTC)
    await _seed_transaction(
        conn,
        user_id,
        account_id,
        time=t,
        amount_cents=500,
        direction="expense",
        amount_uah_cents=500,
    )
    # zero-direction row: should not appear in aggregates at all
    await _seed_transaction(
        conn,
        user_id,
        account_id,
        time=t,
        amount_cents=0,
        direction="zero",
        amount_uah_cents=0,
    )

    resp = await client.get(
        "/transactions/aggregates?bucket=month&from=2026-05-01T00:00:00Z&to=2026-06-01T00:00:00Z&currency=UAH",
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["items"]) == 1
    uah = body["items"][0]["currencies"]["UAH"]
    assert uah["total_expense_cents"] == 500
    assert uah["converted_pct"] == 100.0


async def test_aggregates_converted_pct_partial(
    conn: asyncpg.Connection,
    client: AsyncClient,
    admin_token: str,
) -> None:
    user_id = await _get_admin_id(conn)
    await _set_rls(conn, user_id)
    account_id = await _seed_account(conn, user_id)

    t = datetime(2026, 6, 10, 12, 0, tzinfo=UTC)
    # row A: EUR amount present
    await _seed_transaction(
        conn,
        user_id,
        account_id,
        time=t,
        amount_cents=100,
        direction="expense",
        amount_uah_cents=100,
        amount_eur_cents=3,
    )
    # row B: EUR amount missing — not yet converted
    await _seed_transaction(
        conn,
        user_id,
        account_id,
        time=t,
        amount_cents=200,
        direction="expense",
        amount_uah_cents=200,
        amount_eur_cents=None,
    )

    resp = await client.get(
        "/transactions/aggregates?bucket=month&from=2026-06-01T00:00:00Z&to=2026-07-01T00:00:00Z&currency=EUR&currency=UAH",
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["items"]) == 1
    currencies = body["items"][0]["currencies"]
    assert currencies["EUR"]["converted_pct"] == 50.0
    assert currencies["UAH"]["converted_pct"] == 100.0


async def test_aggregates_timezone_bucketing(
    conn: asyncpg.Connection,
    client: AsyncClient,
    admin_token: str,
) -> None:
    user_id = await _get_admin_id(conn)
    await _set_rls(conn, user_id)
    account_id = await _seed_account(conn, user_id)

    # 2024-01-31T21:30Z = 2024-01-31T23:30 Kyiv — still January in Kyiv timezone
    t = datetime(2024, 1, 31, 21, 30, tzinfo=UTC)
    await _seed_transaction(
        conn,
        user_id,
        account_id,
        time=t,
        amount_cents=777,
        direction="expense",
        amount_uah_cents=777,
    )

    # Set user timezone to Europe/Kyiv
    await conn.execute(
        "UPDATE user_settings SET timezone = 'Europe/Kyiv' WHERE user_id = $1",
        user_id,
    )

    resp = await client.get(
        "/transactions/aggregates?bucket=month&from=2024-01-01T00:00:00Z&to=2024-02-01T00:00:00Z&currency=UAH",
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["items"]) == 1
    # Jan 1 00:00 Kyiv (UTC+2 in winter) = 2023-12-31T22:00:00Z
    assert body["items"][0]["period_start"] == "2023-12-31T22:00:00Z"
    assert body["items"][0]["currencies"]["UAH"]["total_expense_cents"] == 777

    # Restore UTC timezone for test isolation (transaction is rolled back, but setting
    # is on a row that exists across all tests in the session)
    await conn.execute(
        "UPDATE user_settings SET timezone = 'UTC' WHERE user_id = $1",
        user_id,
    )


async def test_aggregates_invalid_bucket_422(
    client: AsyncClient,
    admin_token: str,
) -> None:
    resp = await client.get(
        "/transactions/aggregates?bucket=decade",
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert resp.status_code == 422


async def test_aggregates_invalid_currency_422(
    client: AsyncClient,
    admin_token: str,
) -> None:
    resp = await client.get(
        "/transactions/aggregates?currency=GBP",
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert resp.status_code == 422


async def test_aggregates_invalid_fields_422(
    client: AsyncClient,
    admin_token: str,
) -> None:
    resp = await client.get(
        "/transactions/aggregates?fields=profit",
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert resp.status_code == 422


async def test_aggregates_poisoned_timezone_returns_422(
    conn: asyncpg.Connection,
    client: AsyncClient,
    admin_token: str,
) -> None:
    user_id = await _get_admin_id(conn)
    await _set_rls(conn, user_id)

    # Bypass the write-path validator to simulate a corrupt DB value
    await conn.execute(
        "UPDATE user_settings SET timezone = 'Not/AZone' WHERE user_id = $1",
        user_id,
    )

    resp = await client.get(
        "/transactions/aggregates?bucket=month",
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert resp.status_code == 422
    assert "Not/AZone" in resp.json()["detail"]

    # Restore UTC so subsequent tests are not affected
    await conn.execute(
        "UPDATE user_settings SET timezone = 'UTC' WHERE user_id = $1",
        user_id,
    )


async def test_currency_lowercase_returns_422(
    client: AsyncClient,
    admin_token: str,
) -> None:
    resp = await client.get(
        "/transactions/aggregates?currency=uah",
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert resp.status_code == 422


async def test_repeated_currency_returns_both(
    conn: asyncpg.Connection,
    client: AsyncClient,
    admin_token: str,
) -> None:
    user_id = await _get_admin_id(conn)
    await _set_rls(conn, user_id)
    account_id = await _seed_account(conn, user_id)

    t = datetime(2026, 7, 10, 12, 0, tzinfo=UTC)
    await _seed_transaction(
        conn,
        user_id,
        account_id,
        time=t,
        amount_cents=1000,
        direction="expense",
        amount_uah_cents=1000,
        amount_usd_cents=25,
    )

    resp = await client.get(
        "/transactions/aggregates"
        "?bucket=month"
        "&from=2026-07-01T00:00:00Z"
        "&to=2026-08-01T00:00:00Z"
        "&currency=UAH"
        "&currency=USD",
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["items"]) == 1
    currencies = body["items"][0]["currencies"]
    assert set(currencies.keys()) == {"UAH", "USD"}
    assert "EUR" not in currencies
