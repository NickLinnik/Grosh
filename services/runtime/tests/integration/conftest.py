"""Integration test fixtures for grosh-runtime.

Each test session gets a fresh database, migrated from scratch, dropped on
teardown. Each test gets its own asyncpg connection wrapped in a rolled-back
transaction, so tests are fully isolated.
"""

import json
import os
from collections.abc import AsyncGenerator
from pathlib import Path
from uuid import UUID, uuid4

import asyncpg
import pytest_asyncio
from dotenv import load_dotenv

_ENV_FILE = Path(__file__).resolve().parents[4] / "infra" / ".env"
if _ENV_FILE.exists():
    load_dotenv(_ENV_FILE, override=False)

_db_admin_url = os.environ.pop("DATABASE_URL_ADMIN", None)
if "DATABASE_URL" not in os.environ and _db_admin_url is not None:
    os.environ["DATABASE_URL"] = _db_admin_url

from grosh_shared.test_db import create_test_db, drop_test_db  # noqa: E402


@pytest_asyncio.fixture(loop_scope="session", scope="session")
async def db_pool() -> AsyncGenerator[asyncpg.Pool, None]:
    pool, db_name = await create_test_db("runtime")
    yield pool
    await pool.close()
    await drop_test_db(db_name)


@pytest_asyncio.fixture(loop_scope="session", scope="function")
async def conn(db_pool: asyncpg.Pool) -> AsyncGenerator[asyncpg.Connection, None]:
    async with db_pool.acquire() as connection:
        tx = connection.transaction()
        await tx.start()
        yield connection
        await tx.rollback()


# ---------------------------------------------------------------------------
# DB seed helpers
# ---------------------------------------------------------------------------


async def insert_user(
    conn: asyncpg.Connection,
    user_id: UUID | None = None,
) -> UUID:
    uid = user_id or uuid4()
    await conn.execute(
        """
        INSERT INTO users (id, email, password_hash, display_name, role)
        VALUES ($1, $2, 'hash', 'Test User', 'member')
        """,
        uid,
        f"test-{uid}@example.com",
    )
    return uid


async def insert_account(
    conn: asyncpg.Connection,
    *,
    user_id: UUID,
    type: str,
    currency_code: str,
    iban: str | None = None,
    source: str = "monobank",
    account_id: UUID | None = None,
) -> UUID:
    aid = account_id or uuid4()
    await conn.execute(
        """
        INSERT INTO accounts (id, user_id, source, type, currency_code, iban)
        VALUES ($1, $2, $3, $4, $5, $6)
        """,
        aid,
        user_id,
        source,
        type,
        currency_code,
        iban,
    )
    return aid


async def insert_transaction(
    conn: asyncpg.Connection,
    *,
    id: UUID | None = None,
    user_id: UUID,
    account_id: UUID,
    time,
    amount_cents: int,
    operation_amount_cents: int | None = None,
    mcc: str = "4829",
    direction: str,
    special_category: str | None = None,
    counterparty_iban: str | None = None,
    description: str | None = None,
    related_transaction_id: UUID | None = None,
    source: str = "monobank",
    currency_code: str = "UAH",
    operation_currency_code: str | None = None,
    metadata: dict | None = None,
) -> UUID:
    tx_id = id or uuid4()
    effective_op_amount = (
        operation_amount_cents if operation_amount_cents is not None else amount_cents
    )
    effective_op_currency = operation_currency_code or currency_code
    metadata_json = json.dumps(metadata) if metadata is not None else None
    await conn.execute(
        """
        INSERT INTO transactions (
            id,
            source_id,
            user_id,
            account_id,
            time,
            amount_cents,
            operation_amount_cents,
            currency_code,
            operation_currency_code,
            description,
            mcc,
            cashback_amount_cents,
            hold,
            direction,
            special_category,
            counterparty_iban,
            related_transaction_id,
            source,
            origin,
            metadata
        ) VALUES (
            $1, $2, $3, $4, $5,
            $6, $7, $8, $9, $10,
            $11, $12, $13, $14, $15,
            $16, $17, $18, $19, $20::jsonb
        )
        """,
        tx_id,
        f"src-{tx_id}",
        user_id,
        account_id,
        time,
        amount_cents,
        effective_op_amount,
        currency_code,
        effective_op_currency,
        description,
        mcc,
        0,
        False,
        direction,
        special_category,
        counterparty_iban,
        related_transaction_id,
        source,
        "bank",
        metadata_json,
    )
    return tx_id
