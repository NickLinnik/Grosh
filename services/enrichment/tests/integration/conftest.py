"""Integration test fixtures for grosh-enrichment.

Each test session gets a fresh database, migrated from scratch, dropped on
teardown. Each test gets its own asyncpg connection wrapped in a rolled-back
transaction, so tests are fully isolated.
"""

import json
import os
from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from uuid import UUID, uuid4

import asyncpg
import pytest
import pytest_asyncio
from dotenv import load_dotenv

_ENV_FILE = Path(__file__).resolve().parents[4] / "infra" / ".env"
if _ENV_FILE.exists():
    load_dotenv(_ENV_FILE, override=False)

_db_admin_url = os.environ.pop("DATABASE_URL_ADMIN", None)
if "DATABASE_URL" not in os.environ and _db_admin_url is not None:
    os.environ["DATABASE_URL"] = _db_admin_url

from grosh_shared.test_db import create_test_db, drop_test_db  # noqa: E402

# ---------------------------------------------------------------------------
# Test pool helpers
# ---------------------------------------------------------------------------


class SingleConnectionPool:
    """Test helper: duck-typed pool that always yields a single shared connection."""

    def __init__(self, conn: asyncpg.Connection) -> None:
        self._conn = conn

    @asynccontextmanager
    async def acquire(self) -> AsyncIterator[asyncpg.Connection]:
        yield self._conn


from grosh_enrichment.repositories.account_repo import AccountRepo  # noqa: E402
from grosh_enrichment.repositories.anomaly_repo import AnomalyRepo  # noqa: E402
from grosh_enrichment.repositories.currency_rate_repo import (  # noqa: E402
    CurrencyRateRepo,
)
from grosh_enrichment.services.currency_conversion_service import (  # noqa: E402
    CurrencyConversionService,
)

try:
    from grosh_enrichment.sources.monobank.transfer.repo import (
        TransferQueryRepo,  # noqa: E402
    )
except ImportError:
    TransferQueryRepo = None  # type: ignore[assignment, misc]

try:
    from grosh_enrichment.sources.monobank.transfer.detector import (  # noqa: E402
        MonobankTransferDetection,
    )
except ImportError:
    MonobankTransferDetection = None  # type: ignore[assignment, misc]


@pytest_asyncio.fixture(loop_scope="session", scope="session")
async def db_pool() -> AsyncGenerator[asyncpg.Pool, None]:
    pool, db_name = await create_test_db("pipeline")
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


@pytest.fixture(scope="function")
def rate_repo() -> CurrencyRateRepo:
    return CurrencyRateRepo()


@pytest.fixture(scope="function")
def service(rate_repo: CurrencyRateRepo) -> CurrencyConversionService:
    return CurrencyConversionService(rate_repo)


# ---------------------------------------------------------------------------
# Transfer detection repos
# ---------------------------------------------------------------------------


@pytest.fixture(scope="function")
def transfer_repo():
    if TransferQueryRepo is None:
        pytest.skip("TransferQueryRepo not yet implemented")
    return TransferQueryRepo()


@pytest.fixture(scope="function")
def anomaly_repo() -> AnomalyRepo:
    return AnomalyRepo()


@pytest.fixture(scope="function")
def account_repo() -> AccountRepo:
    return AccountRepo()


@pytest.fixture(scope="function")
def transfer_service(transfer_repo, account_repo, anomaly_repo):
    if MonobankTransferDetection is None:
        pytest.skip("MonobankTransferDetection not yet implemented")
    return MonobankTransferDetection(transfer_repo, account_repo, anomaly_repo)


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


async def get_transaction(
    conn: asyncpg.Connection,
    tx_id: UUID,
) -> asyncpg.Record:
    row = await conn.fetchrow(
        """
        SELECT *
        FROM transactions
        WHERE id = $1
        """,
        tx_id,
    )
    if row is None:
        raise ValueError(f"Transaction {tx_id} not found")
    return row


async def get_anomaly(
    conn: asyncpg.Connection,
    transaction_id: UUID,
) -> asyncpg.Record | None:
    return await conn.fetchrow(
        """
        SELECT *
        FROM transfer_match_anomalies
        WHERE transaction_id = $1
        """,
        transaction_id,
    )


async def count_anomalies(
    conn: asyncpg.Connection,
    user_id: UUID | None = None,
) -> int:
    if user_id is None:
        val = await conn.fetchval(
            """
            SELECT COUNT(*)
            FROM transfer_match_anomalies
            """
        )
    else:
        val = await conn.fetchval(
            """
            SELECT COUNT(*)
            FROM transfer_match_anomalies a
            JOIN transactions t ON t.id = a.transaction_id
            WHERE t.user_id = $1
            """,
            user_id,
        )
    return int(val)


async def get_transfer_metadata(
    conn: asyncpg.Connection,
    tx_id: UUID,
) -> dict | None:
    """Safely walks metadata.layer.transfer on the stored transaction."""
    row = await conn.fetchrow(
        """
        SELECT metadata
        FROM transactions
        WHERE id = $1
        """,
        tx_id,
    )
    raw = row["metadata"] if row else None
    if raw is None:
        return None
    if isinstance(raw, str):
        raw = json.loads(raw)
    return raw.get("layer", {}).get("transfer")
