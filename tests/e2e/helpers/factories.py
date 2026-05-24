"""Factory helpers for e2e tests.

Each factory inserts directly via asyncpg as grosh_admin (bypassing RLS via
table ownership). Sensible defaults; override via kwargs.

Patterns mirror the production schema. Tests should NOT replicate INSERTs
in-line — always go through factories so a schema change ripples through
one helper, not 50 tests.
"""

from __future__ import annotations

import json
import secrets
from datetime import UTC, datetime
from uuid import UUID, uuid4

import asyncpg
import bcrypt

_DEFAULT_PASSWORD = "TestPass123!"  # noqa: S105 — fixture credential


async def build_user(
    pg: asyncpg.Connection,
    *,
    user_id: UUID | None = None,
    email: str | None = None,
    password: str = _DEFAULT_PASSWORD,
    display_name: str = "Test User",
    role: str = "member",
) -> tuple[UUID, str, str]:
    """Insert a user. Returns (user_id, email, plaintext_password).

    The plaintext password is needed for /v1/auth/login. Bcrypt hash is
    stored in the DB.
    """
    uid = user_id or uuid4()
    actual_email = email or f"test-{secrets.token_hex(4)}@example.com"
    password_hash = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
    await pg.execute(
        """
        INSERT INTO users (id, email, password_hash, display_name, role)
        VALUES ($1, $2, $3, $4, $5)
        """,
        uid,
        actual_email,
        password_hash,
        display_name,
        role,
    )
    return uid, actual_email, password


async def build_admin(pg: asyncpg.Connection, **kwargs) -> tuple[UUID, str, str]:
    """Convenience: build_user with role='admin'."""
    return await build_user(pg, role="admin", **kwargs)


async def build_bank_integration(
    pg: asyncpg.Connection,
    *,
    user_id: UUID,
    integration_id: UUID | None = None,
    bank: str = "monobank",
    monobank_client_id: str | None = None,
    webhook_secret: str | None = None,
    encrypted_token: str = "encrypted-test-token",
    key_version: int = 1,
    status: str = "active",
) -> tuple[UUID, str]:
    """Insert a bank_integrations row. Returns (integration_id, webhook_secret)."""
    iid = integration_id or uuid4()
    secret = webhook_secret or secrets.token_hex(32)
    client_id = monobank_client_id or f"mb-client-{secrets.token_hex(4)}"
    config = {
        "webhook_secret": secret,
        "monobank_client_id": client_id,
        "encrypted_token": encrypted_token,
        "key_version": key_version,
    }
    await pg.execute(
        """
        INSERT INTO bank_integrations (id, user_id, bank, config, status)
        VALUES ($1, $2, $3, $4::jsonb, $5)
        """,
        iid,
        user_id,
        bank,
        json.dumps(config),
        status,
    )
    return iid, secret


async def build_account(
    pg: asyncpg.Connection,
    *,
    user_id: UUID,
    account_id: UUID | None = None,
    integration_id: UUID | None = None,
    source: str = "monobank",
    account_type: str = "black",
    currency_code: str = "UAH",
    masked_pan: str | None = None,
    iban: str | None = None,
    external_id: str | None = None,
    cashback_type: str | None = None,
    name: str | None = None,
    is_active: bool = True,
    config: dict | None = None,
) -> UUID:
    """Insert an account. Returns the account_id."""
    aid = account_id or uuid4()
    cfg = config or {}
    await pg.execute(
        """
        INSERT INTO accounts (
            id, user_id, integration_id, source, type, currency_code,
            masked_pan, iban, external_id, cashback_type, name, is_active, config
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13::jsonb)
        """,
        aid,
        user_id,
        integration_id,
        source,
        account_type,
        currency_code,
        masked_pan,
        iban,
        external_id,
        cashback_type,
        name,
        is_active,
        json.dumps(cfg),
    )
    return aid


async def build_transaction(
    pg: asyncpg.Connection,
    *,
    user_id: UUID,
    account_id: UUID,
    transaction_id: UUID | None = None,
    source_id: str | None = None,
    source: str = "monobank",
    origin: str = "bank",
    time: datetime | None = None,
    amount_cents: int = -10000,
    operation_amount_cents: int | None = None,
    currency_code: str = "UAH",
    operation_currency_code: str | None = None,
    description: str = "test transaction",
    mcc: str = "4829",
    cashback_amount_cents: int = 0,
    balance_cents: int | None = None,
    hold: bool = False,
    direction: str = "expense",
    special_category: str | None = None,
    counterparty_iban: str | None = None,
    related_transaction_id: UUID | None = None,
    metadata: dict | None = None,
) -> UUID:
    """Insert a transaction. Returns the transaction_id."""
    tx_id = transaction_id or uuid4()
    sid = source_id or f"src-{secrets.token_hex(8)}"
    tx_time = time or datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
    op_amount = (
        operation_amount_cents if operation_amount_cents is not None else amount_cents
    )
    op_currency = operation_currency_code or currency_code
    meta_json = json.dumps(metadata) if metadata is not None else None
    await pg.execute(
        """
        INSERT INTO transactions (
            id, source_id, user_id, account_id, time,
            amount_cents, operation_amount_cents, currency_code, operation_currency_code,
            description, mcc, cashback_amount_cents, balance_cents, hold,
            direction, special_category, counterparty_iban, related_transaction_id,
            source, origin, metadata
        ) VALUES (
            $1, $2, $3, $4, $5,
            $6, $7, $8, $9,
            $10, $11, $12, $13, $14,
            $15, $16, $17, $18,
            $19, $20, $21::jsonb
        )
        """,
        tx_id,
        sid,
        user_id,
        account_id,
        tx_time,
        amount_cents,
        op_amount,
        currency_code,
        op_currency,
        description,
        mcc,
        cashback_amount_cents,
        balance_cents,
        hold,
        direction,
        special_category,
        counterparty_iban,
        related_transaction_id,
        source,
        origin,
        meta_json,
    )
    return tx_id


async def build_rate(
    pg: asyncpg.Connection,
    *,
    source: str = "nbu",
    currency_from: str = "USD",
    currency_to: str = "UAH",
    rate_mid: float = 40.0,
    rate_buy: float | None = None,
    rate_sell: float | None = None,
    valid_from: datetime | None = None,
    valid_to: datetime | None = None,
    rate_id: UUID | None = None,
) -> UUID:
    """Insert a currency rate row (SCD2). Returns the rate_id."""
    rid = rate_id or uuid4()
    vf = valid_from or datetime(2026, 5, 1, 0, 0, tzinfo=UTC)
    await pg.execute(
        """
        INSERT INTO currency_rates (
            id, source, currency_from, currency_to,
            rate_buy, rate_sell, rate_mid, valid_from, valid_to
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        """,
        rid,
        source,
        currency_from,
        currency_to,
        rate_buy,
        rate_sell,
        rate_mid,
        vf,
        valid_to,
    )
    return rid


async def build_seed_admin(
    pg: asyncpg.Connection,
    password: str = _DEFAULT_PASSWORD,
) -> tuple[UUID, str, str]:
    """Find the seeded admin (from migration 0002) and return creds.

    The seeded admin's password came from $ADMIN_PASSWORD at migration time;
    tests can't recover it. So we update the password hash in place to a known
    value (`TestPass123!` by default). Returns (id, email, password).
    """
    row = await pg.fetchrow("SELECT id, email FROM users WHERE role = 'admin' LIMIT 1")
    if row is None:
        raise RuntimeError("Seed admin not found — migration 0002 didn't run?")
    new_hash = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
    await pg.execute(
        "UPDATE users SET password_hash = $1 WHERE id = $2",
        new_hash,
        row["id"],
    )
    return row["id"], row["email"], password
