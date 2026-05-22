from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

import asyncpg
from grosh_shared.domain.models import TransactionSource


@dataclass(frozen=True)
class CreatedAccount:
    id: UUID
    type: str
    currency_code: str
    name: str | None
    created_at: datetime


@dataclass(frozen=True)
class UpdatedAccount:
    id: UUID
    type: str
    currency_code: str
    name: str | None
    created_at: datetime


class AccountRepo:
    async def create_account(
        self,
        conn: asyncpg.Connection,
        user_id: UUID,
        source: TransactionSource,
        account_type: str,
        currency_code: str,
        integration_id: UUID | None = None,
        masked_pan: str | None = None,
        iban: str | None = None,
        external_id: str | None = None,
        cashback_type: str | None = None,
        name: str | None = None,
    ) -> CreatedAccount:
        row = await conn.fetchrow(
            """
            INSERT INTO accounts
                (user_id, integration_id, source, type, currency_code,
                 masked_pan, iban, external_id, cashback_type, name, is_active)
            VALUES
                ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, true)
            RETURNING
                id,
                type,
                currency_code,
                name,
                created_at
            """,
            user_id,
            integration_id,
            str(source),
            account_type,
            currency_code,
            masked_pan,
            iban,
            external_id,
            cashback_type,
            name,
        )
        return CreatedAccount(
            id=row["id"],
            type=row["type"],
            currency_code=row["currency_code"],
            name=row["name"],
            created_at=row["created_at"],
        )

    async def manual_name_exists(
        self,
        conn: asyncpg.Connection,
        user_id: UUID,
        name: str,
        currency_code: str,
    ) -> bool:
        return await conn.fetchval(
            """
            SELECT EXISTS(
                SELECT 1
                FROM accounts
                WHERE user_id = $1
                    AND name = $2
                    AND currency_code = $3
                    AND source = 'manual'
            )
            """,
            user_id,
            name,
            currency_code,
        )

    async def belongs_to_user(
        self, conn: asyncpg.Connection, account_id: UUID, user_id: UUID
    ) -> bool:
        return await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM accounts WHERE id = $1 AND user_id = $2)",
            account_id,
            user_id,
        )

    async def get_by_id(
        self,
        conn: asyncpg.Connection,
        account_id: UUID,
        user_id: UUID,
    ) -> dict | None:
        """Fetch a minimal account record by ID, scoped to the given user."""
        row = await conn.fetchrow(
            """
            SELECT
                id,
                source,
                user_id,
                name,
                is_active
            FROM accounts
            WHERE id = $1
                AND user_id = $2
            """,
            account_id,
            user_id,
        )
        if row is None:
            return None
        return {
            "id": row["id"],
            "source": row["source"],
            "user_id": row["user_id"],
            "name": row["name"],
            "is_active": row["is_active"],
        }

    async def update_name(
        self,
        conn: asyncpg.Connection,
        account_id: UUID,
        user_id: UUID,
        name: str,
    ) -> UpdatedAccount | None:
        """Update the name of an account.

        Returns the updated record or None if not found.
        """
        row = await conn.fetchrow(
            """
            UPDATE accounts
            SET name = $3
            WHERE id = $1
                AND user_id = $2
            RETURNING
                id,
                type,
                currency_code,
                name,
                created_at
            """,
            account_id,
            user_id,
            name,
        )
        if row is None:
            return None
        return UpdatedAccount(
            id=row["id"],
            type=row["type"],
            currency_code=row["currency_code"],
            name=row["name"],
            created_at=row["created_at"],
        )

    async def soft_delete(
        self,
        conn: asyncpg.Connection,
        account_id: UUID,
        user_id: UUID,
    ) -> bool:
        """Set is_active=false. Returns True if a row was updated."""
        result = await conn.execute(
            """
            UPDATE accounts
            SET is_active = false
            WHERE id = $1
                AND user_id = $2
                AND is_active = true
            """,
            account_id,
            user_id,
        )
        return result == "UPDATE 1"

    async def get_user_id(
        self,
        conn: asyncpg.Connection,
        account_id: UUID,
    ) -> UUID | None:
        """Return the user_id that owns the account, or None if not found.

        Unscoped — does not filter by caller. Use only after an auth check.
        """
        return await conn.fetchval(
            """
            SELECT user_id
            FROM accounts
            WHERE id = $1
            """,
            account_id,
        )

    async def get_external_ref(
        self,
        conn: asyncpg.Connection,
        account_id: UUID,
        user_id: UUID,
    ) -> tuple[str, UUID] | None:
        row = await conn.fetchrow(
            """
            SELECT
                external_id,
                integration_id
            FROM accounts
            WHERE id = $1
                AND user_id = $2
                AND integration_id IS NOT NULL
                AND external_id IS NOT NULL
            """,
            account_id,
            user_id,
        )
        if row is None:
            return None
        return row["external_id"], row["integration_id"]
