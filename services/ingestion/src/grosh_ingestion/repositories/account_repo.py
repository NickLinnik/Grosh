from uuid import UUID

import asyncpg
from grosh_shared.models import TransactionSource


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
    ) -> UUID:
        row = await conn.fetchrow(
            """
            INSERT INTO accounts
                (user_id, integration_id, source, type, currency_code,
                 masked_pan, iban, external_id, cashback_type, name, is_active)
            VALUES
                ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, true)
            RETURNING id
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
        return row["id"]

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
