from uuid import UUID

import asyncpg
from grosh_shared.models import AccountType, TransactionSource


class AccountRepo:
    async def create_account(
        self,
        conn: asyncpg.Connection,
        user_id: UUID,
        source: TransactionSource,
        account_type: AccountType,
        currency_code: str,
        integration_id: UUID | None = None,
        masked_pan: str | None = None,
        iban: str | None = None,
        external_id: str | None = None,
        cashback_type: str | None = None,
    ) -> UUID:
        row = await conn.fetchrow(
            """
            INSERT INTO accounts
                (user_id, integration_id, source, type, currency_code,
                 masked_pan, iban, external_id, cashback_type, is_active)
            VALUES
                ($1, $2, $3, $4, $5, $6, $7, $8, $9, true)
            RETURNING id
            """,
            user_id,
            integration_id,
            str(source),
            str(account_type),
            currency_code,
            masked_pan,
            iban,
            external_id,
            cashback_type,
        )
        return row["id"]

    async def belongs_to_user(
        self, conn: asyncpg.Connection, account_id: UUID, user_id: UUID
    ) -> bool:
        return await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM accounts WHERE id = $1 AND user_id = $2)",
            account_id,
            user_id,
        )
