from uuid import UUID

import asyncpg


class AccountNotFoundError(Exception):
    pass


class AccountRepo:
    async def find_by_iban(
        self,
        conn: asyncpg.Connection,
        iban: str,
        user_id: UUID,
    ) -> UUID | None:
        row = await conn.fetchrow(
            """
            SELECT id
            FROM accounts
            WHERE iban = $1
              AND user_id = $2
            """,
            iban,
            user_id,
        )
        return row["id"] if row is not None else None

    async def get_currency_code(
        self,
        conn: asyncpg.Connection,
        account_id: UUID,
    ) -> str:
        result: str | None = await conn.fetchval(
            """
            SELECT currency_code
            FROM accounts
            WHERE id = $1
            """,
            account_id,
        )
        if result is None:
            raise AccountNotFoundError(f"Account {account_id} not found")
        return result
