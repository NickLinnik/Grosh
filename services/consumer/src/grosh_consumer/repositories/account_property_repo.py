"""Account property queries for transfer detection.

Provides the richer property set (type, currency_code, iban) needed by
description validation in the transfer detection strategy.
"""

from uuid import UUID

import asyncpg

from grosh_consumer.repositories.account_repo import AccountNotFoundError


class AccountPropertyRepo:
    async def get_account_with_properties(
        self,
        conn: asyncpg.Connection,
        account_id: UUID,
    ) -> tuple[str, str, str | None]:
        row = await conn.fetchrow(
            """
            SELECT type, currency_code, iban
            FROM accounts
            WHERE id = $1
            """,
            account_id,
        )
        if row is None:
            raise AccountNotFoundError(f"Account {account_id} not found")
        return row["type"], row["currency_code"], row["iban"]

    async def get_accounts_with_properties(
        self,
        conn: asyncpg.Connection,
        account_ids: list[UUID],
    ) -> dict[UUID, tuple[str, str, str | None]]:
        """Batch-fetch (type, currency_code, iban) for multiple accounts."""
        if not account_ids:
            return {}
        rows = await conn.fetch(
            """
            SELECT id, type, currency_code, iban
            FROM accounts
            WHERE id = ANY($1::uuid[])
            """,
            account_ids,
        )
        return {
            row["id"]: (row["type"], row["currency_code"], row["iban"]) for row in rows
        }

    async def get_user_account_by_iban(
        self,
        conn: asyncpg.Connection,
        iban: str,
        user_id: UUID,
    ) -> tuple[UUID, str, str] | None:
        row = await conn.fetchrow(
            """
            SELECT id, type, currency_code
            FROM accounts
            WHERE iban = $1
              AND user_id = $2
            """,
            iban,
            user_id,
        )
        if row is None:
            return None
        return row["id"], row["type"], row["currency_code"]
