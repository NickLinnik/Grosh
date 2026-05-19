from dataclasses import dataclass
from uuid import UUID

import asyncpg


@dataclass(frozen=True)
class AccountProps:
    id: UUID
    type: str
    currency_code: str
    iban: str | None


class AccountNotFoundError(Exception):
    pass


class AccountRepo:
    async def find_by_iban(
        self,
        conn: asyncpg.Connection,
        iban: str,
        user_id: UUID,
    ) -> AccountProps | None:
        row = await conn.fetchrow(
            """
            SELECT
                id,
                type,
                currency_code,
                iban
            FROM accounts
            WHERE iban = $1
              AND user_id = $2
            """,
            iban,
            user_id,
        )
        if row is None:
            return None
        return AccountProps(
            id=row["id"],
            type=row["type"],
            currency_code=row["currency_code"],
            iban=row["iban"],
        )

    async def get_by_id(
        self,
        conn: asyncpg.Connection,
        account_id: UUID,
    ) -> AccountProps:
        row = await conn.fetchrow(
            """
            SELECT
                id,
                type,
                currency_code,
                iban
            FROM accounts
            WHERE id = $1
            """,
            account_id,
        )
        if row is None:
            raise AccountNotFoundError(f"Account {account_id} not found")
        return AccountProps(
            id=row["id"],
            type=row["type"],
            currency_code=row["currency_code"],
            iban=row["iban"],
        )

    async def get_many_by_ids(
        self,
        conn: asyncpg.Connection,
        account_ids: list[UUID],
    ) -> dict[UUID, AccountProps]:
        if not account_ids:
            return {}
        rows = await conn.fetch(
            """
            SELECT
                id,
                type,
                currency_code,
                iban
            FROM accounts
            WHERE id = ANY($1::uuid[])
            """,
            account_ids,
        )
        return {
            row["id"]: AccountProps(
                id=row["id"],
                type=row["type"],
                currency_code=row["currency_code"],
                iban=row["iban"],
            )
            for row in rows
        }
