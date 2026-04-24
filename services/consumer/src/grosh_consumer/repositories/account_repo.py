from uuid import UUID

import asyncpg


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
