from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

import asyncpg


@dataclass(frozen=True)
class TokenRecord:
    id: UUID
    user_id: UUID
    token_hash: str
    expires_at: datetime
    created_at: datetime


class TokenRepo:
    async def create(
        self,
        conn: asyncpg.Connection,
        user_id: UUID,
        token_hash: str,
        expires_at: datetime,
    ) -> None:
        await conn.execute(
            """
            INSERT INTO refresh_tokens (user_id, token_hash, expires_at)
            VALUES ($1, $2, $3)
            """,
            user_id,
            token_hash,
            expires_at,
        )

    async def get_by_hash(
        self, conn: asyncpg.Connection, token_hash: str
    ) -> TokenRecord | None:
        row = await conn.fetchrow(
            """
            SELECT id, user_id, token_hash, expires_at, created_at
            FROM refresh_tokens
            WHERE token_hash = $1
            """,
            token_hash,
        )
        if row is None:
            return None
        return TokenRecord(
            id=row["id"],
            user_id=row["user_id"],
            token_hash=row["token_hash"],
            expires_at=row["expires_at"],
            created_at=row["created_at"],
        )

    async def delete(self, conn: asyncpg.Connection, token_id: UUID) -> None:
        await conn.execute(
            "DELETE FROM refresh_tokens WHERE id = $1",
            token_id,
        )

    async def delete_by_user(self, conn: asyncpg.Connection, user_id: UUID) -> None:
        await conn.execute(
            "DELETE FROM refresh_tokens WHERE user_id = $1",
            user_id,
        )
