from datetime import datetime
from uuid import UUID

import asyncpg


class TokenRepo:
    async def create(
        self,
        conn: asyncpg.Connection,
        user_id: UUID,
        token_hash: str,
        expires_at: datetime,
    ) -> None:
        await conn.execute(
            "INSERT INTO refresh_tokens (user_id, token_hash, expires_at)"
            " VALUES ($1, $2, $3)",
            user_id,
            token_hash,
            expires_at,
        )

    async def get_by_hash(
        self, conn: asyncpg.Connection, token_hash: str
    ) -> asyncpg.Record | None:
        return await conn.fetchrow(
            "SELECT id, user_id, token_hash, expires_at, created_at"
            " FROM refresh_tokens WHERE token_hash = $1",
            token_hash,
        )

    async def delete(self, conn: asyncpg.Connection, token_id: UUID) -> None:
        await conn.execute("DELETE FROM refresh_tokens WHERE id = $1", token_id)

    async def delete_by_user(self, conn: asyncpg.Connection, user_id: UUID) -> None:
        await conn.execute("DELETE FROM refresh_tokens WHERE user_id = $1", user_id)
