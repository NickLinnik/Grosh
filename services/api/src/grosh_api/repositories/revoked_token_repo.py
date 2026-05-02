from datetime import datetime
from uuid import UUID

import asyncpg


class RevokedTokenRepo:
    async def insert(
        self, conn: asyncpg.Connection, jti: UUID, expires_at: datetime
    ) -> None:
        await conn.execute(
            """
            INSERT INTO revoked_tokens (jti, expires_at)
            VALUES ($1, $2)
            """,
            jti,
            expires_at,
        )

    async def is_revoked(self, conn: asyncpg.Connection, jti: UUID) -> bool:
        return await conn.fetchval(
            """
            SELECT EXISTS(
                SELECT 1
                FROM revoked_tokens
                WHERE jti = $1
            )
            """,
            jti,
        )
