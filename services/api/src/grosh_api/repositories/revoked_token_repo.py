from datetime import datetime
from uuid import UUID

import asyncpg


class RevokedTokenRepo:
    async def insert(
        self, conn: asyncpg.Connection, jti: UUID, expires_at: datetime
    ) -> None:
        # Idempotent insert: /v1/auth/logout may be called twice with the same
        # access token (or after /v1/auth/logout-all already revoked it), and
        # the second call must succeed as a no-op rather than 500.
        await conn.execute(
            """
            INSERT INTO revoked_tokens (jti, expires_at)
            VALUES ($1, $2)
            ON CONFLICT (jti) DO NOTHING
            """,
            jti,
            expires_at,
        )

    async def is_revoked(self, conn: asyncpg.Connection, jti: UUID) -> bool:
        # EXISTS always returns a row — fetchval never returns None here.
        result: bool | None = await conn.fetchval(
            """
            SELECT EXISTS(
                SELECT 1
                FROM revoked_tokens
                WHERE jti = $1
            )
            """,
            jti,
        )
        return bool(result)
