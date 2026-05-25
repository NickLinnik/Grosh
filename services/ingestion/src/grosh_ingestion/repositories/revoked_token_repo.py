from uuid import UUID

import asyncpg


class RevokedTokenRepo:
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
