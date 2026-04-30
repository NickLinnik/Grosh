from uuid import UUID

import asyncpg


class UserRepo:
    async def is_active(self, conn: asyncpg.Connection, user_id: UUID) -> bool:
        result = await conn.fetchval(
            "SELECT is_active FROM users WHERE id = $1", user_id
        )
        return result is True

    async def get_role(self, conn: asyncpg.Connection, user_id: UUID) -> str | None:
        return await conn.fetchval(
            """
            SELECT role
            FROM users
            WHERE id = $1
            """,
            user_id,
        )
