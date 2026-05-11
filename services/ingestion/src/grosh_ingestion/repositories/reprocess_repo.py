from datetime import datetime
from uuid import UUID

import asyncpg


class ReprocessRepo:
    async def lock_exists(self, conn: asyncpg.Connection, user_id: UUID) -> bool:
        return await conn.fetchval(
            """
            SELECT EXISTS(
                SELECT 1
                FROM reprocessing_locks
                WHERE user_id = $1
                LIMIT 1
            )
            """,
            user_id,
        )

    async def last_reprocess_at(
        self, conn: asyncpg.Connection, user_id: UUID
    ) -> datetime | None:
        return await conn.fetchval(
            """
            SELECT max(created_at)
            FROM reprocessing_backups
            WHERE user_id = $1
            """,
            user_id,
        )
