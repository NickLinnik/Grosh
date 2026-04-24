from uuid import UUID

import asyncpg


class UserSettingsRepo:
    async def get_default_rate_source(
        self, conn: asyncpg.Connection, user_id: UUID
    ) -> str | None:
        row = await conn.fetchrow(
            "SELECT default_rate_source FROM user_settings WHERE user_id = $1",
            user_id,
        )
        return row["default_rate_source"] if row else None

    async def is_valid_rate_source(self, conn: asyncpg.Connection, source: str) -> bool:
        return await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM rate_source_config WHERE source = $1)",
            source,
        )
