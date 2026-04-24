from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

import asyncpg


@dataclass(frozen=True)
class UserSettingsRow:
    default_rate_source: str | None
    updated_at: datetime


class SettingsRepo:
    async def get_by_user_id(
        self, conn: asyncpg.Connection, user_id: UUID
    ) -> UserSettingsRow | None:
        row = await conn.fetchrow(
            "SELECT default_rate_source, updated_at"
            " FROM user_settings WHERE user_id = $1",
            user_id,
        )
        if row is None:
            return None
        return UserSettingsRow(
            default_rate_source=row["default_rate_source"],
            updated_at=row["updated_at"],
        )

    async def upsert(
        self,
        conn: asyncpg.Connection,
        user_id: UUID,
        default_rate_source: str | None,
    ) -> UserSettingsRow:
        row = await conn.fetchrow(
            """
            INSERT INTO user_settings (user_id, default_rate_source)
            VALUES ($1, $2)
            ON CONFLICT (user_id)
            DO UPDATE SET default_rate_source = $2
            RETURNING default_rate_source, updated_at
            """,
            user_id,
            default_rate_source,
        )
        return UserSettingsRow(
            default_rate_source=row["default_rate_source"],
            updated_at=row["updated_at"],
        )

    async def validate_rate_source(self, conn: asyncpg.Connection, source: str) -> bool:
        return await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM rate_source_config WHERE source = $1)",
            source,
        )
