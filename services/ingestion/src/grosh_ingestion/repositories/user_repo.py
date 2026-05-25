from datetime import datetime
from typing import cast
from uuid import UUID

import asyncpg
from grosh_shared.domain.models import UserRole


class UserRepo:
    async def is_active(self, conn: asyncpg.Connection, user_id: UUID) -> bool:
        result = await conn.fetchval(
            "SELECT is_active FROM users WHERE id = $1", user_id
        )
        return result is True

    async def get_role(
        self, conn: asyncpg.Connection, user_id: UUID
    ) -> UserRole | None:
        value = await conn.fetchval(
            """
            SELECT role
            FROM users
            WHERE id = $1
            """,
            user_id,
        )
        return UserRole(value) if value is not None else None

    async def list_all_ids(self, conn: asyncpg.Connection) -> list[UUID]:
        """Return all user IDs as a snapshot.

        New users created after this query are NOT included — deliberate
        snapshot semantic for the admin bulk-reprocess endpoint.
        """
        rows = await conn.fetch(
            """
            SELECT id
            FROM users
            ORDER BY id
            """
        )
        return [row["id"] for row in rows]

    async def claim_reprocess_slot(
        self,
        conn: asyncpg.Connection,
        user_id: UUID,
    ) -> bool:
        """Atomic check-and-set: claim a reprocess slot if outside the 1-hour window.

        Returns True if the slot was claimed (timestamp updated to now).
        Returns False if rate-limited (no UPDATE; existing timestamp preserved).
        """
        row = await conn.fetchrow(
            """
            UPDATE users
            SET last_reprocess_started_at = now()
            WHERE id = $1
              AND (
                  last_reprocess_started_at IS NULL
                  OR last_reprocess_started_at < now() - interval '1 hour'
              )
            RETURNING last_reprocess_started_at
            """,
            user_id,
        )
        return row is not None

    async def force_claim_reprocess_slot(
        self,
        conn: asyncpg.Connection,
        user_id: UUID,
    ) -> None:
        """Unconditional UPDATE — admin-only path; bypasses the rate-limit window.

        Still updates the timestamp so subsequent non-force calls within the
        1-hour window are correctly rate-limited.
        """
        row = await conn.fetchrow(
            """
            UPDATE users
            SET last_reprocess_started_at = now()
            WHERE id = $1
            RETURNING last_reprocess_started_at
            """,
            user_id,
        )
        if row is None:
            raise ValueError(f"User {user_id} not found")

    async def get_next_eligible_at(
        self,
        conn: asyncpg.Connection,
        user_id: UUID,
    ) -> datetime | None:
        """Return the timestamp when the user becomes eligible for the next reprocess.

        Returns None if the user has never reprocessed (no prior timestamp).
        """
        result = await conn.fetchval(
            """
            SELECT last_reprocess_started_at + interval '1 hour'
            FROM users
            WHERE id = $1
            """,
            user_id,
        )
        return cast(datetime | None, result)
