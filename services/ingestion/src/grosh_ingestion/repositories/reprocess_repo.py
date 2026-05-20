"""Repository for reprocessing_locks operations owned by the ingestion service.

The ingestion API atomically INSERTs the lock row as part of its trigger
transaction. The consumer pod asserts the row exists and DELETEs it on
successful completion. INSERT ownership is grosh_ingestion; DELETE stays
with grosh_consumer (RBAC enforced in migration 0012).
"""

from uuid import UUID

import asyncpg


class ReprocessRepo:
    async def insert_lock_atomic(
        self,
        conn: asyncpg.Connection,
        user_id: UUID,
    ) -> bool:
        """Insert a reprocessing_locks row for the user.

        Uses ON CONFLICT DO NOTHING so concurrent calls are safe.
        Returns True if the row was inserted, False if it already existed.
        """
        result = await conn.fetchval(
            """
            INSERT INTO reprocessing_locks (user_id)
            VALUES ($1)
            ON CONFLICT DO NOTHING
            RETURNING 1
            """,
            user_id,
        )
        return result is not None

    async def find_locked_user_ids(
        self,
        conn: asyncpg.Connection,
        user_ids: list[UUID],
    ) -> set[UUID]:
        """Return the subset of user_ids that currently have a lock row."""
        if not user_ids:
            return set()
        rows = await conn.fetch(
            """
            SELECT user_id
            FROM reprocessing_locks
            WHERE user_id = ANY($1::uuid[])
            """,
            user_ids,
        )
        return {row["user_id"] for row in rows}
