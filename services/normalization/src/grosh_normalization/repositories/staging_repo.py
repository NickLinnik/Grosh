import json
from dataclasses import dataclass
from typing import Any
from uuid import UUID

import asyncpg


@dataclass(frozen=True)
class StagedRow:
    id: UUID
    payload: dict[str, Any]


class StagingRepo:
    async def insert_staged(
        self,
        conn: asyncpg.Connection,
        user_id: UUID,
        payload: dict[str, Any],
    ) -> None:
        await conn.execute(
            """
            INSERT INTO staging_normalized_transactions (user_id, payload)
            VALUES ($1, $2)
            """,
            user_id,
            json.dumps(payload),
        )

    async def select_staged_for_user(
        self,
        conn: asyncpg.Connection,
        user_id: UUID,
    ) -> list[StagedRow]:
        rows = await conn.fetch(
            """
            SELECT
                id,
                payload
            FROM staging_normalized_transactions
            WHERE user_id = $1
            ORDER BY created_at
            """,
            user_id,
        )
        return [
            StagedRow(id=row["id"], payload=json.loads(row["payload"])) for row in rows
        ]

    async def delete_staged(
        self,
        conn: asyncpg.Connection,
        id: UUID,
    ) -> None:
        await conn.execute(
            """
            DELETE FROM staging_normalized_transactions
            WHERE id = $1
            """,
            id,
        )

    async def select_unlocked_user_ids_with_staged_rows(
        self, conn: asyncpg.Connection
    ) -> list[UUID]:
        rows = await conn.fetch(
            """
            SELECT DISTINCT s.user_id
            FROM staging_normalized_transactions s
            WHERE NOT EXISTS (
                SELECT 1
                FROM reprocessing_locks rl
                WHERE rl.user_id = s.user_id
            )
            """
        )
        return [row["user_id"] for row in rows]

    async def get_staging_age_and_count(
        self, conn: asyncpg.Connection
    ) -> tuple[int | None, int]:
        """Return (age_seconds, row_count) snapshot for the staging table.

        age_seconds is the seconds elapsed since the oldest row's created_at,
        or None when the table is empty (MIN over empty set is NULL, and the
        EXTRACT propagates the NULL). Both values come from one query to keep
        them snapshot-consistent — important because the sweep loop's
        decision-making depends on both.
        """
        row = await conn.fetchrow(
            """
            SELECT
                EXTRACT(EPOCH FROM (now() - MIN(created_at)))::int AS age_seconds,
                COUNT(*)::int AS row_count
            FROM staging_normalized_transactions
            """
        )
        return (row["age_seconds"], row["row_count"])

    async def lock_exists(self, conn: asyncpg.Connection, user_id: UUID) -> bool:
        """Check whether a reprocessing_locks row exists for this user.

        Co-located with staging operations because the staging-routing decision
        in the normalization consumer needs both checks atomic in one transaction.
        """
        value = await conn.fetchval(
            """
            SELECT EXISTS (
                SELECT 1
                FROM reprocessing_locks
                WHERE user_id = $1
            )
            """,
            user_id,
        )
        return bool(value)
