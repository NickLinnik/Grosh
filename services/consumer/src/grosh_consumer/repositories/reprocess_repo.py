"""Repository owning all SQL for the reprocessing job.

Uses a dedicated asyncpg.Connection (not a pool connection) because advisory
locks are session-scoped — a pool connection releases the lock when returned.
"""

import json
import logging
from typing import Any
from uuid import UUID

import asyncpg

logger = logging.getLogger(__name__)


class ReprocessError(Exception):
    """Raised when the post-replay verification or restore step fails."""


class ReprocessRepo:
    async def list_all_user_ids(self, conn: asyncpg.Connection) -> list[UUID]:
        """Return all distinct user IDs that have transactions, ordered."""
        rows = await conn.fetch(
            """
            SELECT DISTINCT user_id
            FROM transactions
            ORDER BY user_id
            """
        )
        return [row["user_id"] for row in rows]

    async def lock_exists(self, conn: asyncpg.Connection, user_id: UUID) -> bool:
        """Return True if a reprocessing_locks row exists for this user.

        Used by the pod entrypoint to assert the ingestion API inserted the lock
        before continuing. If absent, the pod was started spuriously or the lock
        was already released — skip this user cleanly.
        """
        result = await conn.fetchval(
            """
            SELECT 1
            FROM reprocessing_locks
            WHERE user_id = $1
            """,
            user_id,
        )
        return result is not None

    async def acquire_advisory_lock(
        self, conn: asyncpg.Connection, user_id: UUID
    ) -> None:
        """Acquire a session-scoped advisory lock for this user's reprocess slot.

        Defense-in-depth after the lock_exists assertion. The advisory lock
        prevents a concurrent pod restart from processing the same user
        simultaneously. The lock is released by release_lock_atomic.
        """
        lock_key = f"reprocess:{user_id}"
        await conn.execute(
            "SELECT pg_advisory_lock(hashtext($1))",
            lock_key,
        )

    async def clean_stale_locks(self, conn: asyncpg.Connection, user_id: UUID) -> int:
        """Delete reprocessing_locks rows for this user that have no live advisory lock.

        Uses pg_locks introspection — the advisory lock IS the liveness signal.
        Postgres auto-releases advisory locks at session end, so "no holder in
        pg_locks" definitively means the prior session is dead regardless of
        how recently locked_at was written.

        Returns the number of deleted rows; logs a message if any were deleted.
        """
        deleted = await conn.fetchval(
            """
            WITH deleted AS (
                DELETE FROM reprocessing_locks rl
                WHERE rl.user_id = $1
                    AND NOT EXISTS (
                        SELECT 1
                        FROM pg_locks
                        WHERE locktype = 'advisory'
                            AND objid = (
                                hashtext('reprocess:' || rl.user_id::text)::bigint
                                & x'ffffffff'::bigint
                            )
                    )
                RETURNING 1
            )
            SELECT COUNT(*) FROM deleted
            """,
            user_id,
        )
        count = int(deleted)
        if count > 0:
            logger.info(
                "Cleaned %d stale reprocessing_locks row(s) for user %s",
                count,
                user_id,
            )
        return count

    async def snapshot_transactions(
        self, conn: asyncpg.Connection, user_id: UUID
    ) -> list[UUID]:
        """Read all transaction rows, write a JSONB backup, return the IDs.

        The backup row persists regardless of outcome (retained for 30 days per
        ops policy) and is used by restore_from_backup on failure.
        Returns the list of IDs for later verification in verify_snapshot.
        """
        rows = await conn.fetch(
            """
            SELECT id, row_to_json(transactions.*)::jsonb AS row_json
            FROM transactions
            WHERE user_id = $1
            ORDER BY
                time,
                id
            """,
            user_id,
        )
        tx_ids = [row["id"] for row in rows]
        snapshot_data = [
            json.loads(row["row_json"])
            if isinstance(row["row_json"], str)
            else dict(row["row_json"])
            for row in rows
        ]

        await conn.execute(
            """
            INSERT INTO reprocessing_backups (user_id, data)
            VALUES ($1, $2)
            """,
            user_id,
            json.dumps(snapshot_data),
        )
        logger.info(
            "Snapshot of %d transaction(s) written to reprocessing_backups for user %s",
            len(tx_ids),
            user_id,
        )
        return tx_ids

    async def delete_user_transactions(
        self, conn: asyncpg.Connection, user_id: UUID
    ) -> None:
        """Delete all transactions for the given user."""
        await conn.execute(
            "DELETE FROM transactions WHERE user_id = $1",
            user_id,
        )

    async def count_for_user(self, conn: asyncpg.Connection, user_id: UUID) -> int:
        """Return the current transaction count for the user."""
        val = await conn.fetchval(
            "SELECT COUNT(*) FROM transactions WHERE user_id = $1",
            user_id,
        )
        return int(val)

    async def verify_snapshot(
        self, conn: asyncpg.Connection, user_id: UUID, snapshot_ids: list[UUID]
    ) -> list[UUID]:
        """Return IDs from the snapshot that are absent from the transactions table.

        An empty return list means verification passed. New webhook events (IDs not
        in the snapshot) are not checked — they are expected and correct.
        """
        if not snapshot_ids:
            return []

        present_rows = await conn.fetch(
            """
            SELECT id
            FROM transactions
            WHERE user_id = $1
                AND id = ANY($2::uuid[])
            """,
            user_id,
            snapshot_ids,
        )
        present_ids = {row["id"] for row in present_rows}
        return [tx_id for tx_id in snapshot_ids if tx_id not in present_ids]

    async def release_lock_atomic(
        self, conn: asyncpg.Connection, user_id: UUID
    ) -> None:
        """Delete the routing-signal row, release advisory lock, and NOTIFY atomically.

        NOTIFY reprocess_complete fires only on transaction commit (Postgres
        semantics), so the normalization consumer's drain task is woken up exactly
        when the reprocessing_locks row becomes invisible — no gap.
        """
        lock_key = f"reprocess:{user_id}"
        async with conn.transaction():
            await conn.execute(
                "DELETE FROM reprocessing_locks WHERE user_id = $1",
                user_id,
            )
            await conn.execute(
                "SELECT pg_advisory_unlock(hashtext($1))",
                lock_key,
            )
            await conn.execute(
                "SELECT pg_notify('reprocess_complete', $1)",
                str(user_id),
            )

    async def restore_from_backup(
        self, conn: asyncpg.Connection, user_id: UUID
    ) -> None:
        """Re-insert transactions from the most recent reprocessing_backups row.

        The entire restore is wrapped in a single transaction so a mid-restore
        crash leaves no half-restored state — either all rows come back or none.
        The advisory lock is still held during restore, so webhook events continue
        routing to staging. After restore the caller must call release_lock_atomic
        to drain staged events on top of the restored state.

        Raises ReprocessError if no backup row exists.
        """
        backup_row = await conn.fetchrow(
            """
            SELECT data
            FROM reprocessing_backups
            WHERE user_id = $1
            ORDER BY created_at DESC
            LIMIT 1
            """,
            user_id,
        )
        if backup_row is None:
            raise ReprocessError(f"No backup found for user {user_id} — cannot restore")

        raw = backup_row["data"]
        snapshot: list[dict[str, Any]] = (
            json.loads(raw) if isinstance(raw, str) else raw
        )

        logger.warning(
            "Verification failed — restoring %d transaction(s) from backup for user %s",
            len(snapshot),
            user_id,
        )

        # jsonb_populate_record casts every field from JSON using the table's own
        # column type definitions — timestamps, UUIDs, enums, etc. are all handled
        # by Postgres itself. The transaction wrapper makes this all-or-nothing.
        async with conn.transaction():
            for tx in snapshot:
                await conn.execute(
                    """
                    INSERT INTO transactions
                    SELECT * FROM jsonb_populate_record(
                        NULL::transactions,
                        $1::jsonb
                    )
                    ON CONFLICT (id) DO NOTHING
                    """,
                    json.dumps(tx),
                )

        logger.warning(
            "Restore complete for user %s — %d row(s) re-inserted",
            user_id,
            len(snapshot),
        )
