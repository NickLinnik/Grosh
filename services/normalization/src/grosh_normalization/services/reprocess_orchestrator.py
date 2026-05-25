"""Orchestrator for the reprocessing job state machine.

Owns the 11-step orchestration logic and the Kafka publish step that
republishes reconstructed events to normalized_transactions. All SQL goes
through repos; no SQL strings live here. The same instance is reused
across all users in one job run.

The publish step is a private method on this class rather than a separate
service — there's exactly one caller and the orchestrator owns the producer
lifecycle anyway. Side-effecting Kafka logic is grouped at the bottom of
the file under a section comment so the state machine reads top-down
without jumping between modules.
"""

import asyncio
import logging
from uuid import UUID

import asyncpg
from confluent_kafka import KafkaException, Producer
from grosh_shared.domain.models import Topic
from grosh_shared.domain.normalized import NormalizedTransaction

from grosh_normalization.kafka import on_delivery
from grosh_normalization.repositories.reprocess_repo import (
    ReprocessError,
    ReprocessRepo,
)
from grosh_normalization.repositories.transaction_read_repo import TransactionReadRepo

logger = logging.getLogger(__name__)

_CATCHUP_POLL_INTERVAL_S: float = 2.0
_CATCHUP_TIMEOUT_S: float = 300.0  # 5 minutes


class ReprocessOrchestrator:
    def __init__(
        self,
        repo: ReprocessRepo,
        transaction_read_repo: TransactionReadRepo,
        producer: Producer,
    ) -> None:
        self._repo = repo
        self._transaction_read_repo = transaction_read_repo
        self._producer = producer

    async def reprocess_user(self, conn: asyncpg.Connection, user_id: UUID) -> bool:
        """Run the full reprocess state machine for one user.

        Returns True if reprocessing completed (or was attempted), False if the
        lock row was absent and this user was skipped cleanly.

        The dedicated session connection is owned by the caller (entrypoint).
        Advisory locks are session-scoped; the same conn must be used throughout.
        """
        # Step 1: assert the ingestion API inserted the lock before we start.
        # If the row is absent the job was started spuriously or the lock was
        # already released — skip without touching any transactions rows.
        if not await self._repo.lock_exists(conn, user_id):
            logger.info(
                "Reprocessing lock not found for user_id=%s; exiting cleanly",
                user_id,
            )
            return False

        # Step 2: acquire session-scoped advisory lock (defense-in-depth).
        # The lock row already exists (asserted above), so this is a
        # second-layer guard against concurrent pod restarts.
        await self._repo.acquire_advisory_lock(conn, user_id)

        # Step 3: snapshot to backup
        snapshot_ids = await self._repo.snapshot_transactions(conn, user_id)

        # Steps 4-5: read rows, reconstruct events
        rows = await self._transaction_read_repo.select_for_user(conn, user_id)
        events = [row.to_normalized() for row in rows]

        # ---- post-DELETE recovery zone start ----
        await self._repo.delete_user_transactions(conn, user_id)
        logger.info("Deleted %d transactions for user %s", len(rows), user_id)
        try:
            # Steps 6-7: publish replay events
            published = self._publish_normalized_events(user_id, events)
            logger.info("Published %d replay events for user %s", published, user_id)

            # Step 8: wait for pipeline consumer to catch up
            await self._wait_for_pipeline_catchup(conn, user_id, published)

            # Step 9: verify all snapshot IDs are back
            missing = await self._repo.verify_snapshot(conn, user_id, snapshot_ids)
            if missing:
                raise ReprocessError(f"Verification failed: {len(missing)} missing IDs")
        except Exception as exc:
            logger.error(
                "Reprocess failed post-DELETE for user %s — restoring from backup",
                user_id,
            )
            await self._repo.restore_from_backup(conn, user_id)
            await self._repo.release_lock_atomic(conn, user_id)
            raise ReprocessError(
                f"Reprocess failed post-DELETE for user {user_id}, "
                f"restored from backup: {exc}"
            ) from exc
        # ---- post-DELETE recovery zone end ----

        # Step 10: release lock + NOTIFY
        await self._repo.release_lock_atomic(conn, user_id)
        logger.info("Reprocess complete for user %s", user_id)

        return True

    async def _wait_for_pipeline_catchup(
        self,
        conn: asyncpg.Connection,
        user_id: UUID,
        expected_count: int,
    ) -> None:
        """Poll transactions table until the count reaches expected_count.

        New webhook events arriving during reprocess are staged (not written to
        transactions), so the count stabilises at exactly expected_count.
        Times out after _CATCHUP_TIMEOUT_S seconds.
        """
        if expected_count == 0:
            return

        elapsed = 0.0
        while elapsed < _CATCHUP_TIMEOUT_S:
            actual = await self._repo.count_for_user(conn, user_id)
            logger.debug(
                "Catchup poll: user %s has %d/%d transactions",
                user_id,
                actual,
                expected_count,
            )
            if actual >= expected_count:
                return
            await asyncio.sleep(_CATCHUP_POLL_INTERVAL_S)
            elapsed += _CATCHUP_POLL_INTERVAL_S

        raise ReprocessError(
            f"Pipeline catchup timeout for user {user_id}: "
            f"expected {expected_count} transactions after {_CATCHUP_TIMEOUT_S}s"
        )

    # ─── Kafka side effects ─────────────────────────────────────────────
    # Publishing to normalized_transactions is the only Kafka interaction
    # of this orchestrator. Kept as a private method (not a separate class)
    # because there is exactly one caller — the state machine above — and
    # the producer lifecycle is owned by the same scope. The retry-on-
    # BufferError pattern absorbs the one Kafka exception the producer can
    # raise synchronously (the rest land in the on_delivery callback).

    def _publish_normalized_events(
        self, user_id: UUID, events: list[NormalizedTransaction]
    ) -> int:
        """Publish all events for a user to normalized_transactions.

        Returns the count of published messages. Flushes the producer before
        returning so the catchup wait sees the events in-flight. Retries each
        produce() once on synchronous BufferError; delivery errors surface
        through the on_delivery callback.

        Bypasses staging — the reprocess job is the lock holder; staging is
        for events arriving via the normalization consumer while the lock
        is held.
        """
        for event in events:
            payload = event.model_dump_json().encode()
            key = str(event.user_id).encode()
            try:
                self._producer.produce(
                    topic=Topic.normalized_transactions,
                    key=key,
                    value=payload,
                    on_delivery=on_delivery,
                )
            except KafkaException as exc:
                self._producer.flush(timeout=10)
                self._producer.produce(
                    topic=Topic.normalized_transactions,
                    key=key,
                    value=payload,
                    on_delivery=on_delivery,
                )
                logger.warning(
                    "Kafka BufferError for event %s, retried: %s", event.id, exc
                )
            self._producer.poll(0)

        self._producer.flush(timeout=30)
        return len(events)
