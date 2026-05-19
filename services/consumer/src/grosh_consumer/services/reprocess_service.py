"""Service layer for the reprocessing job state machine.

Owns the 11-step orchestration logic. All SQL goes through repos; no SQL
strings live here. The same instance is reused across all users in one job run.
"""

import asyncio
import logging
from uuid import UUID

import asyncpg
from confluent_kafka import KafkaException, Producer
from grosh_shared.models import Topic

from grosh_consumer.kafka import on_delivery
from grosh_consumer.models.normalized import NormalizedTransaction
from grosh_consumer.repositories.reprocess_repo import (
    ReprocessError,
    ReprocessLockConflictError,
    ReprocessRepo,
)
from grosh_consumer.repositories.transaction_repo import TransactionRepo

logger = logging.getLogger(__name__)

_CATCHUP_POLL_INTERVAL_S: float = 2.0
_CATCHUP_TIMEOUT_S: float = 300.0  # 5 minutes


class ReprocessService:
    def __init__(
        self,
        repo: ReprocessRepo,
        transaction_repo: TransactionRepo,
        producer: Producer,
    ) -> None:
        self._repo = repo
        self._transaction_repo = transaction_repo
        self._producer = producer

    async def reprocess_user(self, conn: asyncpg.Connection, user_id: UUID) -> None:
        """Run the full 11-step reprocess state machine for one user.

        The dedicated session connection is owned by the caller (entrypoint).
        Advisory locks are session-scoped; the same conn must be used throughout.
        """
        try:
            # Steps 1-2: stale-lock cleanup + atomic acquire
            await self._repo.clean_stale_locks(conn, user_id)
            await self._repo.acquire_lock_atomic(conn, user_id)

            # Step 3: snapshot to backup
            snapshot_ids = await self._repo.snapshot_transactions(conn, user_id)

            # Steps 4-5: read rows, reconstruct events
            rows = await self._transaction_repo.select_for_user(conn, user_id)
            events = [row.to_normalized() for row in rows]

            # ---- post-DELETE recovery zone start ----
            await self._repo.delete_user_transactions(conn, user_id)
            logger.info("Deleted %d transactions for user %s", len(rows), user_id)
            try:
                # Steps 6-7: publish replay events
                published = self._publish_replay_events(events)
                logger.info(
                    "Published %d replay events for user %s", published, user_id
                )

                # Step 8: wait for pipeline consumer to catch up
                await self._wait_for_pipeline_catchup(conn, user_id, published)

                # Step 9: verify all snapshot IDs are back
                missing = await self._repo.verify_snapshot(conn, user_id, snapshot_ids)
                if missing:
                    raise ReprocessError(
                        f"Verification failed: {len(missing)} missing IDs"
                    )
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

        except ReprocessLockConflictError:
            # routing signal for the caller; no DELETE happened, no recovery needed
            raise
        finally:
            try:
                self._producer.flush(timeout=10)
            except Exception as flush_exc:
                logger.warning("Producer flush failed during cleanup: %s", flush_exc)

    def _publish_replay_events(self, events: list[NormalizedTransaction]) -> int:
        """Publish reconstructed NormalizedTransaction events to normalized topic.

        Bypasses staging — the reprocess job is the lock holder; staging is for
        events arriving via the normalization consumer while the lock is held.
        Returns the count of published messages.
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
