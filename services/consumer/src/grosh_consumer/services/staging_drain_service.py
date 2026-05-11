"""Staging drain service for the normalization consumer.

After the reprocess job releases its lock (via NOTIFY reprocess_complete, '<user_id>'),
this drain replays staged events back to the normalized_transactions topic so the
pipeline consumer can process them normally.

Two trigger paths:
  1. LISTEN reprocess_complete — fast path, woken by the reprocess job's NOTIFY.
  2. Periodic 60s sweep — safety net for missed NOTIFYs (listener reconnect, blip).

Concurrency / at-least-once:
  Both paths call drain_for_user(user_id) without per-user mutual exclusion.
  A NOTIFY can fire while the sweep is mid-flight on the same user, causing two
  concurrent drains. This is intentional: per-user locks would add complexity with
  little benefit because drain_for_user is idempotent — publish-then-delete ordering
  means a crash between publish and delete causes a re-publish, which the pipeline
  consumer's ON CONFLICT (id) DO NOTHING guard absorbs harmlessly. Two concurrent
  drains publish the same rows, and the idempotency guard makes duplicates no-ops.
  At-least-once is the correct semantic here, not exactly-once.

Lifecycle:
  Use as an async context manager:
      async with StagingDrainService(pool, staging_repo):
          ...

  __aenter__ constructs the Kafka producer, spawns the listener and sweep tasks.
  __aexit__ runs the load-bearing 5-step cleanup (see technical-considerations.md §2.8).
"""

import asyncio
import logging
import os
from uuid import UUID

import asyncpg
from confluent_kafka import Producer
from grosh_shared.db_url import for_asyncpg
from grosh_shared.models import Topic

from grosh_consumer.kafka import on_delivery
from grosh_consumer.models.normalized import NormalizedTransaction
from grosh_consumer.repositories.staging_repo import StagedRow, StagingRepo

logger = logging.getLogger(__name__)

_SWEEP_INTERVAL_SECONDS = 60
_LISTENER_RECONNECT_BACKOFF_SECONDS = 5
_NOTIFY_DRAIN_SHUTDOWN_TIMEOUT_SECONDS = 2.0


class StagingDrainService:
    """Background drain task for staged normalization events.

    Use as an async context manager — `__aenter__` spawns the listener and sweep
    background tasks; `__aexit__` runs the load-bearing 5-step cleanup.
    """

    def __init__(self, pool: asyncpg.Pool, staging_repo: StagingRepo) -> None:
        self._pool = pool
        self._staging_repo = staging_repo
        self._listener_conn: asyncpg.Connection | None = None
        self._notify_drain_tasks: set[asyncio.Task] = set()
        # _producer and background tasks are initialized in __aenter__
        self._producer: Producer | None = None
        self._listener_task: asyncio.Task | None = None
        self._sweep_task: asyncio.Task | None = None

    async def __aenter__(self) -> "StagingDrainService":
        self._producer = Producer(
            {
                "bootstrap.servers": os.environ.get(
                    "KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092"
                ),
                "enable.idempotence": "true",
            }
        )
        self._listener_task = asyncio.create_task(self._run_listener())
        self._sweep_task = asyncio.create_task(self._run_sweep())
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Load-bearing 5-step cleanup. Each step has its own try/except so a
        failure in step N does not skip steps N+1..N+4."""

        # Step 1: signal both background tasks to stop.
        try:
            if self._listener_task is not None:
                self._listener_task.cancel()
            if self._sweep_task is not None:
                self._sweep_task.cancel()
        except Exception:
            logger.exception("Error cancelling background tasks in __aexit__ step 1")

        # Step 2: wait for both tasks so their finally blocks can run (which closes
        # the listener connection cleanly via the finally in _connect_and_listen).
        try:
            tasks_to_gather = [
                t for t in (self._listener_task, self._sweep_task) if t is not None
            ]
            if tasks_to_gather:
                await asyncio.gather(*tasks_to_gather, return_exceptions=True)
        except Exception:
            logger.exception("Error awaiting background tasks in __aexit__ step 2")

        # Step 3: defensive idempotent close of the listener connection in case step
        # 2's finally block did not run (e.g. task was never started).
        try:
            if self._listener_conn is not None and not self._listener_conn.is_closed():
                try:
                    await self._listener_conn.remove_listener(
                        "reprocess_complete", self._on_notify
                    )
                except Exception:
                    pass
                await self._listener_conn.close()
        except Exception:
            logger.exception(
                "Error in defensive listener connection close in __aexit__ step 3"
            )

        # Step 4: wait briefly for in-flight NOTIFY-driven drains, then cancel laggards.
        try:
            if self._notify_drain_tasks:
                try:
                    await asyncio.wait_for(
                        asyncio.gather(
                            *self._notify_drain_tasks, return_exceptions=True
                        ),
                        timeout=_NOTIFY_DRAIN_SHUTDOWN_TIMEOUT_SECONDS,
                    )
                except TimeoutError:
                    for task in self._notify_drain_tasks:
                        task.cancel()
        except Exception:
            logger.exception("Error awaiting NOTIFY drain tasks in __aexit__ step 4")

        # Step 5: flush the producer last so in-flight publishes from steps 2 and 4
        # land before the producer goes away.
        try:
            if self._producer is not None:
                self._producer.flush(timeout=10)
        except Exception:
            logger.exception("Error flushing producer in __aexit__ step 5")

    # ------------------------------------------------------------------
    # NOTIFY listener
    # ------------------------------------------------------------------

    async def _run_listener(self) -> None:
        """Maintain a long-lived LISTEN connection, reconnecting on failure.

        Loops until cancelled by `__aexit__`. Each iteration either runs to
        completion via cancellation (the normal path) or fails with a transient
        connection error and backs off before retrying.
        """
        while True:
            try:
                await self._connect_and_listen()
            except asyncio.CancelledError:
                return
            except Exception:
                logger.exception(
                    "Listener connection lost; reconnecting in %ds",
                    _LISTENER_RECONNECT_BACKOFF_SECONDS,
                )
                await asyncio.sleep(_LISTENER_RECONNECT_BACKOFF_SECONDS)

    async def _connect_and_listen(self) -> None:
        dsn = for_asyncpg(os.environ["DATABASE_URL"])
        # Dedicated connection — not from the pool. LISTEN state is per-connection
        # and cannot be shared safely with application connections.
        conn = await asyncpg.connect(dsn)
        self._listener_conn = conn
        try:
            await conn.add_listener("reprocess_complete", self._on_notify)
            logger.info("Staging drain: LISTEN reprocess_complete established")
            # Park until cancelled by __aexit__. asyncpg invokes _on_notify
            # from its own I/O coroutine; this await just keeps the connection
            # open and the task alive.
            await asyncio.Future()
        finally:
            if not conn.is_closed():
                try:
                    await conn.remove_listener("reprocess_complete", self._on_notify)
                except Exception:
                    pass
                await conn.close()
            self._listener_conn = None

    def _on_notify(
        self,
        connection: asyncpg.Connection,
        pid: int,
        channel: str,
        payload: str,
    ) -> None:
        """asyncpg calls this synchronously from its I/O loop.

        We schedule drain_for_user as a new asyncio task so the listener
        callback returns immediately and does not block further notifications.
        The task is tracked in _notify_drain_tasks so __aexit__ can await it.
        """
        try:
            user_id = UUID(payload)
        except ValueError:
            logger.error(
                "reprocess_complete NOTIFY carried non-UUID payload %r; ignoring",
                payload,
            )
            return
        logger.info(
            "Received NOTIFY reprocess_complete for user %s; scheduling drain",
            user_id,
        )
        task = asyncio.create_task(self.drain_for_user(user_id))
        self._notify_drain_tasks.add(task)
        task.add_done_callback(self._notify_drain_tasks.discard)

    # ------------------------------------------------------------------
    # Periodic sweep
    # ------------------------------------------------------------------

    async def _run_sweep(self) -> None:
        """Drain all unlocked users with staged rows every 60 seconds.

        Loops until cancelled by `__aexit__`. `asyncio.sleep` is interruptible
        by `task.cancel()`, so shutdown response is immediate, not 60s-laggy.
        """
        while True:
            await asyncio.sleep(_SWEEP_INTERVAL_SECONDS)
            await self._sweep_once()

    async def _sweep_once(self) -> None:
        """Single sweep iteration — exposed for direct test invocation."""
        try:
            async with self._pool.acquire() as conn:
                user_ids = (
                    await self._staging_repo.select_unlocked_user_ids_with_staged_rows(
                        conn
                    )
                )
        except Exception:
            logger.exception("Sweep query failed; will retry on next cycle")
            return

        if not user_ids:
            return

        logger.info(
            "Periodic sweep: draining %d unlocked user(s) with staged events",
            len(user_ids),
        )
        for user_id in user_ids:
            try:
                await self.drain_for_user(user_id)
            except Exception:
                logger.exception(
                    "Sweep drain failed for user %s; continuing with remaining users",
                    user_id,
                )

    # ------------------------------------------------------------------
    # Core drain logic
    # ------------------------------------------------------------------

    async def drain_for_user(self, user_id: UUID) -> None:
        """Publish all staged rows for user_id to normalized_transactions, then delete.

        Publish-then-delete ordering ensures at-least-once delivery: a crash between
        publish and delete triggers a re-publish on retry, which is idempotent via the
        pipeline consumer's ON CONFLICT (id) DO NOTHING guard.

        On any publish failure: logs loudly and stops processing this user, leaving
        the undelivered row in staging for the next sweep or NOTIFY retry.
        """
        async with self._pool.acquire() as conn:
            rows: list[StagedRow] = await self._staging_repo.select_staged_for_user(
                conn, user_id
            )

        if not rows:
            return

        logger.info(
            "Draining %d staged row(s) for user %s",
            len(rows),
            user_id,
        )

        for row in rows:
            try:
                normalized = NormalizedTransaction.model_validate(row.payload)
            except Exception:
                logger.exception(
                    "Failed to deserialize staged row %s for user %s; "
                    "leaving in staging for retry",
                    row.id,
                    user_id,
                )
                raise

            try:
                self._publish(normalized)
            except Exception:
                logger.exception(
                    "Kafka publish failed for staged row %s (user %s); "
                    "stopping drain — row remains in staging for next sweep",
                    row.id,
                    user_id,
                )
                raise

            async with self._pool.acquire() as conn:
                await self._staging_repo.delete_staged(conn, row.id)

            logger.debug(
                "Drained staged row %s (tx %s) for user %s",
                row.id,
                normalized.id,
                user_id,
            )

        logger.info("Drain complete for user %s (%d row(s))", user_id, len(rows))

    def _publish(self, normalized: NormalizedTransaction) -> None:
        payload = normalized.model_dump_json().encode()
        key = str(normalized.user_id).encode()
        try:
            self._producer.produce(
                topic=Topic.normalized_transactions,
                key=key,
                value=payload,
                on_delivery=on_delivery,
            )
        except BufferError:
            self._producer.flush(timeout=10)
            self._producer.produce(
                topic=Topic.normalized_transactions,
                key=key,
                value=payload,
                on_delivery=on_delivery,
            )
        self._producer.poll(0)
