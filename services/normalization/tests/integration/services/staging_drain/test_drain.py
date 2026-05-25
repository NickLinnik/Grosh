"""Integration tests for StagingDrainService drain logic and NOTIFY dispatch.

Supplements test_staging_drain_integration.py (which covers the sweep and
__aexit__ cleanup isolation). This file pins the remaining uncovered paths:

  - drain_for_user no-op when staging is empty (line 319)
  - drain_for_user publish failure → re-raise, no delete (lines 341-348)
  - _on_notify malformed payload → log error, no task (lines 208-213)
  - _on_notify valid UUID → drain task scheduled (lines 176-191)
  - __aexit__ step 4 cancels in-flight drain on timeout (lines 134-138)

Real Postgres is used (via the session-scoped db_pool fixture). The Kafka
producer is faked — tests do not need a live Redpanda.
"""

import asyncio
import logging
from unittest.mock import MagicMock
from uuid import UUID, uuid4

import pytest
import pytest_asyncio

from grosh_normalization.repositories.staging_repo import StagingRepo
from grosh_normalization.services import staging_drain_service as _sds_module
from grosh_normalization.services.staging_drain_service import StagingDrainService
from tests.helpers import make_event
from tests.integration.conftest import SingleConnectionPool, insert_user

pytestmark = pytest.mark.asyncio


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def staging_repo() -> StagingRepo:
    return StagingRepo()


@pytest_asyncio.fixture
async def drain(conn, staging_repo):
    """StagingDrainService with a mock Kafka producer + SingleConnectionPool.

    Producer is a plain MagicMock — produce() does nothing by default.
    Tests that need failure behaviour configure it directly.
    """
    instance = StagingDrainService(SingleConnectionPool(conn), staging_repo)
    instance._producer = MagicMock()
    return instance


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _stage_event(conn, staging_repo, user_id):
    """Insert one staged normalized event for user_id."""
    event = make_event(user_id=user_id)
    payload = event.model_dump(mode="json")
    await staging_repo.insert_staged(conn, user_id, payload)
    return event


async def _count_staged(conn, user_id) -> int:
    val = await conn.fetchval(
        """
        SELECT COUNT(*)
        FROM staging_normalized_transactions
        WHERE user_id = $1
        """,
        user_id,
    )
    return int(val)


# ---------------------------------------------------------------------------
# Test 4: drain_for_user no-op when staging is empty
# ---------------------------------------------------------------------------


async def test_drain_for_user_with_no_staged_rows_is_noop(conn, drain, staging_repo):
    """Spurious produce + delete on empty input wastes Kafka quota.

    This test catches: spurious publish or DELETE call when nothing to drain.
    """
    user_id = await insert_user(conn)

    await drain.drain_for_user(user_id)

    drain._producer.produce.assert_not_called()
    # Verify staging table is still empty and no delete was attempted.
    assert await _count_staged(conn, user_id) == 0


# ---------------------------------------------------------------------------
# Test 5: publish failure → re-raise without deleting staged rows
# ---------------------------------------------------------------------------


async def test_drain_for_user_publish_failure_re_raises_and_does_not_delete(
    conn, drain, staging_repo
):
    """Data loss: rows deleted before broker confirms.

    This test catches: publish exception path must not delete staged rows.
    """
    user_id = await insert_user(conn)
    await _stage_event(conn, staging_repo, user_id)
    await _stage_event(conn, staging_repo, user_id)

    drain._producer.produce.side_effect = KafkaProduceError("broker unavailable")

    with pytest.raises(KafkaProduceError):
        await drain.drain_for_user(user_id)

    # Both staged rows must remain — nothing was deleted on publish failure.
    assert await _count_staged(conn, user_id) == 2


class KafkaProduceError(Exception):
    """Minimal stand-in for confluent_kafka produce errors in tests."""


# ---------------------------------------------------------------------------
# Test 6: _on_notify malformed payload → log error, no task
# ---------------------------------------------------------------------------


async def test_on_notify_with_malformed_payload_is_logged_and_ignored(
    conn, drain, staging_repo, caplog
):
    """Stray manual NOTIFY crashes the listener task, stopping drain for everyone."""
    caplog.set_level(
        logging.ERROR,
        logger="grosh_normalization.services.staging_drain_service",
    )
    user_id = await insert_user(conn)
    await _stage_event(conn, staging_repo, user_id)

    initial_task_count = len(drain._notify_drain_tasks)

    # Call _on_notify directly — simulates the asyncpg callback with garbage payload.
    drain._on_notify(
        connection=None,  # type: ignore[arg-type]
        pid=0,
        channel="reprocess_complete",
        payload="not-a-uuid",
    )

    # No new drain task should have been scheduled.
    assert len(drain._notify_drain_tasks) == initial_task_count

    # An ERROR log line must have been emitted.
    error_records = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert error_records, (
        "Expected an ERROR log for non-UUID NOTIFY payload; got none. "
        f"Records: {[(r.levelno, r.getMessage()) for r in caplog.records]}"
    )

    # Staging row must be untouched — no drain was triggered.
    assert await _count_staged(conn, user_id) == 1


# ---------------------------------------------------------------------------
# Test 7: _on_notify valid UUID → drain task scheduled
# ---------------------------------------------------------------------------


async def test_on_notify_with_valid_uuid_schedules_drain_task(
    conn, drain, staging_repo
):
    """NOTIFY received but no drain triggered — reprocess hangs forever.

    This test catches: NOTIFY → drain dispatch contract broken silently.
    """
    user_id = await insert_user(conn)
    await _stage_event(conn, staging_repo, user_id)

    initial_task_count = len(drain._notify_drain_tasks)

    # Call _on_notify directly with a valid UUID payload.
    drain._on_notify(
        connection=None,  # type: ignore[arg-type]
        pid=0,
        channel="reprocess_complete",
        payload=str(user_id),
    )

    # Exactly one new drain task must have been added.
    assert len(drain._notify_drain_tasks) == initial_task_count + 1

    # Let the task run to completion so the rolled-back transaction teardown
    # doesn't race with a dangling task holding a DB reference.
    await asyncio.gather(*drain._notify_drain_tasks, return_exceptions=True)


# ---------------------------------------------------------------------------
# Test 8: __aexit__ step 4 cancels in-flight drain on timeout
# ---------------------------------------------------------------------------


async def test_aexit_cancels_in_flight_drain_with_timeout(conn, staging_repo):
    """Shutdown hangs on slow drain → K8s container kill, lost shutdown ordering."""
    original_timeout = _sds_module._NOTIFY_DRAIN_SHUTDOWN_TIMEOUT_SECONDS
    _sds_module._NOTIFY_DRAIN_SHUTDOWN_TIMEOUT_SECONDS = 0.1

    try:
        mock_producer = MagicMock()
        # Make produce a real async-compatible no-op so drain actually starts.
        mock_producer.produce.return_value = None
        mock_producer.poll.return_value = 0

        instance = StagingDrainService(SingleConnectionPool(conn), staging_repo)
        instance._producer = mock_producer

        slow_task_started = asyncio.Event()

        async def _slow_drain(user_id: UUID) -> None:
            slow_task_started.set()
            await asyncio.sleep(10)  # Much longer than the 0.1s timeout.

        # Replace drain_for_user with a slow stub so the task is always in-flight
        # when __aexit__ runs.
        instance.drain_for_user = _slow_drain  # type: ignore[method-assign]

        # Manually register an in-flight task that mimics the NOTIFY path.
        task: asyncio.Task[None] = asyncio.create_task(_slow_drain(uuid4()))
        instance._notify_drain_tasks.add(task)
        task.add_done_callback(instance._notify_drain_tasks.discard)

        # Wait until the slow drain actually starts before triggering __aexit__.
        await asyncio.wait_for(slow_task_started.wait(), timeout=1.0)

        start = asyncio.get_event_loop().time()

        # __aexit__ must return promptly — well within 0.1s timeout + small slack.
        await instance.__aexit__(None, None, None)

        elapsed = asyncio.get_event_loop().time() - start

        # Should complete in << 1s (0.1s timeout + cancellation overhead).
        assert (
            elapsed < 1.0
        ), f"__aexit__ took {elapsed:.2f}s — expected < 1.0s with 0.1s drain timeout"

        # Let cancellation propagate one event-loop tick so task state is final.
        await asyncio.sleep(0)

        # The slow task must have been cancelled. `task.done()` alone would also
        # accept a task that swallowed CancelledError and completed normally —
        # which would mask a real bug where __aexit__'s wait_for didn't cancel.
        assert task.cancelled(), (
            "Expected the in-flight drain task to be cancelled after __aexit__; "
            f"task.done()={task.done()}, task.cancelled()={task.cancelled()}"
        )
    finally:
        _sds_module._NOTIFY_DRAIN_SHUTDOWN_TIMEOUT_SECONDS = original_timeout
