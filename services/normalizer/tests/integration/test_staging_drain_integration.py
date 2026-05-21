"""Integration tests for StagingDrainService.

Tests use real Postgres (via the session-scoped db_pool fixture) and a mock
Kafka producer so we don't need a live Redpanda for these specific scenarios.

Scenarios covered:
  1. NOTIFY path — drain_for_user empties staging for a user.
  2. Sweep path — _sweep_once() drains unlocked users with staged rows.
  3. No-drain-while-locked — sweep leaves staged rows alone when reprocessing_locks
     row exists for that user.
  4. __aexit__ cleanup isolation — a flush() failure in step 5 does not prevent
     earlier steps from running.
  5. NOTIFY drain task tracking — tasks fired via _on_notify are awaited during
     __aexit__ even if __aexit__ is called before the drain completes.
"""

import asyncio
from unittest.mock import MagicMock, patch

import pytest
import pytest_asyncio

from grosh_normalizer.repositories.staging_repo import StagingRepo
from grosh_normalizer.services.staging_drain_service import StagingDrainService
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
    """StagingDrainService with a mock Kafka producer so tests don't need Redpanda.

    Uses SingleConnectionPool so the service acquires the same rolled-back-transaction
    connection the test holds, keeping all writes in the same transaction snapshot.
    """
    instance = StagingDrainService(SingleConnectionPool(conn), staging_repo)
    instance._producer = MagicMock()
    return instance


# ---------------------------------------------------------------------------
# Helper: insert a staged row via the repo
# ---------------------------------------------------------------------------


async def _stage_event(conn, staging_repo, user_id):
    """Insert one staged normalized event and return its payload."""
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
# 1. NOTIFY path
# ---------------------------------------------------------------------------


async def test_notify_path_drains_staged_rows(conn, drain, staging_repo):
    """drain_for_user empties staging for a user (same path _on_notify schedules)."""
    user_id = await insert_user(conn)
    event = await _stage_event(conn, staging_repo, user_id)

    await drain.drain_for_user(user_id)

    assert await _count_staged(conn, user_id) == 0
    drain._producer.produce.assert_called_once()

    call_kwargs = drain._producer.produce.call_args.kwargs
    assert str(user_id).encode() == call_kwargs["key"]
    assert str(event.id).encode() in call_kwargs["value"]


async def test_notify_path_publish_then_delete_ordering(conn, drain, staging_repo):
    """Staging row is deleted AFTER Kafka produce, not before."""
    user_id = await insert_user(conn)
    await _stage_event(conn, staging_repo, user_id)
    await _stage_event(conn, staging_repo, user_id)

    produce_call_count_at_delete: list[int] = []

    original_delete = staging_repo.delete_staged

    async def tracking_delete(c, row_id):
        produce_call_count_at_delete.append(drain._producer.produce.call_count)
        await original_delete(c, row_id)

    staging_repo.delete_staged = tracking_delete

    await drain.drain_for_user(user_id)

    assert drain._producer.produce.call_count == 2
    assert await _count_staged(conn, user_id) == 0
    assert all(count >= 1 for count in produce_call_count_at_delete)


# ---------------------------------------------------------------------------
# 2. Sweep path
# ---------------------------------------------------------------------------


async def test_sweep_drains_unlocked_users(conn, drain, staging_repo):
    """_sweep_once() drains staged rows for users with no reprocessing_locks row."""
    user_id = await insert_user(conn)
    await _stage_event(conn, staging_repo, user_id)

    await drain._sweep_once()

    assert await _count_staged(conn, user_id) == 0
    drain._producer.produce.assert_called_once()


async def test_sweep_drains_multiple_unlocked_users(conn, drain, staging_repo):
    """_sweep_once() processes all unlocked users with staged rows."""
    user_a = await insert_user(conn)
    user_b = await insert_user(conn)

    await _stage_event(conn, staging_repo, user_a)
    await _stage_event(conn, staging_repo, user_b)

    await drain._sweep_once()

    assert await _count_staged(conn, user_a) == 0
    assert await _count_staged(conn, user_b) == 0
    assert drain._producer.produce.call_count == 2


# ---------------------------------------------------------------------------
# 3. No-drain-while-locked
# ---------------------------------------------------------------------------


async def test_sweep_skips_locked_users(conn, drain, staging_repo):
    """Sweep must not drain a user whose reprocessing_locks row still exists."""
    user_id = await insert_user(conn)
    await _stage_event(conn, staging_repo, user_id)

    await conn.execute(
        "INSERT INTO reprocessing_locks (user_id) VALUES ($1)",
        user_id,
    )

    await drain._sweep_once()

    assert await _count_staged(conn, user_id) == 1
    drain._producer.produce.assert_not_called()


async def test_sweep_drains_unlocked_user_while_another_is_locked(
    conn, drain, staging_repo
):
    """Locked user is skipped; unlocked user is drained in the same sweep cycle."""
    locked_user = await insert_user(conn)
    unlocked_user = await insert_user(conn)

    await _stage_event(conn, staging_repo, locked_user)
    await _stage_event(conn, staging_repo, unlocked_user)

    await conn.execute(
        "INSERT INTO reprocessing_locks (user_id) VALUES ($1)",
        locked_user,
    )

    await drain._sweep_once()

    assert await _count_staged(conn, locked_user) == 1
    assert await _count_staged(conn, unlocked_user) == 0
    drain._producer.produce.assert_called_once()


# ---------------------------------------------------------------------------
# 4. __aexit__ cleanup isolation (test gate 2)
# ---------------------------------------------------------------------------


async def test_staging_drain_aexit_runs_all_cleanup_steps_even_on_failure(
    conn, staging_repo
):
    """Per-step try/except in __aexit__ ensures all steps run even when step 5 raises.

    Specifically: listener task cancelled, sweep task cancelled, _listener_conn
    left None/closed, and producer.flush() was attempted even though it raised.
    """
    failing_producer = MagicMock()
    failing_producer.flush.side_effect = RuntimeError("producer is gone")

    with patch(
        "grosh_normalizer.services.staging_drain_service.Producer",
        return_value=failing_producer,
    ):
        instance = StagingDrainService(SingleConnectionPool(conn), staging_repo)
        async with instance:
            # Let the tasks start — just a yield point is enough.
            await asyncio.sleep(0)

    # Both background tasks must be done (cancelled counts as done).
    assert instance._listener_task is not None
    assert instance._listener_task.done()
    assert instance._sweep_task is not None
    assert instance._sweep_task.done()

    # Listener connection cleaned up (either None because _connect_and_listen
    # finalised it, or already None because the task never reached that point).
    assert instance._listener_conn is None

    # Producer flush was attempted despite the listener/sweep already being
    # cancelled — proves step 5 ran even though it raised.
    failing_producer.flush.assert_called_once()


# ---------------------------------------------------------------------------
# 5. NOTIFY drain task tracking (test gate 3)
# ---------------------------------------------------------------------------


async def test_staging_drain_notify_tasks_tracked_and_awaited(conn, staging_repo):
    """_on_notify tasks are tracked and awaited within __aexit__'s 2s budget.

    Steps:
      1. Insert a staged row for a user.
      2. Enter async with — tasks start.
      3. Fire _on_notify directly (simulates the asyncpg callback).
      4. Exit async with immediately — __aexit__ step 4 should wait for the
         in-flight drain task and the staged row should be published before exit.
    """
    user_id = await insert_user(conn)
    await _stage_event(conn, staging_repo, user_id)

    mock_producer = MagicMock()

    with patch(
        "grosh_normalizer.services.staging_drain_service.Producer",
        return_value=mock_producer,
    ):
        instance = StagingDrainService(SingleConnectionPool(conn), staging_repo)
        async with instance:
            # Simulate the asyncpg NOTIFY callback — synchronous call, schedules a task.
            instance._on_notify(
                connection=None,  # type: ignore[arg-type]
                pid=0,
                channel="reprocess_complete",
                payload=str(user_id),
            )
            # Yield control so the drain task can be created and added to the set.
            await asyncio.sleep(0)
            # __aexit__ runs here — must wait up to 2s for the drain task to finish.

    # The staged row must have been published during the shutdown window.
    mock_producer.produce.assert_called_once()
    call_kwargs = mock_producer.produce.call_args.kwargs
    assert str(user_id).encode() == call_kwargs["key"]

    # The staging table must be empty — drain completed before __aexit__ returned.
    assert await _count_staged(conn, user_id) == 0
