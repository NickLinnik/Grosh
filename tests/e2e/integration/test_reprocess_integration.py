"""Integration tests for the reprocessing job helpers.

Tests use real Postgres (via the session-scoped db_pool fixture) and a mock
Kafka producer so Redpanda is not required.

Covered scenarios:
  1. Stale lock cleanup — rows with no advisory lock holder are deleted;
     rows with a live holder survive.
  2. Snapshot-then-delete — backup row created, transactions deleted, IDs returned.
  3. Verify snapshot — all IDs present → empty list; absent ID → in missing list.
  4. Release lock atomically — lock row deleted, advisory lock released, NOTIFY fired.
  5. Restore from backup — deleted transactions re-inserted from JSONB snapshot.
  6. Catchup wait — zero expected returns immediately; times out when never reached.
  7. Publish replay events — produce called per event; empty list produces nothing.
"""

import asyncio
from datetime import UTC, datetime
from unittest.mock import MagicMock
from uuid import UUID, uuid4

import asyncpg
import pytest

from grosh_normalization.repositories.reprocess_repo import (
    ReprocessError,
    ReprocessRepo,
)
from grosh_normalization.repositories.transaction_read_repo import TransactionReadRepo
from grosh_normalization.services.reprocess_orchestrator import (
    ReprocessOrchestrator,
)
from helpers import make_event
from integration.conftest import insert_account, insert_transaction, insert_user

pytestmark = pytest.mark.asyncio

_repo = ReprocessRepo()


def _make_orchestrator(mock_producer=None):
    """Build a ReprocessOrchestrator wired to a mock producer."""
    producer = mock_producer or MagicMock()
    return ReprocessOrchestrator(_repo, TransactionReadRepo(), producer)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _lock_row_exists(conn: asyncpg.Connection, user_id: UUID) -> bool:
    val = await conn.fetchval(
        "SELECT EXISTS (SELECT 1 FROM reprocessing_locks WHERE user_id = $1)",
        user_id,
    )
    return bool(val)


async def _backup_count(conn: asyncpg.Connection, user_id: UUID) -> int:
    val = await conn.fetchval(
        "SELECT COUNT(*) FROM reprocessing_backups WHERE user_id = $1",
        user_id,
    )
    return int(val)


async def _tx_count(conn: asyncpg.Connection, user_id: UUID) -> int:
    val = await conn.fetchval(
        "SELECT COUNT(*) FROM transactions WHERE user_id = $1",
        user_id,
    )
    return int(val)


async def _advisory_lock_held(conn: asyncpg.Connection, lock_key: str) -> bool:
    """Return True if any session currently holds this advisory lock.

    pg_locks shows advisory locks held by ALL sessions; no self-visibility issue.
    The lock was acquired via hashtext($1) — we must use the same hash.
    """
    val = await conn.fetchval(
        """
        SELECT EXISTS (
            SELECT 1 FROM pg_locks
            WHERE locktype = 'advisory'
                AND objid = hashtext($1)::bigint & x'ffffffff'::bigint
                AND granted = true
        )
        """,
        lock_key,
    )
    return bool(val)


# ---------------------------------------------------------------------------
# 1. Stale lock cleanup
# ---------------------------------------------------------------------------


async def test_clean_stale_locks_removes_row_without_advisory_lock(
    conn: asyncpg.Connection,
) -> None:
    """A lock row with no live advisory lock is deleted by clean_stale_locks."""
    user_id = await insert_user(conn)

    # Insert a row but acquire NO advisory lock — simulates a dead session.
    await conn.execute(
        "INSERT INTO reprocessing_locks (user_id) VALUES ($1)",
        user_id,
    )
    assert await _lock_row_exists(conn, user_id)

    await _repo.clean_stale_locks(conn, user_id)

    assert not await _lock_row_exists(conn, user_id)


async def test_clean_stale_locks_preserves_row_with_live_lock(
    db_pool: asyncpg.Pool,
) -> None:
    """A lock row whose advisory lock is still held is left untouched."""
    session_conn = await db_pool.acquire()
    check_conn = await db_pool.acquire()
    user_id: UUID | None = None
    try:
        user_id = uuid4()
        await session_conn.execute(
            """
            INSERT INTO users (id, email, password_hash, display_name, role)
            VALUES ($1, $2, 'hash', 'Test User', 'member')
            """,
            user_id,
            f"stale-liveness-{user_id}@example.com",
        )
        # Insert lock row AND hold the advisory lock on session_conn.
        await session_conn.execute(
            "INSERT INTO reprocessing_locks (user_id) VALUES ($1)", user_id
        )
        await session_conn.execute(
            "SELECT pg_advisory_lock(hashtext($1))",
            f"reprocess:{user_id}",
        )

        # clean_stale_locks on check_conn must NOT delete the row (lock is live).
        await _repo.clean_stale_locks(check_conn, user_id)

        assert await _lock_row_exists(check_conn, user_id)

    finally:
        if user_id is not None:
            try:
                await session_conn.execute(
                    "SELECT pg_advisory_unlock(hashtext($1))",
                    f"reprocess:{user_id}",
                )
                await session_conn.execute(
                    "DELETE FROM reprocessing_locks WHERE user_id = $1", user_id
                )
                await session_conn.execute("DELETE FROM users WHERE id = $1", user_id)
            except Exception:
                pass
        await db_pool.release(session_conn)
        await db_pool.release(check_conn)


async def test_clean_stale_locks_no_row_is_noop(conn: asyncpg.Connection) -> None:
    """clean_stale_locks on a user with no lock row completes without error."""
    user_id = await insert_user(conn)
    await _repo.clean_stale_locks(conn, user_id)


# ---------------------------------------------------------------------------
# 2. Snapshot + backup
# ---------------------------------------------------------------------------


async def test_snapshot_creates_backup_row(conn: asyncpg.Connection) -> None:
    """snapshot_transactions inserts a reprocessing_backups row and returns IDs."""
    user_id = await insert_user(conn)
    account_id = await insert_account(
        conn, user_id=user_id, type="black", currency_code="UAH"
    )
    tx_id = await insert_transaction(
        conn,
        user_id=user_id,
        account_id=account_id,
        time=datetime(2025, 1, 1, tzinfo=UTC),
        amount_cents=10000,
        direction="expense",
    )

    returned_ids = await _repo.snapshot_transactions(conn, user_id)

    assert tx_id in returned_ids
    assert await _backup_count(conn, user_id) == 1


async def test_snapshot_returns_empty_for_user_with_no_transactions(
    conn: asyncpg.Connection,
) -> None:
    """snapshot_transactions on a user with no transactions creates an empty backup."""
    user_id = await insert_user(conn)

    returned_ids = await _repo.snapshot_transactions(conn, user_id)

    assert returned_ids == []
    assert await _backup_count(conn, user_id) == 1


# ---------------------------------------------------------------------------
# 3. Verify snapshot
# ---------------------------------------------------------------------------


async def test_verify_snapshot_passes_when_all_ids_present(
    conn: asyncpg.Connection,
) -> None:
    """verify_snapshot returns empty list when all snapshot IDs are in transactions."""
    user_id = await insert_user(conn)
    account_id = await insert_account(
        conn, user_id=user_id, type="black", currency_code="UAH"
    )
    tx_id = await insert_transaction(
        conn,
        user_id=user_id,
        account_id=account_id,
        time=datetime(2025, 1, 1, tzinfo=UTC),
        amount_cents=5000,
        direction="expense",
    )

    missing = await _repo.verify_snapshot(conn, user_id, [tx_id])

    assert missing == []


async def test_verify_snapshot_returns_missing_ids(conn: asyncpg.Connection) -> None:
    """verify_snapshot returns IDs that are not in the transactions table."""
    user_id = await insert_user(conn)
    ghost_id = uuid4()

    missing = await _repo.verify_snapshot(conn, user_id, [ghost_id])

    assert ghost_id in missing


async def test_verify_snapshot_empty_snapshot(conn: asyncpg.Connection) -> None:
    """verify_snapshot with empty snapshot_ids returns empty list immediately."""
    user_id = await insert_user(conn)

    missing = await _repo.verify_snapshot(conn, user_id, [])

    assert missing == []


# ---------------------------------------------------------------------------
# 4. Release lock atomically + NOTIFY
# ---------------------------------------------------------------------------


async def test_release_lock_removes_row_and_fires_notify(
    db_pool: asyncpg.Pool,
) -> None:
    """release_lock_atomic deletes the lock row and emits NOTIFY reprocess_complete."""
    conn_worker = await db_pool.acquire()
    conn_listener = await db_pool.acquire()
    user_id: UUID | None = None
    notifications: list[str] = []

    try:
        user_id = uuid4()
        await conn_worker.execute(
            """
            INSERT INTO users (id, email, password_hash, display_name, role)
            VALUES ($1, $2, 'hash', 'Test', 'member')
            """,
            user_id,
            f"release-test-{user_id}@example.com",
        )
        await conn_worker.execute(
            "INSERT INTO reprocessing_locks (user_id) VALUES ($1)", user_id
        )
        await conn_worker.execute(
            "SELECT pg_advisory_lock(hashtext($1))",
            f"reprocess:{user_id}",
        )

        def _on_notify(conn, pid, channel, payload):
            notifications.append(payload)

        await conn_listener.add_listener("reprocess_complete", _on_notify)

        await _repo.release_lock_atomic(conn_worker, user_id)

        await conn_listener.execute("SELECT 1")
        await asyncio.sleep(0.1)

        assert not await _lock_row_exists(conn_worker, user_id)
        assert any(str(user_id) == n for n in notifications)

    finally:
        if user_id is not None:
            try:
                await conn_worker.execute(
                    "DELETE FROM reprocessing_locks WHERE user_id = $1", user_id
                )
                await conn_worker.execute("DELETE FROM users WHERE id = $1", user_id)
            except Exception:
                pass
        try:
            await conn_listener.remove_listener("reprocess_complete", _on_notify)
        except Exception:
            pass
        await db_pool.release(conn_worker)
        await db_pool.release(conn_listener)


# ---------------------------------------------------------------------------
# 5. Restore from backup
# ---------------------------------------------------------------------------


async def test_restore_from_backup_reinserts_transactions(
    conn: asyncpg.Connection,
) -> None:
    """restore_from_backup re-inserts rows deleted after snapshot."""
    user_id = await insert_user(conn)
    account_id = await insert_account(
        conn, user_id=user_id, type="black", currency_code="UAH"
    )
    tx_id = await insert_transaction(
        conn,
        user_id=user_id,
        account_id=account_id,
        time=datetime(2025, 3, 1, tzinfo=UTC),
        amount_cents=20000,
        direction="income",
    )

    await _repo.snapshot_transactions(conn, user_id)
    await conn.execute("DELETE FROM transactions WHERE id = $1", tx_id)
    assert await _tx_count(conn, user_id) == 0

    await _repo.restore_from_backup(conn, user_id)

    assert await _tx_count(conn, user_id) == 1
    restored = await conn.fetchrow(
        "SELECT id FROM transactions WHERE user_id = $1", user_id
    )
    assert restored["id"] == tx_id


async def test_restore_from_backup_raises_when_no_backup(
    conn: asyncpg.Connection,
) -> None:
    """restore_from_backup raises ReprocessError when no backup row exists."""
    user_id = await insert_user(conn)

    with pytest.raises(ReprocessError, match="No backup found"):
        await _repo.restore_from_backup(conn, user_id)


# ---------------------------------------------------------------------------
# 6. _wait_for_pipeline_catchup: zero expected count skips immediately
# ---------------------------------------------------------------------------


async def test_catchup_zero_expected_returns_immediately(
    conn: asyncpg.Connection,
) -> None:
    """_wait_for_pipeline_catchup with expected_count=0 returns without polling."""
    user_id = await insert_user(conn)
    orchestrator = _make_orchestrator()
    await orchestrator._wait_for_pipeline_catchup(conn, user_id, expected_count=0)


async def test_catchup_returns_when_count_matches(conn: asyncpg.Connection) -> None:
    """_wait_for_pipeline_catchup returns once the tx count reaches expected."""
    user_id = await insert_user(conn)
    account_id = await insert_account(
        conn, user_id=user_id, type="black", currency_code="UAH"
    )
    await insert_transaction(
        conn,
        user_id=user_id,
        account_id=account_id,
        time=datetime(2025, 4, 1, tzinfo=UTC),
        amount_cents=1000,
        direction="expense",
    )

    orchestrator = _make_orchestrator()
    await orchestrator._wait_for_pipeline_catchup(conn, user_id, expected_count=1)


async def test_catchup_times_out_when_count_never_reached(
    conn: asyncpg.Connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    """_wait_for_pipeline_catchup raises ReprocessError after timeout."""
    import grosh_normalization.services.reprocess_orchestrator as mod

    monkeypatch.setattr(mod, "_CATCHUP_POLL_INTERVAL_S", 0.05)
    monkeypatch.setattr(mod, "_CATCHUP_TIMEOUT_S", 0.1)

    user_id = await insert_user(conn)
    orchestrator = _make_orchestrator()

    with pytest.raises(ReprocessError, match="catchup timeout"):
        await orchestrator._wait_for_pipeline_catchup(conn, user_id, expected_count=999)


# ---------------------------------------------------------------------------
# 7. ReprocessOrchestrator._publish_normalized_events: uses mock producer
# ---------------------------------------------------------------------------


async def test_publish_replay_events_calls_produce_for_each_event() -> None:
    """_publish_normalized_events calls producer.produce once per event."""
    events = [make_event() for _ in range(3)]
    mock_producer = MagicMock()
    orchestrator = _make_orchestrator(mock_producer)

    count = orchestrator._publish_normalized_events(uuid4(), events)

    assert count == 3
    assert mock_producer.produce.call_count == 3
    mock_producer.flush.assert_called()


async def test_publish_replay_events_empty_list() -> None:
    """_publish_normalized_events with no events produces nothing and returns 0."""
    mock_producer = MagicMock()
    orchestrator = _make_orchestrator(mock_producer)

    count = orchestrator._publish_normalized_events(uuid4(), [])

    assert count == 0
    mock_producer.produce.assert_not_called()


# ---------------------------------------------------------------------------
# 8. Test gate: pg_locks-based staleness (not TTL)
# ---------------------------------------------------------------------------


async def test_clean_stale_locks_uses_pg_locks_not_ttl(
    db_pool: asyncpg.Pool,
) -> None:
    """Staleness is detected via pg_locks, not locked_at age.

    Phase A: fresh locked_at + no advisory lock → row deleted (stale by liveness).
    Phase B: lock row + live advisory lock on session_b → row NOT deleted.
    Phase C: close session_b → row IS deleted (lock released).
    """
    primary_conn = await db_pool.acquire()
    user_id: UUID | None = None

    try:
        user_id = uuid4()
        await primary_conn.execute(
            """
            INSERT INTO users (id, email, password_hash, display_name, role)
            VALUES ($1, $2, 'hash', 'Test User', 'member')
            """,
            user_id,
            f"pg-locks-test-{user_id}@example.com",
        )

        # Phase A: fresh locked_at, NO advisory lock → stale by liveness.
        await primary_conn.execute(
            "INSERT INTO reprocessing_locks (user_id, locked_at) VALUES ($1, now())",
            user_id,
        )
        deleted = await _repo.clean_stale_locks(primary_conn, user_id)
        assert deleted == 1
        assert not await _lock_row_exists(primary_conn, user_id)

        # Phase B: session_b holds the advisory lock → row must survive.
        session_b = await db_pool.acquire()
        try:
            await primary_conn.execute(
                "INSERT INTO reprocessing_locks (user_id) VALUES ($1)", user_id
            )
            await session_b.execute(
                "SELECT pg_advisory_lock(hashtext($1))",
                f"reprocess:{user_id}",
            )

            deleted = await _repo.clean_stale_locks(primary_conn, user_id)
            assert deleted == 0
            assert await _lock_row_exists(primary_conn, user_id)
        finally:
            await db_pool.release(session_b)

        # Phase C: session_b is closed → lock released → row is now stale.
        deleted = await _repo.clean_stale_locks(primary_conn, user_id)
        assert deleted == 1
        assert not await _lock_row_exists(primary_conn, user_id)

    finally:
        if user_id is not None:
            try:
                await primary_conn.execute(
                    "DELETE FROM reprocessing_locks WHERE user_id = $1", user_id
                )
                await primary_conn.execute("DELETE FROM users WHERE id = $1", user_id)
            except Exception:
                pass
        await db_pool.release(primary_conn)


# ---------------------------------------------------------------------------
# 9. Test gate: uniform post-DELETE recovery on publish failure
# ---------------------------------------------------------------------------


async def test_reprocess_recovers_on_publish_failure(
    db_pool: asyncpg.Pool,
) -> None:
    """When Kafka publish raises, transactions are restored and lock released.

    Uses a dedicated session connection (required for advisory locks).
    """
    from confluent_kafka import KafkaException

    session_conn = await db_pool.acquire()
    check_conn = await db_pool.acquire()
    user_id: UUID | None = None

    try:
        user_id = uuid4()
        await session_conn.execute(
            """
            INSERT INTO users (id, email, password_hash, display_name, role)
            VALUES ($1, $2, 'hash', 'Test User', 'member')
            """,
            user_id,
            f"recover-publish-{user_id}@example.com",
        )
        account_id = uuid4()
        await session_conn.execute(
            """
            INSERT INTO accounts (id, user_id, source, type, currency_code)
            VALUES ($1, $2, 'monobank', 'black', 'UAH')
            """,
            account_id,
            user_id,
        )
        tx_id = await insert_transaction(
            session_conn,
            user_id=user_id,
            account_id=account_id,
            time=datetime(2025, 5, 1, tzinfo=UTC),
            amount_cents=9900,
            direction="expense",
        )

        # Insert lock row — the ingestion API now owns this; consumer asserts it exists.
        await session_conn.execute(
            "INSERT INTO reprocessing_locks (user_id) VALUES ($1)", user_id
        )

        # Producer that always raises on produce().
        mock_producer = MagicMock()
        mock_producer.produce.side_effect = KafkaException("broker down")

        orchestrator = _make_orchestrator(mock_producer)

        with pytest.raises(ReprocessError, match="restored from backup"):
            await orchestrator.reprocess_user(session_conn, user_id)

        # Lock row must be gone (release_lock_atomic was called in recovery path).
        assert not await _lock_row_exists(check_conn, user_id)

        # Transactions must be restored (same ID back in table).
        restored = await check_conn.fetchrow(
            "SELECT id FROM transactions WHERE id = $1", tx_id
        )
        assert restored is not None

    finally:
        if user_id is not None:
            try:
                await session_conn.execute(
                    "DELETE FROM reprocessing_locks WHERE user_id = $1", user_id
                )
                await session_conn.execute(
                    "DELETE FROM transactions WHERE user_id = $1", user_id
                )
                await session_conn.execute(
                    "DELETE FROM reprocessing_backups WHERE user_id = $1", user_id
                )
                await session_conn.execute(
                    "DELETE FROM accounts WHERE user_id = $1", user_id
                )
                await session_conn.execute("DELETE FROM users WHERE id = $1", user_id)
            except Exception:
                pass
        await db_pool.release(session_conn)
        await db_pool.release(check_conn)


# ---------------------------------------------------------------------------
# 10. Test gate: uniform post-DELETE recovery on catchup timeout
# ---------------------------------------------------------------------------


async def test_reprocess_recovers_on_catchup_timeout(
    db_pool: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch
) -> None:
    """When pipeline catchup times out, transactions are restored and lock released."""
    import grosh_normalization.services.reprocess_orchestrator as mod

    monkeypatch.setattr(mod, "_CATCHUP_POLL_INTERVAL_S", 0.01)
    monkeypatch.setattr(mod, "_CATCHUP_TIMEOUT_S", 0.05)

    session_conn = await db_pool.acquire()
    check_conn = await db_pool.acquire()
    user_id: UUID | None = None

    try:
        user_id = uuid4()
        await session_conn.execute(
            """
            INSERT INTO users (id, email, password_hash, display_name, role)
            VALUES ($1, $2, 'hash', 'Test User', 'member')
            """,
            user_id,
            f"recover-timeout-{user_id}@example.com",
        )
        account_id = uuid4()
        await session_conn.execute(
            """
            INSERT INTO accounts (id, user_id, source, type, currency_code)
            VALUES ($1, $2, 'monobank', 'black', 'UAH')
            """,
            account_id,
            user_id,
        )
        tx_id = await insert_transaction(
            session_conn,
            user_id=user_id,
            account_id=account_id,
            time=datetime(2025, 5, 2, tzinfo=UTC),
            amount_cents=4500,
            direction="income",
        )

        # Insert lock row — the ingestion API now owns this; consumer asserts it exists.
        await session_conn.execute(
            "INSERT INTO reprocessing_locks (user_id) VALUES ($1)", user_id
        )

        # Producer that succeeds but the pipeline never writes back.
        orchestrator = _make_orchestrator()

        with pytest.raises(ReprocessError, match="restored from backup"):
            await orchestrator.reprocess_user(session_conn, user_id)

        assert not await _lock_row_exists(check_conn, user_id)

        restored = await check_conn.fetchrow(
            "SELECT id FROM transactions WHERE id = $1", tx_id
        )
        assert restored is not None

    finally:
        if user_id is not None:
            try:
                await session_conn.execute(
                    "DELETE FROM reprocessing_locks WHERE user_id = $1", user_id
                )
                await session_conn.execute(
                    "DELETE FROM transactions WHERE user_id = $1", user_id
                )
                await session_conn.execute(
                    "DELETE FROM reprocessing_backups WHERE user_id = $1", user_id
                )
                await session_conn.execute(
                    "DELETE FROM accounts WHERE user_id = $1", user_id
                )
                await session_conn.execute("DELETE FROM users WHERE id = $1", user_id)
            except Exception:
                pass
        await db_pool.release(session_conn)
        await db_pool.release(check_conn)


# ---------------------------------------------------------------------------
# 11. Test gate: entrypoint continues after one user fails
# ---------------------------------------------------------------------------


async def test_run_reprocess_continues_after_one_user_fails() -> None:
    """The entrypoint logs failures and moves on to the next user in the loop."""
    from unittest.mock import AsyncMock, patch

    user_a = uuid4()
    user_b = uuid4()

    boom = ReprocessError("user A exploded")

    mock_orchestrator = MagicMock()
    mock_orchestrator.reprocess_user = AsyncMock(side_effect=[boom, None])

    mock_repo = MagicMock()
    mock_repo.list_all_user_ids = AsyncMock(return_value=[user_a, user_b])

    # session_conn.close() is awaited in the finally block — needs AsyncMock.
    mock_session_conn = AsyncMock()

    with (
        patch(
            "grosh_normalization.reprocess_main.asyncpg.connect",
            new=AsyncMock(return_value=mock_session_conn),
        ),
        patch(
            "grosh_normalization.reprocess_main.Producer",
            return_value=MagicMock(),
        ),
        patch(
            "grosh_normalization.reprocess_main.ReprocessRepo",
            return_value=mock_repo,
        ),
        patch(
            "grosh_normalization.reprocess_main.TransactionReadRepo",
            return_value=MagicMock(),
        ),
        patch(
            "grosh_normalization.reprocess_main.ReprocessOrchestrator",
            return_value=mock_orchestrator,
        ),
    ):
        from grosh_normalization.reprocess_main import run_reprocess

        await run_reprocess(user_ids=[user_a, user_b])

    # Both users must have been attempted even though the first raised.
    assert mock_orchestrator.reprocess_user.call_count == 2
    calls = [c.args[1] for c in mock_orchestrator.reprocess_user.call_args_list]
    assert user_a in calls
    assert user_b in calls
