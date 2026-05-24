"""Unit tests for ReprocessOrchestrator.

Uses in-memory fakes for ReprocessRepo and TransactionReadRepo so no DB or
Kafka is required. Tests cover decision points the e2e CANNOT easily isolate:

  1. lock_exists returns False → reprocess_user returns False without touching
     any transactions (early-exit path).
  2. Verification failure (missing IDs after publish) → ReprocessError raised
     and restore + release_lock_atomic called.
  3. KafkaException on first produce → flush + retry (BufferError retry branch
     in _publish_normalized_events).
  4. _publish_normalized_events returns correct count for N events.
  5. _publish_normalized_events with empty events → no produce calls, returns 0.
  6. _wait_for_pipeline_catchup with expected_count=0 → returns immediately.
  7. _wait_for_pipeline_catchup returns when count reaches expected.
  8. _wait_for_pipeline_catchup raises ReprocessError on timeout.
  9. Publish raises KafkaException even after retry → ReprocessError raised,
     restore path executed.
"""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest
from confluent_kafka import KafkaException
from grosh_shared.domain.normalized import NormalizedTransaction, TransactionRow

import grosh_normalization.services.reprocess_orchestrator as mod
from grosh_normalization.repositories.reprocess_repo import ReprocessError
from grosh_normalization.services.reprocess_orchestrator import ReprocessOrchestrator
from tests.helpers import make_event

# ---------------------------------------------------------------------------
# In-memory fakes
# ---------------------------------------------------------------------------


class InMemoryReprocessRepo:
    """Fake ReprocessRepo that models real query semantics in memory.

    Tracks which methods were called so tests can assert orchestration order.
    All methods accept `conn` but ignore it — same pattern as other services
    in this project.
    """

    def __init__(
        self,
        *,
        lock_present: bool = True,
        snapshot_ids: list[UUID] | None = None,
        count_for_user_value: int = 0,
        verify_missing: list[UUID] | None = None,
    ) -> None:
        self.lock_present = lock_present
        self._snapshot_ids: list[UUID] = snapshot_ids or []
        self._count_for_user_value = count_for_user_value
        self._verify_missing: list[UUID] = verify_missing or []

        # Call trackers
        self.advisory_lock_acquired = False
        self.snapshot_called = False
        self.delete_called = False
        self.restore_called = False
        self.release_called = False
        self.verify_called = False

    async def lock_exists(self, conn: object, user_id: UUID) -> bool:
        return self.lock_present

    async def acquire_advisory_lock(self, conn: object, user_id: UUID) -> None:
        self.advisory_lock_acquired = True

    async def snapshot_transactions(self, conn: object, user_id: UUID) -> list[UUID]:
        self.snapshot_called = True
        return list(self._snapshot_ids)

    async def delete_user_transactions(self, conn: object, user_id: UUID) -> None:
        self.delete_called = True

    async def count_for_user(self, conn: object, user_id: UUID) -> int:
        return self._count_for_user_value

    async def verify_snapshot(
        self, conn: object, user_id: UUID, snapshot_ids: list[UUID]
    ) -> list[UUID]:
        self.verify_called = True
        return list(self._verify_missing)

    async def restore_from_backup(self, conn: object, user_id: UUID) -> None:
        self.restore_called = True

    async def release_lock_atomic(self, conn: object, user_id: UUID) -> None:
        self.release_called = True


class InMemoryTransactionReadRepo:
    """Fake TransactionReadRepo returning pre-configured rows."""

    def __init__(self, rows: list[NormalizedTransaction] | None = None) -> None:
        self._rows = rows or []

    async def select_for_user(
        self, conn: object, user_id: UUID
    ) -> list[TransactionRow]:
        # Mirror real SQL `WHERE user_id = $1` so unit tests can catch missing-filter
        # regressions. Real repo at services/normalization/.../transaction_read_repo.py.
        result = []
        for tx in self._rows:
            if tx.user_id != user_id:
                continue
            result.append(
                TransactionRow(
                    id=tx.id,
                    source=tx.source,
                    source_id=tx.source_id,
                    user_id=tx.user_id,
                    account_id=tx.account_id,
                    time=tx.time,
                    amount_cents=tx.amount_cents,
                    operation_amount_cents=tx.operation_amount_cents,
                    operation_currency_code=tx.operation_currency_code,
                    description=tx.description,
                    mcc=tx.mcc,
                    cashback_amount_cents=tx.cashback_amount_cents,
                    balance_cents=tx.balance_cents,
                    hold=tx.hold,
                    direction=tx.direction,
                    counterparty_iban=tx.counterparty_iban,
                    rate_source=tx.rate_source,
                    metadata=tx.metadata or {"source": {}},
                )
            )
        return result


class FakeProducer:
    """Minimal Kafka producer fake — records calls, supports configurable failure."""

    def __init__(self, *, fail_on_first_produce: bool = False) -> None:
        self._fail_on_first_produce = fail_on_first_produce
        self._produce_call_count = 0
        self.produce_calls: list[dict] = []
        self.flush_calls: list[float | None] = []
        self.poll_calls: list[int] = []

    def produce(
        self, *, topic: object, key: bytes, value: bytes, on_delivery=None
    ) -> None:
        self._produce_call_count += 1
        if self._fail_on_first_produce and self._produce_call_count == 1:
            raise KafkaException("broker down")
        self.produce_calls.append({"topic": topic, "key": key, "value": value})

    def flush(self, timeout: float | None = None) -> None:
        self.flush_calls.append(timeout)

    def poll(self, timeout: int) -> None:
        self.poll_calls.append(timeout)


def _make_orchestrator(
    repo: InMemoryReprocessRepo | None = None,
    read_repo: InMemoryTransactionReadRepo | None = None,
    producer: FakeProducer | None = None,
) -> ReprocessOrchestrator:
    return ReprocessOrchestrator(
        repo=repo or InMemoryReprocessRepo(),
        transaction_read_repo=read_repo or InMemoryTransactionReadRepo(),
        producer=producer or FakeProducer(),
    )


# ---------------------------------------------------------------------------
# 1. Early exit when lock is absent
# ---------------------------------------------------------------------------


async def test_reprocess_user_skips_when_lock_absent() -> None:
    """reprocess_user returns False and touches nothing when lock row absent.

    Bug class: orchestrator proceeds with transaction delete even though
    the job was started spuriously or the lock was already released — causes
    silent data deletion with no recovery path.
    """
    repo = InMemoryReprocessRepo(lock_present=False)
    orchestrator = _make_orchestrator(repo=repo)

    result = await orchestrator.reprocess_user(conn=None, user_id=uuid4())

    assert result is False
    assert not repo.snapshot_called
    assert not repo.delete_called
    assert not repo.restore_called
    assert not repo.release_called


# ---------------------------------------------------------------------------
# 2. Verification failure → restore + release
# ---------------------------------------------------------------------------


async def test_reprocess_user_restores_on_verification_failure() -> None:
    """When verify_snapshot returns missing IDs, restore + release are called.

    Bug class: verification failure silently passes → the user's re-enriched
    transactions are missing IDs that were in the original snapshot; data loss
    not detected.
    """
    user_id = uuid4()
    missing_id = uuid4()
    repo = InMemoryReprocessRepo(
        lock_present=True,
        snapshot_ids=[missing_id],
        count_for_user_value=1,
        verify_missing=[missing_id],  # simulate missing after replay
    )
    read_repo = InMemoryTransactionReadRepo(rows=[make_event(user_id=user_id)])
    producer = FakeProducer()
    orchestrator = _make_orchestrator(repo=repo, read_repo=read_repo, producer=producer)

    with pytest.raises(ReprocessError, match="restored from backup"):
        await orchestrator.reprocess_user(conn=None, user_id=user_id)

    assert repo.restore_called
    assert repo.release_called


# ---------------------------------------------------------------------------
# 3. KafkaException on first produce → flush + retry
# ---------------------------------------------------------------------------


async def test_publish_retries_on_kafka_exception() -> None:
    """_publish_normalized_events flushes and retries when first produce raises.

    Bug class: KafkaException on first produce aborts all remaining events
    without retry — partial publish causes catchup to wait forever (timeout)
    and triggers unnecessary restore.
    """
    producer = FakeProducer(fail_on_first_produce=True)
    orchestrator = _make_orchestrator(producer=producer)
    events = [make_event()]

    count = orchestrator._publish_normalized_events(uuid4(), events)

    # Retry path: first produce raises, flush is called, second produce succeeds.
    assert count == 1
    assert len(producer.flush_calls) >= 1
    # Two produce calls: one that raised, one retry.
    assert producer._produce_call_count == 2


# ---------------------------------------------------------------------------
# 4. _publish_normalized_events: correct count for N events
# ---------------------------------------------------------------------------


async def test_publish_returns_correct_count_for_multiple_events() -> None:
    """_publish_normalized_events returns len(events), not a static value.

    Bug class: hardcoded return value causes catchup to wait for the wrong
    number of transactions — either exits early (missed rows) or hangs forever.
    """
    producer = FakeProducer()
    orchestrator = _make_orchestrator(producer=producer)
    events = [make_event() for _ in range(5)]

    count = orchestrator._publish_normalized_events(uuid4(), events)

    assert count == 5
    assert len(producer.produce_calls) == 5
    assert producer.poll_calls.count(0) == 5


# ---------------------------------------------------------------------------
# 5. _publish_normalized_events: empty events → zero calls
# ---------------------------------------------------------------------------


async def test_publish_empty_events_returns_zero_and_no_produce_calls() -> None:
    """_publish_normalized_events with empty list produces nothing.

    Bug class: produce called with empty value or spurious flush for a user
    with no transactions — harmless but confusing; flush timeout wasted.
    """
    producer = FakeProducer()
    orchestrator = _make_orchestrator(producer=producer)

    count = orchestrator._publish_normalized_events(uuid4(), [])

    assert count == 0
    assert len(producer.produce_calls) == 0
    # flush(timeout=30) is still called — this is intentional per implementation.
    assert len(producer.flush_calls) >= 1


# ---------------------------------------------------------------------------
# 6. _wait_for_pipeline_catchup: expected_count=0 returns immediately
# ---------------------------------------------------------------------------


async def test_catchup_zero_expected_returns_immediately() -> None:
    """_wait_for_pipeline_catchup with expected_count=0 returns without polling.

    Bug class: asyncio.sleep called unnecessarily when there are no events to
    wait for → Job runtime inflated; poll loop never exits if count_for_user
    also returns 0 and condition is `>= expected` (true immediately but sleep
    called first).
    """
    repo = InMemoryReprocessRepo(count_for_user_value=0)
    orchestrator = _make_orchestrator(repo=repo)

    # Must return without sleeping (early return is synchronous, no timeout needed).
    await orchestrator._wait_for_pipeline_catchup(
        conn=None, user_id=uuid4(), expected_count=0
    )


# ---------------------------------------------------------------------------
# 7. _wait_for_pipeline_catchup: returns when count matches
# ---------------------------------------------------------------------------


async def test_catchup_returns_when_count_reaches_expected() -> None:
    """_wait_for_pipeline_catchup returns once count_for_user >= expected_count.

    Bug class: comparison uses strict > instead of >= → catchup never returns
    when expected == actual, causing every reprocess to time out.
    """
    repo = InMemoryReprocessRepo(count_for_user_value=3)
    orchestrator = _make_orchestrator(repo=repo)

    original_interval = mod._CATCHUP_POLL_INTERVAL_S
    original_timeout = mod._CATCHUP_TIMEOUT_S
    mod._CATCHUP_POLL_INTERVAL_S = 0.01
    mod._CATCHUP_TIMEOUT_S = 5.0
    try:
        await orchestrator._wait_for_pipeline_catchup(
            conn=None, user_id=uuid4(), expected_count=3
        )
    finally:
        mod._CATCHUP_POLL_INTERVAL_S = original_interval
        mod._CATCHUP_TIMEOUT_S = original_timeout


# ---------------------------------------------------------------------------
# 8. _wait_for_pipeline_catchup: raises on timeout
# ---------------------------------------------------------------------------


async def test_catchup_raises_reprocess_error_on_timeout() -> None:
    """_wait_for_pipeline_catchup raises ReprocessError when count never reaches target.

    Bug class: timeout silently passes without raising → caller's except block
    never runs, lock is never released, next job start finds a stale lock.
    """
    repo = InMemoryReprocessRepo(count_for_user_value=0)
    orchestrator = _make_orchestrator(repo=repo)

    original_interval = mod._CATCHUP_POLL_INTERVAL_S
    original_timeout = mod._CATCHUP_TIMEOUT_S
    mod._CATCHUP_POLL_INTERVAL_S = 0.01
    mod._CATCHUP_TIMEOUT_S = 0.05
    try:
        with pytest.raises(ReprocessError, match="catchup timeout"):
            await orchestrator._wait_for_pipeline_catchup(
                conn=None, user_id=uuid4(), expected_count=999
            )
    finally:
        mod._CATCHUP_POLL_INTERVAL_S = original_interval
        mod._CATCHUP_TIMEOUT_S = original_timeout


# ---------------------------------------------------------------------------
# 9. KafkaException on retry also → ReprocessError, restore executed
# ---------------------------------------------------------------------------


async def test_reprocess_user_restores_on_kafka_exception_after_retry() -> None:
    """When produce raises on both first and retry attempt, restore path executes.

    Bug class: exception from _publish_normalized_events not caught by the
    outer try/except → restore_from_backup never called, lock not released,
    transactions table left empty after DELETE.
    """

    class AlwaysFailProducer:
        def produce(self, **kwargs) -> None:
            raise KafkaException("broker permanently down")

        def flush(self, timeout=None) -> None:
            pass

        def poll(self, timeout: int) -> None:
            pass

    user_id = uuid4()
    repo = InMemoryReprocessRepo(lock_present=True, snapshot_ids=[uuid4()])
    read_repo = InMemoryTransactionReadRepo(rows=[make_event(user_id=user_id)])
    orchestrator = ReprocessOrchestrator(
        repo=repo,
        transaction_read_repo=read_repo,
        producer=AlwaysFailProducer(),  # type: ignore[arg-type]
    )

    with pytest.raises(ReprocessError, match="restored from backup"):
        await orchestrator.reprocess_user(conn=None, user_id=user_id)

    assert repo.restore_called
    assert repo.release_called


# ---------------------------------------------------------------------------
# 10. Happy path: lock present, publish succeeds, verify passes → True returned
# ---------------------------------------------------------------------------


async def test_reprocess_user_returns_true_on_success() -> None:
    """reprocess_user returns True and calls release_lock_atomic on the success path.

    Bug class: release_lock_atomic not called on success → advisory lock held
    forever, next reprocess job for this user blocks indefinitely at acquire step.
    Also: returning False on success misleads the entrypoint's per-user logging.
    """
    user_id = uuid4()
    tx_id = uuid4()
    repo = InMemoryReprocessRepo(
        lock_present=True,
        snapshot_ids=[tx_id],
        count_for_user_value=1,  # catchup sees count == expected immediately
        verify_missing=[],  # verification passes
    )
    read_repo = InMemoryTransactionReadRepo(rows=[make_event(user_id=user_id)])
    producer = FakeProducer()
    orchestrator = _make_orchestrator(repo=repo, read_repo=read_repo, producer=producer)

    original_interval = mod._CATCHUP_POLL_INTERVAL_S
    original_timeout = mod._CATCHUP_TIMEOUT_S
    mod._CATCHUP_POLL_INTERVAL_S = 0.01
    mod._CATCHUP_TIMEOUT_S = 5.0
    try:
        result = await orchestrator.reprocess_user(conn=None, user_id=user_id)
    finally:
        mod._CATCHUP_POLL_INTERVAL_S = original_interval
        mod._CATCHUP_TIMEOUT_S = original_timeout

    assert result is True
    assert repo.release_called
    assert not repo.restore_called
