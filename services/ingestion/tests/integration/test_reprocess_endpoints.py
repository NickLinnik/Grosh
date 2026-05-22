"""Integration tests for the new reprocess endpoints (Slice 22).

Per-user cases:
  (a) Trigger → immediate second trigger → 409 REPROCESS_LOCKED.
  (b) Trigger → delete lock row → consumer sees absent lock, skips user cleanly.
  (c) K8s API raises ApiException on create → 502 JOB_SUBMISSION_FAILED, no lock row.
  (d) Auth: user A cannot trigger reprocess for user B → 403 INSUFFICIENT_PERMISSIONS.

Admin bulk cases:
  (a) POST /v1/admin/reprocess with user_ids=null → all users processed,
      skipped=[].
  (b) Pre-lock one of three users, bulk with all three → job started,
      pre-locked user skipped.
  (c) Pre-lock all users, bulk with user_ids=null → job_id=None,
      status_url=None, full skipped list.
"""

import os
from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import MagicMock
from uuid import UUID, uuid4

import asyncpg
import jwt
import pytest
import pytest_asyncio
from grosh_shared.db.rls import set_rls_user_id, set_rls_user_role
from httpx import ASGITransport, AsyncClient

from grosh_ingestion.deps import (
    get_db_conn,
    get_job_status_service,
    get_reprocess_dispatcher,
)
from grosh_ingestion.main import app
from grosh_ingestion.repositories.user_repo import UserRepo
from grosh_ingestion.services.job_status_service import JobStatusService
from grosh_ingestion.services.reprocess_dispatcher import ReprocessDispatcher

_JWT_SECRET = os.environ.get("JWT_SECRET", "super-secret-dev-jwt-key-change-in-prod")
_MOCK_JOB_NAME = "grosh-reprocess-abc12345-1234567890"


# ---------------------------------------------------------------------------
# Pool shim — makes app.state.pool return the test connection
# ---------------------------------------------------------------------------


class _SingleConnPool:
    """Wraps one asyncpg connection to satisfy pool.acquire() calls."""

    def __init__(self, conn: asyncpg.Connection) -> None:
        self._conn = conn

    def acquire(self) -> "_SingleConnPool":
        return self

    async def __aenter__(self) -> asyncpg.Connection:
        return self._conn

    async def __aexit__(self, *_: Any) -> None:
        pass


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_token(user_id: UUID, role: str = "member") -> str:
    payload = {
        "sub": str(user_id),
        "exp": datetime.now(UTC) + timedelta(minutes=15),
    }
    return jwt.encode(payload, _JWT_SECRET, algorithm="HS256")


async def _insert_user(
    conn: asyncpg.Connection, user_id: UUID, role: str = "member"
) -> None:
    await conn.execute(
        """
        INSERT INTO users
            (id, email, password_hash, display_name, role)
        VALUES
            ($1, $2, 'x', 'Test User', $3)
        """,
        user_id,
        f"test-{user_id}@example.com",
        role,
    )


async def _lock_exists(conn: asyncpg.Connection, user_id: UUID) -> bool:
    val = await conn.fetchval(
        "SELECT EXISTS(SELECT 1 FROM reprocessing_locks WHERE user_id = $1)",
        user_id,
    )
    return bool(val)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="function")
def mock_dispatcher() -> MagicMock:
    dispatcher = MagicMock(spec=ReprocessDispatcher)
    dispatcher.submit.return_value = _MOCK_JOB_NAME
    return dispatcher


@pytest.fixture(scope="function")
def mock_status_service() -> MagicMock:
    service = MagicMock(spec=JobStatusService)
    # Default: fetch_status returns None (job not found)
    service.fetch_status.return_value = None
    return service


@pytest_asyncio.fixture(loop_scope="session", scope="function")
async def client(
    conn: asyncpg.Connection,
    mock_dispatcher: MagicMock,
    mock_status_service: MagicMock,
) -> AsyncGenerator[AsyncClient, None]:
    app.state.pool = _SingleConnPool(conn)

    async def _override_db_conn() -> AsyncGenerator[asyncpg.Connection, None]:
        async with conn.transaction():
            yield conn

    prev_db_conn = app.dependency_overrides.get(get_db_conn)
    prev_dispatcher = app.dependency_overrides.get(get_reprocess_dispatcher)
    prev_status = app.dependency_overrides.get(get_job_status_service)

    app.dependency_overrides[get_db_conn] = _override_db_conn
    app.dependency_overrides[get_reprocess_dispatcher] = lambda: mock_dispatcher
    app.dependency_overrides[get_job_status_service] = lambda: mock_status_service

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as c:
        yield c

    if prev_db_conn is None:
        app.dependency_overrides.pop(get_db_conn, None)
    else:
        app.dependency_overrides[get_db_conn] = prev_db_conn

    if prev_dispatcher is None:
        app.dependency_overrides.pop(get_reprocess_dispatcher, None)
    else:
        app.dependency_overrides[get_reprocess_dispatcher] = prev_dispatcher

    if prev_status is None:
        app.dependency_overrides.pop(get_job_status_service, None)
    else:
        app.dependency_overrides[get_job_status_service] = prev_status


# ---------------------------------------------------------------------------
# Per-user: case (a) — double trigger → 409
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_per_user_second_trigger_returns_409(
    conn: asyncpg.Connection,
    client: AsyncClient,
    mock_dispatcher: MagicMock,
    mock_status_service: MagicMock,
) -> None:
    """Second trigger while lock exists → 409 REPROCESS_LOCKED with job details."""
    user_id = uuid4()
    await _insert_user(conn, user_id)
    token = _make_token(user_id)

    # Wire find_active_job to return the job name that the first trigger produces.
    mock_status_service.find_active_job.return_value = _MOCK_JOB_NAME

    resp1 = await client.post(
        f"/v1/users/{user_id}/reprocess",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp1.status_code == 202, resp1.text
    first_job_id = resp1.json()["job_id"]

    # Rewind the rate-limit timestamp so the second trigger is not rate-limited
    # (the lock row from the first call is still present — that's what causes 409).
    await conn.execute(
        """
        UPDATE users
        SET last_reprocess_started_at = now() - interval '1 hour 1 minute'
        WHERE id = $1
        """,
        user_id,
    )

    resp2 = await client.post(
        f"/v1/users/{user_id}/reprocess",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp2.status_code == 409, resp2.text
    body = resp2.json()
    assert body["code"] == "REPROCESS_LOCKED"

    expected_status_url = f"/v1/users/{user_id}/reprocess/{first_job_id}"
    detail = body["detail"]
    assert first_job_id in detail
    assert expected_status_url in detail
    assert f"Reprocessing already in progress for user {user_id}" in detail
    assert f"Existing job: {first_job_id}" in detail
    assert f"Poll {expected_status_url} for progress" in detail

    # dispatcher.submit called exactly once (second trigger rejected before submit)
    mock_dispatcher.submit.assert_called_once()


# ---------------------------------------------------------------------------
# Per-user: case (b) — lock deleted before pod starts → consumer skips cleanly
# (tested as a unit-level assertion on the service logic)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_consumer_skips_when_lock_absent(
    conn: asyncpg.Connection,
) -> None:
    """Consumer reprocess_user returns False when lock absent (no txn touched)."""
    from unittest.mock import MagicMock

    from grosh_normalization.repositories.reprocess_repo import ReprocessRepo
    from grosh_normalization.repositories.transaction_read_repo import (
        TransactionReadRepo,
    )
    from grosh_normalization.services.reprocess_orchestrator import (
        ReprocessOrchestrator,
    )

    user_id = uuid4()
    # Do NOT insert a lock row — simulate the consumer being called spuriously

    mock_producer = MagicMock()
    mock_producer.flush = MagicMock()

    repo = ReprocessRepo()
    orchestrator = ReprocessOrchestrator(repo, TransactionReadRepo(), mock_producer)

    result = await orchestrator.reprocess_user(conn, user_id)

    assert result is False
    # Ensure no lock row was created (consumer must not insert)
    assert not await _lock_exists(conn, user_id)
    # Advisory lock NOT acquired (would fail on non-existent lock).
    # Verify by checking transactions were not touched (no exception = no writes).
    tx_count = await conn.fetchval(
        "SELECT COUNT(*) FROM transactions WHERE user_id = $1", user_id
    )
    assert tx_count == 0


@pytest.mark.asyncio
async def test_consumer_proceeds_when_lock_exists(
    conn: asyncpg.Connection,
) -> None:
    """Consumer reprocess_user proceeds past lock assertion when lock row exists."""
    from unittest.mock import MagicMock

    from grosh_normalization.repositories.reprocess_repo import ReprocessRepo
    from grosh_normalization.repositories.transaction_read_repo import (
        TransactionReadRepo,
    )
    from grosh_normalization.services.reprocess_orchestrator import (
        ReprocessOrchestrator,
    )

    user_id = uuid4()
    # reprocessing_locks has a FK on users.id — insert the user first.
    await conn.execute(
        """
        INSERT INTO users (id, email, password_hash, display_name, role)
        VALUES ($1, $2, 'x', 'Reprocess Test', 'member')
        """,
        user_id,
        f"reprocess-test-{user_id}@example.com",
    )
    # Insert the lock row (simulating what the ingestion API does)
    await conn.execute(
        "INSERT INTO reprocessing_locks (user_id) VALUES ($1)",
        user_id,
    )

    mock_producer = MagicMock()
    mock_producer.flush = MagicMock()
    mock_producer.produce = MagicMock()
    mock_producer.poll = MagicMock()

    repo = ReprocessRepo()
    orchestrator = ReprocessOrchestrator(repo, TransactionReadRepo(), mock_producer)

    # The orchestrator will proceed past the lock assertion and attempt the full
    # pipeline. With no transactions to process it should succeed (0 items).
    result = await orchestrator.reprocess_user(conn, user_id)

    assert result is True
    # Lock row should be deleted (release_lock_atomic ran)
    assert not await _lock_exists(conn, user_id)


# ---------------------------------------------------------------------------
# Per-user: case (c) — K8s failure → 502, lock row rolled back
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_per_user_k8s_failure_returns_502_and_no_lock(
    conn: asyncpg.Connection,
    client: AsyncClient,
    mock_dispatcher: MagicMock,
) -> None:
    """K8s submit fails → 502 JOB_SUBMISSION_FAILED, lock row does not persist."""
    user_id = uuid4()
    await _insert_user(conn, user_id)
    from grosh_ingestion.errors import K8sDispatchError

    mock_dispatcher.submit.side_effect = K8sDispatchError("K8s API exploded")
    token = _make_token(user_id)

    resp = await client.post(
        f"/v1/users/{user_id}/reprocess",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert resp.status_code == 502, resp.text
    body = resp.json()
    assert body["code"] == "JOB_SUBMISSION_FAILED"

    # Lock row must not exist — transaction rolled back
    assert not await _lock_exists(conn, user_id)

    # Reset side effect for other tests
    mock_dispatcher.submit.side_effect = None
    mock_dispatcher.submit.return_value = _MOCK_JOB_NAME


# ---------------------------------------------------------------------------
# Per-user: case (d) — user A cannot reprocess user B → 403
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_per_user_cross_user_auth_forbidden(
    conn: asyncpg.Connection,
    client: AsyncClient,
) -> None:
    """User A tries to trigger reprocess for user B → 403 INSUFFICIENT_PERMISSIONS."""
    user_a = uuid4()
    user_b = uuid4()
    await _insert_user(conn, user_a)
    await _insert_user(conn, user_b)
    token_a = _make_token(user_a)

    resp = await client.post(
        f"/v1/users/{user_b}/reprocess",
        headers={"Authorization": f"Bearer {token_a}"},
    )

    assert resp.status_code == 403, resp.text
    body = resp.json()
    assert body["code"] == "INSUFFICIENT_PERMISSIONS"


# ---------------------------------------------------------------------------
# Admin bulk: case (a) — null user_ids → all users processed
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_admin_bulk_explicit_user_ids_processes_all(
    conn: asyncpg.Connection,
    client: AsyncClient,
    mock_dispatcher: MagicMock,
) -> None:
    """POST /v1/admin/reprocess with explicit user_ids → those users processed."""
    admin_id = uuid4()
    user1 = uuid4()
    user2 = uuid4()
    await _insert_user(conn, admin_id, role="admin")
    await _insert_user(conn, user1)
    await _insert_user(conn, user2)
    token = _make_token(admin_id)

    resp = await client.post(
        "/v1/admin/reprocess",
        json={"user_ids": [str(admin_id), str(user1), str(user2)]},
        headers={"Authorization": f"Bearer {token}"},
    )

    assert resp.status_code == 202, resp.text
    body = resp.json()
    assert body["job_id"] == _MOCK_JOB_NAME
    assert body["status_url"] == f"/v1/admin/reprocess/{_MOCK_JOB_NAME}"
    assert body["skipped"] == []

    # All three users should have lock rows
    assert await _lock_exists(conn, admin_id)
    assert await _lock_exists(conn, user1)
    assert await _lock_exists(conn, user2)

    submit_call = mock_dispatcher.submit.call_args
    submitted_ids = set(submit_call[0][0])  # first positional arg: user_ids list
    assert submitted_ids == {admin_id, user1, user2}
    assert submit_call[0][1] is None  # caller_user_id is None for admin bulk


@pytest.mark.asyncio
async def test_admin_bulk_null_user_ids_includes_snapshot(
    conn: asyncpg.Connection,
    client: AsyncClient,
    mock_dispatcher: MagicMock,
) -> None:
    """POST /v1/admin/reprocess with user_ids=null uses a snapshot of all current users.

    The snapshot includes ALL users in the DB at query time. This test verifies
    that the three freshly inserted users are included in the job and that the
    response has job_id set and skipped=[].
    """
    admin_id = uuid4()
    user1 = uuid4()
    user2 = uuid4()
    await _insert_user(conn, admin_id, role="admin")
    await _insert_user(conn, user1)
    await _insert_user(conn, user2)
    token = _make_token(admin_id)

    resp = await client.post(
        "/v1/admin/reprocess",
        json={"user_ids": None},
        headers={"Authorization": f"Bearer {token}"},
    )

    assert resp.status_code == 202, resp.text
    body = resp.json()
    assert body["job_id"] is not None
    assert body["status_url"] is not None
    # No skipped users (none were pre-locked)
    assert body["skipped"] == []

    # Our three users must have lock rows
    assert await _lock_exists(conn, admin_id)
    assert await _lock_exists(conn, user1)
    assert await _lock_exists(conn, user2)

    # Submitted set includes at least our three users (may include others from session)
    submit_call = mock_dispatcher.submit.call_args
    submitted_ids = set(submit_call[0][0])
    assert {admin_id, user1, user2}.issubset(submitted_ids)
    assert submit_call[0][1] is None  # caller_user_id is None for admin bulk


# ---------------------------------------------------------------------------
# Admin bulk: case (b) — one pre-locked user → skipped in response
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_admin_bulk_pre_locked_user_is_skipped(
    conn: asyncpg.Connection,
    client: AsyncClient,
    mock_dispatcher: MagicMock,
) -> None:
    """Pre-lock 1 of 3 users; bulk with all 3 → job_id set, 1 user skipped."""
    admin_id = uuid4()
    user1 = uuid4()
    user2 = uuid4()
    user3 = uuid4()
    await _insert_user(conn, admin_id, role="admin")
    await _insert_user(conn, user1)
    await _insert_user(conn, user2)
    await _insert_user(conn, user3)

    # Pre-lock user2
    await conn.execute("INSERT INTO reprocessing_locks (user_id) VALUES ($1)", user2)

    token = _make_token(admin_id)
    resp = await client.post(
        "/v1/admin/reprocess",
        json={"user_ids": [str(user1), str(user2), str(user3)]},
        headers={"Authorization": f"Bearer {token}"},
    )

    assert resp.status_code == 202, resp.text
    body = resp.json()
    assert body["job_id"] is not None
    assert body["status_url"] is not None

    skipped = body["skipped"]
    assert len(skipped) == 1
    assert UUID(skipped[0]["user_id"]) == user2
    assert skipped[0]["reason"] == "REPROCESS_LOCKED"

    # user1 and user3 should have been submitted
    submit_call = mock_dispatcher.submit.call_args
    submitted_ids = set(submit_call[0][0])
    assert user1 in submitted_ids
    assert user3 in submitted_ids
    assert user2 not in submitted_ids


# ---------------------------------------------------------------------------
# Admin bulk: case (c) — all users locked → job_id=None
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_admin_bulk_all_locked_returns_null_job_id(
    conn: asyncpg.Connection,
    client: AsyncClient,
    mock_dispatcher: MagicMock,
) -> None:
    """All explicit target users already locked → job_id=None, status_url=None."""
    admin_id = uuid4()
    user1 = uuid4()
    user2 = uuid4()
    await _insert_user(conn, admin_id, role="admin")
    await _insert_user(conn, user1)
    await _insert_user(conn, user2)

    # Pre-lock all three explicitly targeted users
    await conn.execute("INSERT INTO reprocessing_locks (user_id) VALUES ($1)", admin_id)
    await conn.execute("INSERT INTO reprocessing_locks (user_id) VALUES ($1)", user1)
    await conn.execute("INSERT INTO reprocessing_locks (user_id) VALUES ($1)", user2)

    token = _make_token(admin_id)
    resp = await client.post(
        "/v1/admin/reprocess",
        json={"user_ids": [str(admin_id), str(user1), str(user2)]},
        headers={"Authorization": f"Bearer {token}"},
    )

    assert resp.status_code == 202, resp.text
    body = resp.json()
    assert body["job_id"] is None
    assert body["status_url"] is None
    assert len(body["skipped"]) == 3
    skipped_ids = {UUID(s["user_id"]) for s in body["skipped"]}
    assert skipped_ids == {admin_id, user1, user2}

    mock_dispatcher.submit.assert_not_called()

    mock_dispatcher.submit.assert_not_called()


# ---------------------------------------------------------------------------
# Slice 33: app.current_user_role-aware list_all_ids under production RLS
#
# Uses the conn fixture (grosh_admin) but drops the table-owner bypass via
# `SET LOCAL ROLE grosh_ingestion` so the production RLS regime fires. The
# conn fixture's outer transaction rolls back automatically at test end,
# reverting the role switch and discarding the seeded rows.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_admin_list_all_ids_returns_all_users_with_role_set(
    conn: asyncpg.Connection,
) -> None:
    """With app.current_user_role='admin', list_all_ids returns every user.

    Before Slice 33, the ingestion service set only app.current_user_id;
    the users_admin_all policy carve-out (from migration 0013) never
    triggered, and list_all_ids was silently RLS-filtered to the admin's
    own row only.
    """
    admin_id = uuid4()
    member1_id = uuid4()
    member2_id = uuid4()

    # Seed as grosh_admin (RLS-exempt) before the role switch.
    await _insert_user(conn, admin_id, role="admin")
    await _insert_user(conn, member1_id)
    await _insert_user(conn, member2_id)

    # Drop the table-owner bypass and impersonate an admin caller. The
    # users_admin_all carve-out is what makes all rows visible.
    await conn.execute("SET LOCAL ROLE grosh_ingestion")
    await set_rls_user_id(conn, admin_id)
    await set_rls_user_role(conn, "admin")

    repo = UserRepo()
    all_ids = await repo.list_all_ids(conn)
    assert set(all_ids) >= {
        admin_id,
        member1_id,
        member2_id,
    }, f"Admin role should see all seeded users, got {all_ids}"


@pytest.mark.asyncio
async def test_member_list_all_ids_returns_only_self_under_rls(
    conn: asyncpg.Connection,
) -> None:
    """A non-admin caller under production RLS sees only their own row in users.

    Confirms the Slice 33 auth-setup change does not widen non-admin
    visibility.
    """
    member1_id = uuid4()
    member2_id = uuid4()
    member3_id = uuid4()

    await _insert_user(conn, member1_id)
    await _insert_user(conn, member2_id)
    await _insert_user(conn, member3_id)

    await conn.execute("SET LOCAL ROLE grosh_ingestion")
    await set_rls_user_id(conn, member1_id)
    await set_rls_user_role(conn, "member")

    repo = UserRepo()
    all_ids = await repo.list_all_ids(conn)
    # RLS filters to the caller's own row. The users_isolation_select policy
    # only matches `id = app.current_user_id()`; the admin carve-out doesn't
    # fire because role != 'admin'.
    assert all_ids == [
        member1_id
    ], f"Member role should see only own row, got {all_ids}"


# ---------------------------------------------------------------------------
# Slice 34: reprocess rate-limit tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_reprocess_rate_limit_blocks_second_trigger(
    conn: asyncpg.Connection,
    client: AsyncClient,
    mock_dispatcher: MagicMock,
) -> None:
    """Second trigger within 1h window → 429; time-travel → 202 again."""
    user_a = uuid4()
    await _insert_user(conn, user_a)
    token = _make_token(user_a)

    # First trigger — no prior timestamp → slot claimed, job submitted.
    resp1 = await client.post(
        f"/v1/users/{user_a}/reprocess",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp1.status_code == 202, resp1.text

    # Second trigger immediately — within the 1-hour window → rate-limited.
    resp2 = await client.post(
        f"/v1/users/{user_a}/reprocess",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp2.status_code == 429, resp2.text
    body = resp2.json()
    assert body["code"] == "RATE_LIMITED"
    assert "Next reprocess allowed at" in body["detail"]

    # Simulate 1h+1m passing by rewinding last_reprocess_started_at.
    await conn.execute(
        """
        UPDATE users
        SET last_reprocess_started_at = now() - interval '1 hour 1 minute'
        WHERE id = $1
        """,
        user_a,
    )
    # Also clear the lock so the third trigger is not blocked by REPROCESS_LOCKED.
    await conn.execute(
        "DELETE FROM reprocessing_locks WHERE user_id = $1",
        user_a,
    )

    # Third trigger — now outside the window → 202 again.
    resp3 = await client.post(
        f"/v1/users/{user_a}/reprocess",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp3.status_code == 202, resp3.text


@pytest.mark.asyncio
async def test_admin_force_bypasses_check_and_updates_timestamp(
    conn: asyncpg.Connection,
    client: AsyncClient,
    mock_dispatcher: MagicMock,
) -> None:
    """force=true bypasses rate-limit; subsequent non-force is rate-limited."""
    admin_id = uuid4()
    user_a = uuid4()
    await _insert_user(conn, admin_id, role="admin")
    await _insert_user(conn, user_a)

    # Seed user_a with a recent timestamp (5 minutes ago — within window).
    await conn.execute(
        """
        UPDATE users
        SET last_reprocess_started_at = now() - interval '5 minutes'
        WHERE id = $1
        """,
        user_a,
    )

    token = _make_token(admin_id)

    # Force=true → bypass the rate-limit, update the timestamp, submit job.
    resp1 = await client.post(
        "/v1/admin/reprocess",
        json={"user_ids": [str(user_a)], "force": True},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp1.status_code == 202, resp1.text
    body1 = resp1.json()
    skipped_ids = [s["user_id"] for s in body1["skipped"]]
    assert str(user_a) not in skipped_ids

    # Clear the lock so the second call can attempt insertion.
    await conn.execute(
        "DELETE FROM reprocessing_locks WHERE user_id = $1",
        user_a,
    )

    # Non-force call now → user_a was updated to now() by the force call → RATE_LIMITED.
    resp2 = await client.post(
        "/v1/admin/reprocess",
        json={"user_ids": [str(user_a)], "force": False},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp2.status_code == 202, resp2.text
    body2 = resp2.json()
    skipped = body2["skipped"]
    assert len(skipped) == 1
    assert UUID(skipped[0]["user_id"]) == user_a
    assert skipped[0]["reason"] == "RATE_LIMITED"


@pytest.mark.asyncio
async def test_reprocess_rollback_on_k8s_api_exception(
    conn: asyncpg.Connection,
    client: AsyncClient,
    mock_dispatcher: MagicMock,
) -> None:
    """K8sDispatchError (wrapping ApiException) → 502, state rolled back.

    The dispatcher wraps kubernetes ApiException into K8sDispatchError before
    propagating. The exception handler returns 502 and the transaction rolls
    back — leaving both timestamp and lock row absent.
    """
    from grosh_ingestion.errors import K8sDispatchError

    user_a = uuid4()
    await _insert_user(conn, user_a)
    token = _make_token(user_a)

    mock_dispatcher.submit.side_effect = K8sDispatchError(
        "Cannot create reprocess job: (500) Reason: Internal Server Error"
    )

    resp = await client.post(
        f"/v1/users/{user_a}/reprocess",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 502, resp.text
    assert resp.json()["code"] == "JOB_SUBMISSION_FAILED"

    # Timestamp must be NULL — transaction was rolled back.
    ts = await conn.fetchval(
        "SELECT last_reprocess_started_at FROM users WHERE id = $1",
        user_a,
    )
    assert ts is None, f"Expected NULL timestamp after rollback, got {ts}"

    # No lock row left.
    assert not await _lock_exists(conn, user_a)

    mock_dispatcher.submit.side_effect = None
    mock_dispatcher.submit.return_value = _MOCK_JOB_NAME


@pytest.mark.asyncio
async def test_reprocess_rollback_on_k8s_timeout(
    conn: asyncpg.Connection,
    client: AsyncClient,
    mock_dispatcher: MagicMock,
) -> None:
    """urllib3 ReadTimeoutError from dispatcher → 502, timestamp/lock rolled back."""
    from grosh_ingestion.errors import K8sDispatchError

    user_a = uuid4()
    await _insert_user(conn, user_a)
    token = _make_token(user_a)

    # The dispatcher wraps urllib3 timeout into K8sDispatchError.
    mock_dispatcher.submit.side_effect = K8sDispatchError(
        "Cannot create reprocess job: Read timed out"
    )

    resp = await client.post(
        f"/v1/users/{user_a}/reprocess",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 502, resp.text
    assert resp.json()["code"] == "JOB_SUBMISSION_FAILED"

    ts = await conn.fetchval(
        "SELECT last_reprocess_started_at FROM users WHERE id = $1",
        user_a,
    )
    assert ts is None, f"Expected NULL timestamp after rollback, got {ts}"

    assert not await _lock_exists(conn, user_a)

    mock_dispatcher.submit.side_effect = None
    mock_dispatcher.submit.return_value = _MOCK_JOB_NAME
