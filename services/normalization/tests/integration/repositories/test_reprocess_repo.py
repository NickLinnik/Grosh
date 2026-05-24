"""Integration tests for ReprocessRepo and TransactionReadRepo.

Both repos hit real SQL — correctness of queries (row_to_json round-trip,
jsonb_populate_record restore, ANY($2::uuid[]) lookup, pg_locks-based staleness)
can only be verified against a real Postgres instance. Each test uses the
rolled-back-transaction fixture so state never leaks between tests.

ReprocessRepo scenarios:
  1. lock_exists: returns False when absent, True when row present.
  2. count_for_user: returns correct integer count.
  3. list_all_user_ids: includes users with transactions, excludes users without.
  4. snapshot_transactions: inserts backup row and returns IDs.
  5. snapshot_transactions per-user scoping: user A snapshot does not include
     user B's rows (query has WHERE user_id = $1).
  6. snapshot_transactions empty: backup row created, empty ID list returned.
  7. delete_user_transactions: removes all rows for that user, not another user's.
  8. verify_snapshot: all IDs present → empty list.
  9. verify_snapshot: deleted ID → returned as missing.
 10. verify_snapshot: empty snapshot_ids → empty list (early return path).
 11. restore_from_backup: re-inserts deleted transactions; idempotent on conflict.
 12. restore_from_backup: raises ReprocessError when no backup row exists.

TransactionReadRepo scenarios:
 13. select_for_user: rows returned in (time, id) ascending order.
 14. select_for_user: scopes to requesting user — excludes other user's rows.
"""

from datetime import UTC, datetime
from uuid import UUID, uuid4

import asyncpg
import pytest

from grosh_normalization.repositories.reprocess_repo import (
    ReprocessError,
    ReprocessRepo,
)
from grosh_normalization.repositories.transaction_read_repo import TransactionReadRepo
from tests.integration.conftest import insert_account, insert_transaction, insert_user

pytestmark = pytest.mark.asyncio

_repo = ReprocessRepo()
_read_repo = TransactionReadRepo()

_T1 = datetime(2025, 1, 1, tzinfo=UTC)
_T2 = datetime(2025, 2, 1, tzinfo=UTC)
_T3 = datetime(2025, 3, 1, tzinfo=UTC)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


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


async def _setup_user_with_account(conn: asyncpg.Connection) -> tuple[UUID, UUID]:
    """Insert a user + account and return (user_id, account_id)."""
    user_id = await insert_user(conn)
    account_id = await insert_account(
        conn, user_id=user_id, type="black", currency_code="UAH"
    )
    return user_id, account_id


# ---------------------------------------------------------------------------
# 1. lock_exists
# ---------------------------------------------------------------------------


async def test_lock_exists_returns_false_when_absent(conn: asyncpg.Connection) -> None:
    """lock_exists returns False when no reprocessing_locks row exists.

    Bug class: orchestrator would skip reprocessing for a live user if this
    returns True incorrectly (false positive on absent row).
    """
    user_id = await insert_user(conn)

    result = await _repo.lock_exists(conn, user_id)

    assert result is False


async def test_lock_exists_returns_true_when_row_present(
    conn: asyncpg.Connection,
) -> None:
    """lock_exists returns True when the lock row exists.

    Bug class: orchestrator proceeds with live user when lock row was deleted
    early (false negative on present row).
    """
    user_id = await insert_user(conn)
    await conn.execute("INSERT INTO reprocessing_locks (user_id) VALUES ($1)", user_id)

    result = await _repo.lock_exists(conn, user_id)

    assert result is True


# ---------------------------------------------------------------------------
# 2. count_for_user
# ---------------------------------------------------------------------------


async def test_count_for_user_returns_correct_count(conn: asyncpg.Connection) -> None:
    """count_for_user returns the actual row count, not a cached or static value.

    Bug class: catchup wait terminates early (count reads wrong) → verification
    fails → unnecessary restore triggered.
    """
    user_id, account_id = await _setup_user_with_account(conn)
    await insert_transaction(
        conn,
        user_id=user_id,
        account_id=account_id,
        time=_T1,
        amount_cents=1000,
        direction="expense",
    )
    await insert_transaction(
        conn,
        user_id=user_id,
        account_id=account_id,
        time=_T2,
        amount_cents=2000,
        direction="income",
    )

    count = await _repo.count_for_user(conn, user_id)

    assert count == 2


# ---------------------------------------------------------------------------
# 3. list_all_user_ids
# ---------------------------------------------------------------------------


async def test_list_all_user_ids_includes_users_with_transactions(
    conn: asyncpg.Connection,
) -> None:
    """list_all_user_ids returns users that have at least one transaction.

    Bug class: reprocess job skips a user with transactions entirely (missed
    from DISTINCT query) or processes a user who has no rows (wasteful but safe).
    """
    user_id, account_id = await _setup_user_with_account(conn)
    await insert_transaction(
        conn,
        user_id=user_id,
        account_id=account_id,
        time=_T1,
        amount_cents=500,
        direction="expense",
    )

    ids = await _repo.list_all_user_ids(conn)

    assert user_id in ids


async def test_list_all_user_ids_excludes_users_without_transactions(
    conn: asyncpg.Connection,
) -> None:
    """list_all_user_ids excludes users that have no transactions.

    Bug class: reprocess job wastes a full reprocess cycle on a user with
    zero rows, potentially acquiring and releasing locks unnecessarily.
    """
    user_id_no_tx = await insert_user(conn)

    ids = await _repo.list_all_user_ids(conn)

    assert user_id_no_tx not in ids


# ---------------------------------------------------------------------------
# 4. snapshot_transactions: round-trip and backup creation
# ---------------------------------------------------------------------------


async def test_snapshot_transactions_creates_backup_and_returns_ids(
    conn: asyncpg.Connection,
) -> None:
    """snapshot_transactions inserts a backup row and returns the transaction IDs.

    Bug class: IDs returned don't match actual snapshot → verify_snapshot
    reports false positives; backup row not inserted → restore_from_backup
    raises ReprocessError on the recovery path.
    """
    user_id, account_id = await _setup_user_with_account(conn)
    tx_id = await insert_transaction(
        conn,
        user_id=user_id,
        account_id=account_id,
        time=_T1,
        amount_cents=10000,
        direction="expense",
    )

    returned_ids = await _repo.snapshot_transactions(conn, user_id)

    assert tx_id in returned_ids
    assert len(returned_ids) == 1
    assert await _backup_count(conn, user_id) == 1


async def test_snapshot_transactions_empty_user_creates_backup_row(
    conn: asyncpg.Connection,
) -> None:
    """snapshot_transactions for a user with no transactions still creates a backup.

    Bug class: restore_from_backup raises ReprocessError when a user has zero
    transactions because no backup row was written, breaking the recovery path.
    """
    user_id = await insert_user(conn)

    returned_ids = await _repo.snapshot_transactions(conn, user_id)

    assert returned_ids == []
    assert await _backup_count(conn, user_id) == 1


# ---------------------------------------------------------------------------
# 5. snapshot_transactions per-user scoping
# ---------------------------------------------------------------------------


async def test_snapshot_transactions_scoped_to_requesting_user(
    conn: asyncpg.Connection,
) -> None:
    """snapshot_transactions returns only the requesting user's transactions.

    Bug class: missing WHERE user_id = $1 in snapshot query → user A's backup
    contains user B's rows → restore overwrites wrong user's data.
    """
    user_a, account_a = await _setup_user_with_account(conn)
    user_b, account_b = await _setup_user_with_account(conn)

    tx_a = await insert_transaction(
        conn,
        user_id=user_a,
        account_id=account_a,
        time=_T1,
        amount_cents=100,
        direction="expense",
    )
    await insert_transaction(
        conn,
        user_id=user_b,
        account_id=account_b,
        time=_T2,
        amount_cents=200,
        direction="income",
    )

    ids_a = await _repo.snapshot_transactions(conn, user_a)

    assert tx_a in ids_a
    assert len(ids_a) == 1  # user B's tx must not appear


# ---------------------------------------------------------------------------
# 6. delete_user_transactions
# ---------------------------------------------------------------------------


async def test_delete_user_transactions_removes_only_target_user(
    conn: asyncpg.Connection,
) -> None:
    """delete_user_transactions removes all rows for the target user, no others.

    Bug class: missing WHERE user_id = $1 (DELETE without predicate) wipes
    all users' transactions at once — catastrophic data loss.
    """
    user_a, account_a = await _setup_user_with_account(conn)
    user_b, account_b = await _setup_user_with_account(conn)

    await insert_transaction(
        conn,
        user_id=user_a,
        account_id=account_a,
        time=_T1,
        amount_cents=1000,
        direction="expense",
    )
    await insert_transaction(
        conn,
        user_id=user_b,
        account_id=account_b,
        time=_T2,
        amount_cents=2000,
        direction="income",
    )

    await _repo.delete_user_transactions(conn, user_a)

    assert await _tx_count(conn, user_a) == 0
    assert await _tx_count(conn, user_b) == 1


# ---------------------------------------------------------------------------
# 7. verify_snapshot
# ---------------------------------------------------------------------------


async def test_verify_snapshot_returns_empty_when_all_ids_present(
    conn: asyncpg.Connection,
) -> None:
    """verify_snapshot returns [] when all snapshot IDs exist in transactions.

    Bug class: false missing IDs → orchestrator triggers unnecessary restore
    after a successful replay, overwriting the re-enriched transactions.
    """
    user_id, account_id = await _setup_user_with_account(conn)
    tx_id = await insert_transaction(
        conn,
        user_id=user_id,
        account_id=account_id,
        time=_T1,
        amount_cents=5000,
        direction="expense",
    )

    missing = await _repo.verify_snapshot(conn, user_id, [tx_id])

    assert missing == []


async def test_verify_snapshot_returns_missing_ids_when_row_deleted(
    conn: asyncpg.Connection,
) -> None:
    """verify_snapshot returns IDs whose transactions were not re-written.

    Bug class: verify_snapshot passes on incomplete replay → enrichment
    service sees fewer rows than expected without any error signal.
    """
    user_id = await insert_user(conn)
    ghost_id = uuid4()

    missing = await _repo.verify_snapshot(conn, user_id, [ghost_id])

    assert ghost_id in missing


async def test_verify_snapshot_empty_snapshot_ids_returns_empty(
    conn: asyncpg.Connection,
) -> None:
    """verify_snapshot with empty snapshot_ids returns [] via early return.

    Bug class: ANY($2::uuid[]) with an empty array hits a Postgres exception
    or returns unexpected results instead of the documented empty list.
    """
    user_id = await insert_user(conn)

    missing = await _repo.verify_snapshot(conn, user_id, [])

    assert missing == []


# ---------------------------------------------------------------------------
# 8. restore_from_backup
# ---------------------------------------------------------------------------


async def test_restore_from_backup_reinserts_deleted_transactions(
    conn: asyncpg.Connection,
) -> None:
    """restore_from_backup brings back exactly the rows that were deleted.

    Bug class: jsonb_populate_record casts wrong column types → INSERT fails
    or inserts corrupt data; backup row read incorrectly → wrong user's data restored.
    """
    user_id, account_id = await _setup_user_with_account(conn)
    tx_id = await insert_transaction(
        conn,
        user_id=user_id,
        account_id=account_id,
        time=_T1,
        amount_cents=99900,
        direction="expense",
    )

    await _repo.snapshot_transactions(conn, user_id)
    await conn.execute("DELETE FROM transactions WHERE id = $1", tx_id)
    assert await _tx_count(conn, user_id) == 0

    await _repo.restore_from_backup(conn, user_id)

    assert await _tx_count(conn, user_id) == 1
    restored = await conn.fetchrow(
        "SELECT id, amount_cents FROM transactions WHERE user_id = $1", user_id
    )
    assert restored is not None
    assert restored["id"] == tx_id
    assert restored["amount_cents"] == 99900


async def test_restore_from_backup_is_idempotent_on_conflict(
    conn: asyncpg.Connection,
) -> None:
    """restore_from_backup with ON CONFLICT DO NOTHING handles already-present IDs.

    Bug class: if restore is called twice (retry scenario), it must not raise
    a unique-constraint error and must not create duplicate rows.
    """
    user_id, account_id = await _setup_user_with_account(conn)
    await insert_transaction(
        conn,
        user_id=user_id,
        account_id=account_id,
        time=_T2,
        amount_cents=1500,
        direction="income",
    )

    await _repo.snapshot_transactions(conn, user_id)
    # Transaction is still present — restore should silently skip on conflict.
    await _repo.restore_from_backup(conn, user_id)

    assert await _tx_count(conn, user_id) == 1


async def test_restore_from_backup_raises_when_no_backup_exists(
    conn: asyncpg.Connection,
) -> None:
    """restore_from_backup raises ReprocessError when no backup row exists.

    Bug class: silent failure on missing backup → recovery path returns normally
    without restoring data; user's transaction history is silently lost.
    """
    user_id = await insert_user(conn)

    with pytest.raises(ReprocessError, match="No backup found"):
        await _repo.restore_from_backup(conn, user_id)


# ---------------------------------------------------------------------------
# 9. TransactionReadRepo.select_for_user
# ---------------------------------------------------------------------------


async def test_select_for_user_returns_rows_in_time_id_order(
    conn: asyncpg.Connection,
) -> None:
    """select_for_user returns rows ordered by (time ASC, id ASC).

    Bug class: out-of-order replay events cause the enrichment service to
    compute incorrect running balances or mismatch transfer pairs.
    """
    user_id, account_id = await _setup_user_with_account(conn)
    tx_early = await insert_transaction(
        conn,
        user_id=user_id,
        account_id=account_id,
        time=_T1,
        amount_cents=100,
        direction="expense",
    )
    tx_late = await insert_transaction(
        conn,
        user_id=user_id,
        account_id=account_id,
        time=_T3,
        amount_cents=200,
        direction="income",
    )
    tx_mid = await insert_transaction(
        conn,
        user_id=user_id,
        account_id=account_id,
        time=_T2,
        amount_cents=300,
        direction="expense",
    )

    rows = await _read_repo.select_for_user(conn, user_id)

    assert len(rows) == 3
    assert rows[0].id == tx_early
    assert rows[1].id == tx_mid
    assert rows[2].id == tx_late


async def test_select_for_user_scoped_to_requesting_user(
    conn: asyncpg.Connection,
) -> None:
    """select_for_user returns only the requesting user's rows.

    Bug class: missing WHERE user_id = $1 → replay publishes another user's
    transactions under the wrong user_id; data corruption in enrichment.
    """
    user_a, account_a = await _setup_user_with_account(conn)
    user_b, account_b = await _setup_user_with_account(conn)

    tx_a = await insert_transaction(
        conn,
        user_id=user_a,
        account_id=account_a,
        time=_T1,
        amount_cents=100,
        direction="expense",
    )
    await insert_transaction(
        conn,
        user_id=user_b,
        account_id=account_b,
        time=_T2,
        amount_cents=200,
        direction="income",
    )

    rows_a = await _read_repo.select_for_user(conn, user_a)

    assert len(rows_a) == 1
    assert rows_a[0].id == tx_a
    assert rows_a[0].user_id == user_a
