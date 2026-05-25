"""Integration tests for FOR UPDATE SKIP LOCKED and advisory lock behavior.

These tests require two concurrent database connections. The `conn` fixture
provides the first (per-test rolled-back) connection. The second connection
is acquired directly from `db_pool` and managed manually.

IMPORTANT: rows inserted in `conn` (inside a rolled-back transaction) are NOT
visible to a second connection that runs OUTSIDE that transaction. To test
concurrent locking, we use two separate connections that are both within
their own committed (or not-yet-committed) transactions.

The correct approach for SKIP LOCKED integration tests:
- Use two separate connections from the pool, each managing their own
  BEGIN/COMMIT. The test method itself handles teardown (DELETE rows).
- We do NOT use the `conn` fixture for data setup in these tests because
  its transaction is invisible to other connections.
"""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

pytestmark = pytest.mark.asyncio

T = datetime(2026, 6, 1, 12, 0, 0, tzinfo=UTC)
MCC_TRANSFER = "4829"
USER_EMAIL_COUNTER = [0]


def _unique_email() -> str:
    USER_EMAIL_COUNTER[0] += 1
    return f"lock-test-{USER_EMAIL_COUNTER[0]}@example.com"


async def _setup_committed_data(pool) -> tuple[str, str, str, str, str, str]:
    """Insert user, two accounts, and two transactions — all committed.

    Returns (user_id, fop_account_id, black_account_id, partner_tx_id, black_iban,
             incoming_tx_id).

    partner_tx_id  — the pre-existing income on the black account (the row that
                     the locking tests will try to claim via FOR UPDATE SKIP LOCKED).
    incoming_tx_id — a pre-inserted FOP expense that can be used as a valid
                     related_transaction_id when claiming partner_tx_id (satisfies
                     the DEFERRABLE INITIALLY DEFERRED FK at commit time).

    Uses a fresh connection outside any transaction so data is visible to all.
    """
    async with pool.acquire() as setup_conn:
        uid = uuid4()
        await setup_conn.execute(
            """
            INSERT INTO users (id, email, password_hash, display_name, role)
            VALUES ($1, $2, 'hash', 'Lock Test User', 'member')
            """,
            uid,
            _unique_email(),
        )
        fop_id = uuid4()
        await setup_conn.execute(
            """
            INSERT INTO accounts (id, user_id, source, type, currency_code, iban)
            VALUES ($1, $2, 'monobank', 'fop', 'UAH', $3)
            """,
            fop_id,
            uid,
            f"UA111{uuid4().hex[:20]}",
        )
        black_id = uuid4()
        black_iban = f"UA444{uuid4().hex[:20]}"
        await setup_conn.execute(
            """
            INSERT INTO accounts (id, user_id, source, type, currency_code, iban)
            VALUES ($1, $2, 'monobank', 'black', 'UAH', $3)
            """,
            black_id,
            uid,
            black_iban,
        )
        partner_id = uuid4()
        await setup_conn.execute(
            """
            INSERT INTO transactions (
                id, source_id, user_id, account_id, time, amount_cents,
                operation_amount_cents, currency_code, operation_currency_code,
                mcc, cashback_amount_cents, hold, direction,
                source, origin
            ) VALUES (
                $1, $2, $3, $4, $5, $6,
                $7, 'UAH', 'UAH',
                $8, 0, false, 'income',
                'monobank', 'bank'
            )
            """,
            partner_id,
            f"src-{partner_id}",
            uid,
            black_id,
            T + timedelta(seconds=1),
            5000,
            5000,
            MCC_TRANSFER,
        )
        # A pre-inserted FOP expense — used as the "incoming" tx in claim tests
        # so that related_transaction_id FK is satisfied at commit time.
        incoming_id = uuid4()
        await setup_conn.execute(
            """
            INSERT INTO transactions (
                id, source_id, user_id, account_id, time, amount_cents,
                operation_amount_cents, currency_code, operation_currency_code,
                mcc, cashback_amount_cents, hold, direction,
                source, origin
            ) VALUES (
                $1, $2, $3, $4, $5, $6,
                $7, 'UAH', 'UAH',
                $8, 0, false, 'expense',
                'monobank', 'bank'
            )
            """,
            incoming_id,
            f"src-{incoming_id}",
            uid,
            fop_id,
            T,
            5000,
            5000,
            MCC_TRANSFER,
        )
        return (
            str(uid),
            str(fop_id),
            str(black_id),
            str(partner_id),
            black_iban,
            str(incoming_id),
        )


async def _teardown_committed_data(pool, user_id: str) -> None:
    """Delete all data for the given user (committed)."""
    import uuid

    uid = uuid.UUID(user_id)
    async with pool.acquire() as teardown_conn:
        await teardown_conn.execute(
            "DELETE FROM users WHERE id = $1",
            uid,
        )


# ---------------------------------------------------------------------------
# §17 FOR UPDATE SKIP LOCKED behavior (cases 221–223)
# ---------------------------------------------------------------------------


async def test_221_single_connection_claims_partner_successfully(db_pool):
    """Case 221: single connection claims partner via find_universal_candidates."""
    import uuid

    uid_str, _, black_str, partner_str, _, incoming_str = await _setup_committed_data(
        db_pool
    )
    uid = uuid.UUID(uid_str)
    black_id = uuid.UUID(black_str)
    partner_id = uuid.UUID(partner_str)
    incoming_id = uuid.UUID(incoming_str)

    try:
        async with db_pool.acquire() as conn1:
            tx1 = conn1.transaction()
            await tx1.start()

            rows = await conn1.fetch(
                """
                SELECT id
                FROM transactions
                WHERE user_id = $1
                  AND account_id = $2
                  AND direction = 'income'
                  AND mcc = '4829'
                  AND related_transaction_id IS NULL
                  AND time BETWEEN $3::timestamptz - (2 * INTERVAL '1 second')
                              AND $3::timestamptz + (2 * INTERVAL '1 second')
                FOR UPDATE SKIP LOCKED
                """,
                uid,
                black_id,
                T,
            )

            assert len(rows) == 1
            assert rows[0]["id"] == partner_id

            await conn1.execute(
                """
                UPDATE transactions
                SET related_transaction_id = $1,
                    special_category = 'transfer'
                WHERE id = $2
                """,
                incoming_id,
                partner_id,
            )
            await tx1.commit()

        async with db_pool.acquire() as verify_conn:
            row = await verify_conn.fetchrow(
                """
                SELECT special_category, related_transaction_id
                FROM transactions
                WHERE id = $1
                """,
                partner_id,
            )
            assert str(row["special_category"]) == "transfer"
            assert row["related_transaction_id"] == incoming_id

    finally:
        await _teardown_committed_data(db_pool, uid_str)


async def test_222_two_connections_race_one_wins_one_gets_empty(db_pool):
    """Case 222: two connections race for the same partner row.

    Conn1 holds FOR UPDATE lock without committing.
    Conn2 issues same query with SKIP LOCKED → gets empty result (row is locked).
    Conn1 commits. Conn2's candidate is gone (already claimed).
    """
    import uuid

    uid_str, _, black_str, partner_str, _, incoming_str = await _setup_committed_data(
        db_pool
    )
    uid = uuid.UUID(uid_str)
    black_id = uuid.UUID(black_str)
    partner_id = uuid.UUID(partner_str)
    incoming_id = uuid.UUID(incoming_str)

    try:
        async with db_pool.acquire() as conn1, db_pool.acquire() as conn2:
            tx1 = conn1.transaction()
            await tx1.start()

            # Conn1: lock the partner row.
            rows1 = await conn1.fetch(
                """
                SELECT id
                FROM transactions
                WHERE user_id = $1
                  AND account_id = $2
                  AND direction = 'income'
                  AND mcc = '4829'
                  AND related_transaction_id IS NULL
                  AND time BETWEEN $3::timestamptz - (2 * INTERVAL '1 second')
                              AND $3::timestamptz + (2 * INTERVAL '1 second')
                FOR UPDATE SKIP LOCKED
                """,
                uid,
                black_id,
                T,
            )
            assert len(rows1) == 1
            assert rows1[0]["id"] == partner_id

            # Conn2: same query with SKIP LOCKED → row locked by conn1 → empty.
            tx2 = conn2.transaction()
            await tx2.start()
            rows2 = await conn2.fetch(
                """
                SELECT id
                FROM transactions
                WHERE user_id = $1
                  AND account_id = $2
                  AND direction = 'income'
                  AND mcc = '4829'
                  AND related_transaction_id IS NULL
                  AND time BETWEEN $3::timestamptz - (2 * INTERVAL '1 second')
                              AND $3::timestamptz + (2 * INTERVAL '1 second')
                FOR UPDATE SKIP LOCKED
                """,
                uid,
                black_id,
                T,
            )
            assert len(rows2) == 0, "SKIP LOCKED must return empty when row is locked"
            await tx2.rollback()

            # Conn1 claims and commits.
            await conn1.execute(
                """
                UPDATE transactions
                SET related_transaction_id = $1,
                    special_category = 'transfer'
                WHERE id = $2
                """,
                incoming_id,
                partner_id,
            )
            await tx1.commit()

    finally:
        await _teardown_committed_data(db_pool, uid_str)


async def test_223_locked_row_released_on_rollback_available_to_next_query(db_pool):
    """Case 223: conn1 locks row then rolls back → conn2 can acquire the row."""
    import uuid

    uid_str, _, black_str, partner_str, *_ = await _setup_committed_data(db_pool)
    uid = uuid.UUID(uid_str)
    black_id = uuid.UUID(black_str)
    partner_id = uuid.UUID(partner_str)

    try:
        async with db_pool.acquire() as conn1, db_pool.acquire() as conn2:
            tx1 = conn1.transaction()
            await tx1.start()

            # Conn1: lock row.
            rows1 = await conn1.fetch(
                """
                SELECT id
                FROM transactions
                WHERE user_id = $1
                  AND account_id = $2
                  AND direction = 'income'
                  AND mcc = '4829'
                  AND related_transaction_id IS NULL
                  AND time BETWEEN $3::timestamptz - (2 * INTERVAL '1 second')
                              AND $3::timestamptz + (2 * INTERVAL '1 second')
                FOR UPDATE SKIP LOCKED
                """,
                uid,
                black_id,
                T,
            )
            assert len(rows1) == 1

            # Conn1 rolls back — lock released.
            await tx1.rollback()

            # Conn2: row is now available.
            tx2 = conn2.transaction()
            await tx2.start()
            rows2 = await conn2.fetch(
                """
                SELECT id
                FROM transactions
                WHERE user_id = $1
                  AND account_id = $2
                  AND direction = 'income'
                  AND mcc = '4829'
                  AND related_transaction_id IS NULL
                  AND time BETWEEN $3::timestamptz - (2 * INTERVAL '1 second')
                              AND $3::timestamptz + (2 * INTERVAL '1 second')
                FOR UPDATE SKIP LOCKED
                """,
                uid,
                black_id,
                T,
            )
            assert len(rows2) == 1
            assert rows2[0]["id"] == partner_id
            await tx2.rollback()

    finally:
        await _teardown_committed_data(db_pool, uid_str)


# ---------------------------------------------------------------------------
# §17 Advisory lock interplay (case 224)
# ---------------------------------------------------------------------------


async def test_224_advisory_lock_blocks_concurrent_holder(db_pool):
    """Case 224: pg_advisory_xact_lock — second conn blocked while first holds lock.

    Uses pg_try_advisory_xact_lock() (returns bool) to avoid blocking the test.
    Conn1 acquires the lock. Conn2 tries and gets False (lock not available).
    Conn1 commits → lock released. Conn2 (in new transaction) gets True.
    """
    import hashlib

    user_id = uuid4()
    raw = int(hashlib.md5(f"reprocess:{user_id}".encode()).hexdigest()[:8], 16)
    # Fit into postgres bigint range.
    lock_key = raw % (2**63)

    async with db_pool.acquire() as conn1, db_pool.acquire() as conn2:
        tx1 = conn1.transaction()
        await tx1.start()

        acquired1 = await conn1.fetchval(
            "SELECT pg_try_advisory_xact_lock($1)", lock_key
        )
        assert acquired1 is True

        # Conn2 (in its own transaction) tries the same lock — must fail.
        tx2 = conn2.transaction()
        await tx2.start()
        acquired2 = await conn2.fetchval(
            "SELECT pg_try_advisory_xact_lock($1)", lock_key
        )
        assert acquired2 is False, "Advisory lock must be held exclusively by conn1"
        await tx2.rollback()

        # Conn1 commits → lock released.
        await tx1.commit()

        # Conn2 in a new transaction can now acquire.
        tx3 = conn2.transaction()
        await tx3.start()
        acquired3 = await conn2.fetchval(
            "SELECT pg_try_advisory_xact_lock($1)", lock_key
        )
        assert acquired3 is True
        await tx3.rollback()
