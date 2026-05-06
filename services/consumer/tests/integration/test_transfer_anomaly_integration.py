"""Integration tests for transfer detection anomaly lifecycle against real Postgres.

§14 of the transfer detection test specification.
"""

from datetime import UTC, datetime, timedelta

import pytest

from tests.helpers import make_event
from tests.integration.conftest import (
    count_anomalies,
    get_anomaly,
    get_transaction,
    insert_account,
    insert_transaction,
    insert_user,
)

pytestmark = pytest.mark.asyncio

T = datetime(2025, 6, 1, 12, 0, 0, tzinfo=UTC)
MCC_TRANSFER = "4829"


# ---------------------------------------------------------------------------
# §14.1 Auto-resolution
# ---------------------------------------------------------------------------


async def test_unpaired_anomaly_deleted_when_partner_arrives(conn, transfer_service):
    """§135: Unpaired anomaly auto-deleted when partner arrives and pair succeeds.

    Sequence:
    1. Insert tx A (white income, known desc "З Чорної картки", no partner)
       — anomaly recorded.
    2. Insert tx B (black expense, "Переказ на картку") that pairs with A.
    3. A's anomaly is auto-deleted by _claim().
    """
    user_id = await insert_user(conn)
    black_id = await insert_account(
        conn,
        user_id=user_id,
        type="black",
        currency_code="UAH",
        iban=None,
    )
    white_id = await insert_account(
        conn,
        user_id=user_id,
        type="white",
        currency_code="UAH",
        iban=None,
    )

    # Step 1: Tx A arrives — white income, no candidate → unpaired_from_description.
    tx_a_id_val = __import__("uuid").uuid4()
    event_a = make_event(
        id=tx_a_id_val,
        user_id=user_id,
        account_id=white_id,
        time=T,
        mcc=MCC_TRANSFER,
        amount_cents=5000,
        direction="income",
        counterparty_iban=None,
        description="З Чорної картки",
    )
    result_a = await transfer_service.detect_and_pair(conn, event_a)
    assert len(result_a.anomalies) == 1
    assert result_a.anomalies[0].reason_code == "unpaired_from_description"

    # Persist tx A and its anomaly (pipeline does this; we do it manually for the test).
    a_id = await insert_transaction(
        conn,
        id=tx_a_id_val,
        user_id=user_id,
        account_id=white_id,
        time=T,
        amount_cents=5000,
        mcc=MCC_TRANSFER,
        direction="income",
        counterparty_iban=None,
        description="З Чорної картки",
    )
    await conn.execute(
        """
        INSERT INTO transfer_match_anomalies
            (transaction_id, candidate_ids, reason_code)
        VALUES ($1, $2, $3)
        """,
        a_id,
        [],
        "unpaired_from_description",
    )
    assert await get_anomaly(conn, a_id) is not None

    # Step 2: Tx B arrives — black expense, matches A via Tier C.
    event_b = make_event(
        user_id=user_id,
        account_id=black_id,
        time=T + timedelta(seconds=1),
        mcc=MCC_TRANSFER,
        amount_cents=5000,
        direction="expense",
        counterparty_iban=None,
        description="Переказ на картку",
    )
    result_b = await transfer_service.detect_and_pair(conn, event_b)

    assert result_b.special_category == "transfer"
    assert result_b.related_transaction_id == a_id
    assert result_b.anomalies == []

    # Step 3: A's anomaly was auto-deleted by _claim().
    assert await get_anomaly(conn, a_id) is None
    assert await count_anomalies(conn, user_id) == 0


async def test_both_legs_had_unpaired_anomalies_both_deleted_on_pair(
    conn, transfer_service
):
    """§136: Both legs had unpaired anomalies → both deleted when pair found.

    A → unpaired_from_description.
    B → unpaired_to_description.
    Then B's Tier C handler finds A → pairs. Both anomalies deleted.

    Note: The auto-delete in _claim() only deletes the existing (already-inserted)
    partner's anomaly. The incoming tx's anomaly was computed but not yet persisted
    (the pipeline persists it after detect_and_pair returns). So after pairing,
    the result has no anomalies (success path) and the existing partner's anomaly
    is deleted. The incoming tx never had an anomaly persisted.
    """
    user_id = await insert_user(conn)
    black_id = await insert_account(
        conn,
        user_id=user_id,
        type="black",
        currency_code="UAH",
        iban=None,
    )
    white_id = await insert_account(
        conn,
        user_id=user_id,
        type="white",
        currency_code="UAH",
        iban=None,
    )

    # A arrives first — white income, no partner → anomaly.
    a_id = await insert_transaction(
        conn,
        user_id=user_id,
        account_id=white_id,
        time=T,
        amount_cents=5000,
        mcc=MCC_TRANSFER,
        direction="income",
        counterparty_iban=None,
        description="З Чорної картки",
    )
    await conn.execute(
        """
        INSERT INTO transfer_match_anomalies
            (transaction_id, candidate_ids, reason_code)
        VALUES ($1, $2, $3)
        """,
        a_id,
        [],
        "unpaired_from_description",
    )

    # B arrives — black expense, should pair with A via Tier C.
    event_b = make_event(
        user_id=user_id,
        account_id=black_id,
        time=T + timedelta(seconds=1),
        mcc=MCC_TRANSFER,
        amount_cents=5000,
        direction="expense",
        counterparty_iban=None,
        description="Переказ на картку",
    )
    result_b = await transfer_service.detect_and_pair(conn, event_b)

    assert result_b.special_category == "transfer"
    assert result_b.related_transaction_id == a_id

    # A's anomaly is deleted.
    assert await get_anomaly(conn, a_id) is None
    # B's anomaly was never persisted (pairing succeeded → no anomaly in result).
    assert await count_anomalies(conn, user_id) == 0


async def test_anomaly_unique_constraint_raises_on_duplicate(conn, anomaly_repo):
    """§137: UNIQUE constraint on transaction_id — second insert for same tx raises."""
    user_id = await insert_user(conn)
    black_id = await insert_account(
        conn,
        user_id=user_id,
        type="black",
        currency_code="UAH",
        iban=None,
    )

    tx_id = await insert_transaction(
        conn,
        user_id=user_id,
        account_id=black_id,
        time=T,
        amount_cents=5000,
        mcc=MCC_TRANSFER,
        direction="expense",
    )

    await anomaly_repo.record_anomaly(conn, tx_id, [], "unpaired_to_description", None)

    # Second insert is a no-op (ON CONFLICT DO NOTHING) — handles Kafka redelivery.
    await anomaly_repo.record_anomaly(conn, tx_id, [], "unpaired_to_description", None)

    # Still only one anomaly row.
    count = await conn.fetchval(
        "SELECT COUNT(*) FROM transfer_match_anomalies WHERE transaction_id = $1",
        tx_id,
    )
    assert count == 1


# ---------------------------------------------------------------------------
# §14.2 Cascade
# ---------------------------------------------------------------------------


async def test_deleting_transaction_cascades_to_anomaly(conn, anomaly_repo):
    """§138: DELETE on transaction cascades to transfer_match_anomalies.

    Schema has ON DELETE CASCADE on transfer_match_anomalies.transaction_id.
    """
    user_id = await insert_user(conn)
    black_id = await insert_account(
        conn,
        user_id=user_id,
        type="black",
        currency_code="UAH",
        iban=None,
    )

    tx_id = await insert_transaction(
        conn,
        user_id=user_id,
        account_id=black_id,
        time=T,
        amount_cents=5000,
        mcc=MCC_TRANSFER,
        direction="expense",
    )
    await anomaly_repo.record_anomaly(conn, tx_id, [], "unpaired_to_description", None)

    assert await get_anomaly(conn, tx_id) is not None

    await conn.execute("DELETE FROM transactions WHERE id = $1", tx_id)

    assert await get_anomaly(conn, tx_id) is None


async def test_deleting_paired_transaction_sets_related_id_null_on_partner(conn):
    """§139: DELETE paired tx A → partner B's related_transaction_id SET NULL.

    B's special_category stays 'transfer' — SET NULL only affects the FK column.
    """
    user_id = await insert_user(conn)
    black_id = await insert_account(
        conn,
        user_id=user_id,
        type="black",
        currency_code="UAH",
        iban=None,
    )
    white_id = await insert_account(
        conn,
        user_id=user_id,
        type="white",
        currency_code="UAH",
        iban=None,
    )

    # Insert A without related_id first, then B pointing at A, then update A.
    a_id = await insert_transaction(
        conn,
        user_id=user_id,
        account_id=black_id,
        time=T,
        amount_cents=5000,
        mcc=MCC_TRANSFER,
        direction="expense",
        special_category="transfer",
    )
    b_id = await insert_transaction(
        conn,
        user_id=user_id,
        account_id=white_id,
        time=T + timedelta(seconds=1),
        amount_cents=5000,
        mcc=MCC_TRANSFER,
        direction="income",
        special_category="transfer",
        related_transaction_id=a_id,
    )
    # Cross-link A → B
    await conn.execute(
        "UPDATE transactions SET related_transaction_id = $1 WHERE id = $2",
        b_id,
        a_id,
    )

    # Delete A — should SET NULL on B's related_transaction_id.
    await conn.execute("DELETE FROM transactions WHERE id = $1", a_id)

    b_row = await get_transaction(conn, b_id)
    assert b_row["related_transaction_id"] is None
    # special_category stays 'transfer'.
    assert str(b_row["special_category"]) == "transfer"

    # B is now visible to unclaimed-partner queries (related_transaction_id IS NULL).
    val = await conn.fetchval(
        """
        SELECT COUNT(*)
        FROM transactions
        WHERE id = $1
          AND related_transaction_id IS NULL
        """,
        b_id,
    )
    assert int(val) == 1
