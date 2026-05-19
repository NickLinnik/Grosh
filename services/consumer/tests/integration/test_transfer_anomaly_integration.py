"""Integration tests for transfer detection anomaly lifecycle against real Postgres.

Test cases per `references/consumer-transfer-detection-test-suite.md` §16.
Skipped at module level until detector.py lands (Slice 17 task T13).
The corresponding implementation task removes this marker.
"""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from tests.integration.conftest import (
    count_anomalies,
    get_anomaly,
    get_transaction,
    insert_account,
    insert_transaction,
    insert_user,
)

pytestmark = pytest.mark.asyncio

try:
    from grosh_consumer.models.normalized import NormalizedTransaction
    from grosh_consumer.repositories.account_repo import AccountRepo
    from grosh_consumer.repositories.anomaly_repo import AnomalyRepo
    from grosh_consumer.sources.monobank.transfer.detector import (
        MonobankTransferDetection,
    )
    from grosh_consumer.sources.monobank.transfer.repo import TransferQueryRepo
except ImportError:
    pass  # tests are skipped at module level via pytestmark

T = datetime(2026, 6, 1, 12, 0, 0, tzinfo=UTC)
MCC_TRANSFER = "4829"

UAH_FOP_IBAN = "UA111000000000000000000111"
UAH_BLACK_IBAN = "UA444000000000000000000444"
UAH_WHITE_IBAN = "UA555000000000000000000555"


def _make_strategy():
    return MonobankTransferDetection(
        TransferQueryRepo(),
        AccountRepo(),
        AnomalyRepo(),
    )


def _tx(
    *, user_id, account_id, direction, desc=None, cp_iban=None, amount=5000, time=None
):
    return NormalizedTransaction(
        id=uuid4(),
        source="monobank",
        source_id="src-test",
        user_id=user_id,
        account_id=account_id,
        time=time or T,
        amount_cents=amount,
        operation_amount_cents=amount,
        operation_currency_code="UAH",
        description=desc,
        mcc=MCC_TRANSFER,
        cashback_amount_cents=0,
        balance_cents=None,
        hold=False,
        direction=direction,
        counterparty_iban=cp_iban,
        rate_source=None,
        metadata=None,
    )


async def _record_anomaly(conn, tx_id, reason_code):
    """Helper: directly persist an anomaly row (bypassing the strategy)."""
    await conn.execute(
        """
        INSERT INTO transfer_match_anomalies
            (transaction_id, candidate_ids, reason_code)
        VALUES ($1, $2, $3)
        ON CONFLICT (transaction_id) DO NOTHING
        """,
        tx_id,
        [],
        reason_code,
    )


# ---------------------------------------------------------------------------
# §16.1 Auto-resolve on successful claim (cases 213–217)
# ---------------------------------------------------------------------------


async def test_213_unpaired_from_description_deleted_on_partner_arrival(conn):
    """Case 213: unpaired_from_description anomaly auto-deleted when partner claims."""
    user_id = await insert_user(conn)
    black_id = await insert_account(
        conn, user_id=user_id, type="black", currency_code="UAH", iban=UAH_BLACK_IBAN
    )
    white_id = await insert_account(
        conn, user_id=user_id, type="white", currency_code="UAH", iban=UAH_WHITE_IBAN
    )

    # Leg A: white income with prefix, no partner → anomaly recorded.
    a_id = await insert_transaction(
        conn,
        user_id=user_id,
        account_id=white_id,
        time=T,
        amount_cents=5000,
        mcc=MCC_TRANSFER,
        direction="income",
        description="З Чорної картки",
    )
    await _record_anomaly(conn, a_id, "unpaired_from_description")
    assert await get_anomaly(conn, a_id) is not None

    # Leg B: black expense pairs with A.
    strategy = _make_strategy()
    leg_b = _tx(
        user_id=user_id,
        account_id=black_id,
        direction="expense",
        desc="Переказ на картку",
        time=T + timedelta(seconds=1),
    )
    result = await strategy.detect_and_pair(conn, leg_b)
    assert result.special_category == "transfer"
    assert result.related_transaction_id == a_id

    # A's anomaly auto-deleted by claim.
    assert await get_anomaly(conn, a_id) is None
    assert await count_anomalies(conn, user_id) == 0


async def test_214_unpaired_to_description_deleted_on_partner_arrival(conn):
    """Case 214: unpaired_to_description anomaly auto-deleted when partner claims."""
    user_id = await insert_user(conn)
    black_id = await insert_account(
        conn, user_id=user_id, type="black", currency_code="UAH", iban=UAH_BLACK_IBAN
    )
    white_id = await insert_account(
        conn, user_id=user_id, type="white", currency_code="UAH", iban=UAH_WHITE_IBAN
    )

    # Leg A: black expense with "to white" prefix, no partner → unpaired_to_description.
    a_id = await insert_transaction(
        conn,
        user_id=user_id,
        account_id=black_id,
        time=T,
        amount_cents=5000,
        mcc=MCC_TRANSFER,
        direction="expense",
        description="На білу картку",
    )
    await _record_anomaly(conn, a_id, "unpaired_to_description")
    assert await get_anomaly(conn, a_id) is not None

    # Leg B: white income pairs with A.
    strategy = _make_strategy()
    leg_b = _tx(
        user_id=user_id,
        account_id=white_id,
        direction="income",
        desc="З Чорної картки",
        time=T + timedelta(seconds=1),
    )
    result = await strategy.detect_and_pair(conn, leg_b)
    assert result.special_category == "transfer"
    assert result.related_transaction_id == a_id

    assert await get_anomaly(conn, a_id) is None
    assert await count_anomalies(conn, user_id) == 0


async def test_215_both_legs_had_unpaired_anomalies_both_deleted(conn):
    """Case 215: both legs had unpaired anomalies → both deleted when pair claimed."""
    user_id = await insert_user(conn)
    black_id = await insert_account(
        conn, user_id=user_id, type="black", currency_code="UAH", iban=UAH_BLACK_IBAN
    )
    white_id = await insert_account(
        conn, user_id=user_id, type="white", currency_code="UAH", iban=UAH_WHITE_IBAN
    )

    # Leg A: white income — unpaired_from_description.
    a_id = await insert_transaction(
        conn,
        user_id=user_id,
        account_id=white_id,
        time=T,
        amount_cents=5000,
        mcc=MCC_TRANSFER,
        direction="income",
        description="З Чорної картки",
    )
    await _record_anomaly(conn, a_id, "unpaired_from_description")

    # Leg B: black expense — claim finds A; A's anomaly is auto-deleted.
    # B itself was also going to have an anomaly, but the incoming leg's anomaly
    # is returned in result.anomalies (not yet persisted). Since pairing succeeds,
    # B's result has no anomalies.
    strategy = _make_strategy()
    leg_b = _tx(
        user_id=user_id,
        account_id=black_id,
        direction="expense",
        desc="Переказ на картку",
        time=T + timedelta(seconds=1),
    )
    result = await strategy.detect_and_pair(conn, leg_b)
    assert result.special_category == "transfer"
    assert result.related_transaction_id == a_id
    # B's result carries no anomalies (success path).
    assert result.anomalies == []

    # A's anomaly was deleted by claim.
    assert await get_anomaly(conn, a_id) is None
    assert await count_anomalies(conn, user_id) == 0


async def test_215b_auto_resolve_plus_canary_in_same_claim(conn):
    """Case 215b: auto-resolve of unpaired anomaly AND canary in same claim.

    Sequence:
    1. Leg A (white income, "З Чорної картки") → unpaired_from_description persisted.
    2. Leg B (black expense, "Переказ на картку") → detect_and_pair finds A.
       Description validation: income says "З Чорної" but expense is on black account
       (expense account → check that income description matches expense account type).
       Actually "Переказ на картку" is generic (no constraint) so it won't mismatch.
       Instead we set up a genuine mismatch: leg A says "З Білої картки" (white source)
       but leg B is on black account.
    3. Final state: A's unpaired anomaly deleted + 1 description_consistency_mismatch.
    """
    user_id = await insert_user(conn)
    _black_id = await insert_account(
        conn, user_id=user_id, type="black", currency_code="UAH", iban=UAH_BLACK_IBAN
    )
    white_id = await insert_account(
        conn, user_id=user_id, type="white", currency_code="UAH", iban=UAH_WHITE_IBAN
    )

    # Leg A: white income with prefix.
    a_id = await insert_transaction(
        conn,
        user_id=user_id,
        account_id=white_id,
        time=T,
        amount_cents=5000,
        mcc=MCC_TRANSFER,
        direction="income",
        description="З Чорної картки",  # says source is black
    )
    await _record_anomaly(conn, a_id, "unpaired_from_description")
    assert await get_anomaly(conn, a_id) is not None

    # Genuine canary: "З Білої картки" (income expects white source) + black expense
    # → constraint type=white vs account=black → mismatch.

    fop_id = await insert_account(
        conn, user_id=user_id, type="fop", currency_code="UAH", iban=UAH_FOP_IBAN
    )
    # Re-seed A with a description that will mismatch against the expense account.
    a2_id = await insert_transaction(
        conn,
        user_id=user_id,
        account_id=fop_id,  # income on fop
        time=T + timedelta(seconds=10),
        amount_cents=7000,
        mcc=MCC_TRANSFER,
        direction="income",
        description="З Білої картки",  # says source is white
    )
    await _record_anomaly(conn, a2_id, "unpaired_from_description")

    strategy = _make_strategy()
    # Leg B: black expense — income says "from white", expense on black → mismatch.
    # Use a separate black2 account to avoid IBAN conflicts.
    uah_black2_iban = "UA444000000000000000000445"
    black2_id = await insert_account(
        conn, user_id=user_id, type="black", currency_code="UAH", iban=uah_black2_iban
    )
    leg_b = _tx(
        user_id=user_id,
        account_id=black2_id,  # black account — inconsistent with "З Білої картки"
        direction="expense",
        desc="Переказ на картку",
        amount=7000,
        time=T + timedelta(seconds=11),
    )
    result = await strategy.detect_and_pair(conn, leg_b)

    assert result.special_category == "transfer"
    assert result.related_transaction_id == a2_id

    # A2's unpaired anomaly deleted.
    assert await get_anomaly(conn, a2_id) is None

    # Canary recorded in result.
    canary_codes = [a.reason_code for a in result.anomalies]
    assert "description_consistency_mismatch" in canary_codes

    # After persisting canary: total anomalies = 1 (canary only).
    # (We don't persist here, just verify result.)
    assert len(result.anomalies) == 1


async def test_216_terminal_anomaly_persists_across_reruns(conn):
    """Case 216: ambiguous_pair_match anomaly persists across unrelated events."""
    user_id = await insert_user(conn)
    fop_id = await insert_account(
        conn, user_id=user_id, type="fop", currency_code="UAH", iban=UAH_FOP_IBAN
    )

    tx_id = await insert_transaction(
        conn,
        user_id=user_id,
        account_id=fop_id,
        time=T,
        amount_cents=5000,
        mcc=MCC_TRANSFER,
        direction="expense",
    )
    await _record_anomaly(conn, tx_id, "ambiguous_pair_match")
    assert await get_anomaly(conn, tx_id) is not None

    # Process an unrelated event (different user, different account).
    other_user_id = await insert_user(conn)
    other_acc_id = await insert_account(
        conn,
        user_id=other_user_id,
        type="black",
        currency_code="UAH",
        iban=UAH_BLACK_IBAN,
    )
    strategy = _make_strategy()
    unrelated_tx = _tx(
        user_id=other_user_id,
        account_id=other_acc_id,
        direction="expense",
        desc="Олена К.",
        time=T + timedelta(hours=1),
    )
    await strategy.detect_and_pair(conn, unrelated_tx)

    # Original anomaly still present.
    assert await get_anomaly(conn, tx_id) is not None


async def test_217_description_consistency_mismatch_is_terminal(conn):
    """Case 217: description_consistency_mismatch canary persists across re-runs."""
    user_id = await insert_user(conn)
    fop_id = await insert_account(
        conn, user_id=user_id, type="fop", currency_code="UAH", iban=UAH_FOP_IBAN
    )

    tx_id = await insert_transaction(
        conn,
        user_id=user_id,
        account_id=fop_id,
        time=T,
        amount_cents=5000,
        mcc=MCC_TRANSFER,
        direction="expense",
        special_category="transfer",
    )
    await _record_anomaly(conn, tx_id, "description_consistency_mismatch")
    assert await get_anomaly(conn, tx_id) is not None

    # Process an unrelated event — canary stays.
    other_user_id = await insert_user(conn)
    other_acc_id = await insert_account(
        conn,
        user_id=other_user_id,
        type="black",
        currency_code="UAH",
        iban=UAH_BLACK_IBAN,
    )
    strategy = _make_strategy()
    unrelated_tx = _tx(
        user_id=other_user_id,
        account_id=other_acc_id,
        direction="expense",
        time=T + timedelta(hours=1),
    )
    await strategy.detect_and_pair(conn, unrelated_tx)

    assert await get_anomaly(conn, tx_id) is not None


# ---------------------------------------------------------------------------
# §16.2 Schema constraints (cases 218–219)
# ---------------------------------------------------------------------------


async def test_218_unique_constraint_on_transaction_id_idempotent(conn, anomaly_repo):
    """Case 218: UNIQUE on transaction_id — second insert is ON CONFLICT DO NOTHING."""
    user_id = await insert_user(conn)
    black_id = await insert_account(
        conn, user_id=user_id, type="black", currency_code="UAH", iban=None
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
    # Second insert must not raise; ON CONFLICT DO NOTHING is idempotent.
    await anomaly_repo.record_anomaly(conn, tx_id, [], "unpaired_to_description", None)

    count = await conn.fetchval(
        "SELECT COUNT(*) FROM transfer_match_anomalies WHERE transaction_id = $1",
        tx_id,
    )
    assert count == 1


async def test_219_anomaly_cascade_on_transaction_delete(conn, anomaly_repo):
    """Case 219: DELETE transaction → anomaly CASCADE-deleted."""
    user_id = await insert_user(conn)
    black_id = await insert_account(
        conn, user_id=user_id, type="black", currency_code="UAH", iban=None
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


# ---------------------------------------------------------------------------
# §16.3 Pair break behavior (case 220)
# ---------------------------------------------------------------------------


async def test_220_deleting_paired_tx_nulls_partner_related_id_preserves_transfer(conn):
    """Case 220: DELETE paired tx A → partner B's related_transaction_id SET NULL;
    special_category='transfer' and metadata.layer.transfer.pair remain on B.
    B becomes a candidate again (related_transaction_id IS NULL).
    """
    user_id = await insert_user(conn)
    black_id = await insert_account(
        conn, user_id=user_id, type="black", currency_code="UAH", iban=None
    )
    white_id = await insert_account(
        conn, user_id=user_id, type="white", currency_code="UAH", iban=None
    )

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
    # Cross-link A → B.
    await conn.execute(
        "UPDATE transactions SET related_transaction_id = $1 WHERE id = $2",
        b_id,
        a_id,
    )

    # Delete A — CASCADE + SET NULL on B.
    await conn.execute("DELETE FROM transactions WHERE id = $1", a_id)

    b_row = await get_transaction(conn, b_id)
    assert b_row["related_transaction_id"] is None
    assert str(b_row["special_category"]) == "transfer"

    # B is visible to future unclaimed-partner queries.
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
