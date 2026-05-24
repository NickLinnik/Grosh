"""Integration tests for directional transitive rule (multi-hop chain).

Test cases per `references/consumer-transfer-detection-test-suite.md` §13.
Skipped at module level until detector.py lands (Slice 17 task T13).
The corresponding implementation task removes this marker.
"""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from tests.integration.conftest import (
    count_anomalies,
    get_transaction,
    insert_account,
    insert_transaction,
    insert_user,
)

pytestmark = pytest.mark.asyncio


try:
    from grosh_shared.domain.normalized import NormalizedTransaction

    from grosh_enrichment.repositories.account_repo import AccountRepo
    from grosh_enrichment.repositories.anomaly_repo import AnomalyRepo
    from grosh_enrichment.sources.monobank.transfer.detector import (
        MonobankTransferDetection,
    )
    from grosh_enrichment.sources.monobank.transfer.repo import TransferQueryRepo
except ImportError:
    pass  # tests are skipped at module level via pytestmark

T = datetime(2026, 6, 1, 12, 0, 0, tzinfo=UTC)
MCC_TRANSFER = "4829"

UAH_FOP_IBAN = "UA111000000000000000000111"
USD_FOP_IBAN = "UA222000000000000000000222"
EUR_FOP_IBAN = "UA333000000000000000000333"
UAH_BLACK_IBAN = "UA444000000000000000000444"
UAH_WHITE_IBAN = "UA555000000000000000000555"


def _make_strategy():
    return MonobankTransferDetection(
        TransferQueryRepo(),
        AccountRepo(),
        AnomalyRepo(),
    )


def _tx(
    *,
    user_id,
    account_id,
    direction,
    time=None,
    amount=5000,
    op_amount=None,
    cp_iban=None,
    desc=None,
    mcc=MCC_TRANSFER,
    currency="UAH",
):
    return NormalizedTransaction(
        id=uuid4(),
        source="monobank",
        source_id="src-test",
        user_id=user_id,
        account_id=account_id,
        time=time or T,
        amount_cents=amount,
        operation_amount_cents=op_amount if op_amount is not None else amount,
        operation_currency_code=currency,
        description=desc,
        mcc=mcc,
        cashback_amount_cents=0,
        balance_cents=None,
        hold=False,
        direction=direction,
        counterparty_iban=cp_iban,
        rate_source=None,
        metadata=None,
    )


# ---------------------------------------------------------------------------
# §13.1 Directional rule on individual rows (cases 198–201)
# ---------------------------------------------------------------------------


async def test_198_multi_hop_expense_cp_iban_status_transitive(conn):
    """Case 198: multi-hop expense with chain-end IBAN → cp_iban_status=transitive."""
    user_id = await insert_user(conn)
    usd_fop_id = await insert_account(
        conn, user_id=user_id, type="fop", currency_code="USD", iban=USD_FOP_IBAN
    )
    # Register the "chain end" account so the lookup could resolve it if not suppressed.
    await insert_account(
        conn, user_id=user_id, type="black", currency_code="UAH", iban=UAH_BLACK_IBAN
    )

    strategy = _make_strategy()
    # Multi-hop expense: cp_iban points at the final destination card (chain-end IBAN).
    tx = _tx(
        user_id=user_id,
        account_id=usd_fop_id,
        direction="expense",
        cp_iban=UAH_BLACK_IBAN,  # chain-end IBAN
        desc="На гривневий рахунок ФОП для переказу на картку",
    )

    result = await strategy.detect_and_pair(conn, tx)
    assert result.metadata_block is not None
    assert result.metadata_block["row"]["cp_iban_status"] == "transitive"


async def test_199_multi_hop_income_cp_iban_status_honest(conn):
    """Case 199: multi-hop income with honest IBAN → cp_iban_status=honest."""
    user_id = await insert_user(conn)
    uah_fop_id = await insert_account(
        conn, user_id=user_id, type="fop", currency_code="UAH", iban=UAH_FOP_IBAN
    )
    # Register the USD FOP account so the IBAN lookup resolves it.
    await insert_account(
        conn, user_id=user_id, type="fop", currency_code="USD", iban=USD_FOP_IBAN
    )

    strategy = _make_strategy()
    # Multi-hop income: cp_iban points at the immediate partner (USD FOP) — honest.
    tx = _tx(
        user_id=user_id,
        account_id=uah_fop_id,
        direction="income",
        cp_iban=USD_FOP_IBAN,  # immediate partner — honest on income side
        desc="З доларового рахунку ФОП для переказу на картку",
    )

    result = await strategy.detect_and_pair(conn, tx)
    assert result.metadata_block is not None
    assert result.metadata_block["row"]["cp_iban_status"] == "honest"


async def test_200_non_multi_hop_expense_honest_iban_not_suppressed(conn):
    """Case 200: non-multi-hop expense with honest IBAN → cp_iban_status=honest."""
    user_id = await insert_user(conn)
    uah_fop_id = await insert_account(
        conn, user_id=user_id, type="fop", currency_code="UAH", iban=UAH_FOP_IBAN
    )
    await insert_account(
        conn, user_id=user_id, type="black", currency_code="UAH", iban=UAH_BLACK_IBAN
    )

    strategy = _make_strategy()
    tx = _tx(
        user_id=user_id,
        account_id=uah_fop_id,
        direction="expense",
        cp_iban=UAH_BLACK_IBAN,
        desc="На чорну картку",  # NOT a multi-hop description
    )

    result = await strategy.detect_and_pair(conn, tx)
    assert result.metadata_block is not None
    assert result.metadata_block["row"]["cp_iban_status"] == "honest"


async def test_201_non_multi_hop_income_honest_iban(conn):
    """Case 201: non-multi-hop income with honest IBAN → cp_iban_status=honest."""
    user_id = await insert_user(conn)
    uah_black_id = await insert_account(
        conn, user_id=user_id, type="black", currency_code="UAH", iban=UAH_BLACK_IBAN
    )
    await insert_account(
        conn, user_id=user_id, type="fop", currency_code="UAH", iban=UAH_FOP_IBAN
    )

    strategy = _make_strategy()
    tx = _tx(
        user_id=user_id,
        account_id=uah_black_id,
        direction="income",
        cp_iban=UAH_FOP_IBAN,
        desc="З гривневого рахунку ФОП",
    )

    result = await strategy.detect_and_pair(conn, tx)
    assert result.metadata_block is not None
    assert result.metadata_block["row"]["cp_iban_status"] == "honest"


# ---------------------------------------------------------------------------
# §13.2 4-leg multi-hop chain (cases 202–204)
# ---------------------------------------------------------------------------
#
# Chain: USD FOP expense (leg1) → UAH FOP income (leg2) [pair 1]
#        UAH FOP expense (leg3) → UAH card income (leg4) [pair 2]
#
# Leg1 cp_iban = UAH_BLACK_IBAN (chain-end, transitive)
# Leg2 cp_iban = USD_FOP_IBAN (honest, immediate partner)
# Leg3 cp_iban = UAH_BLACK_IBAN (honest, direct)
# Leg4 cp_iban = UAH_FOP_IBAN (honest, direct)
#
# Amounts: legs 1+2 use 50000 USD / 2190000 UAH asymmetric pair.
#          legs 3+4 use 5000 UAH direct.


async def _build_chain_accounts(conn, user_id):
    """Create all four accounts for the 4-leg chain test."""
    usd_fop_id = await insert_account(
        conn, user_id=user_id, type="fop", currency_code="USD", iban=USD_FOP_IBAN
    )
    uah_fop_id = await insert_account(
        conn, user_id=user_id, type="fop", currency_code="UAH", iban=UAH_FOP_IBAN
    )
    black_id = await insert_account(
        conn, user_id=user_id, type="black", currency_code="UAH", iban=UAH_BLACK_IBAN
    )
    return usd_fop_id, uah_fop_id, black_id


async def test_202_process_4_leg_chain_in_order(conn):
    """Case 202: process 4-leg chain in order 1,2,3,4 → two distinct pairs claimed."""
    user_id = await insert_user(conn)
    usd_fop_id, uah_fop_id, black_id = await _build_chain_accounts(conn, user_id)
    strategy = _make_strategy()

    # Leg 1: USD FOP expense (multi-hop, chain-end IBAN points at black card)
    leg1 = _tx(
        user_id=user_id,
        account_id=usd_fop_id,
        direction="expense",
        time=T,
        amount=50000,
        op_amount=2190000,
        currency="UAH",
        cp_iban=UAH_BLACK_IBAN,  # chain-end → transitive
        desc="На гривневий рахунок ФОП для переказу на картку",
    )
    res1 = await strategy.detect_and_pair(conn, leg1)
    # Leg2 not yet in DB → no pair yet.
    assert res1.special_category is None
    _leg1_id = await insert_transaction(
        conn,
        id=leg1.id,
        user_id=user_id,
        account_id=usd_fop_id,
        time=T,
        amount_cents=50000,
        operation_amount_cents=2190000,
        mcc=MCC_TRANSFER,
        direction="expense",
        counterparty_iban=UAH_BLACK_IBAN,
        description=leg1.description,
        currency_code="USD",
    )

    # Leg 2: UAH FOP income (multi-hop income, honest IBAN → USD FOP)
    leg2 = _tx(
        user_id=user_id,
        account_id=uah_fop_id,
        direction="income",
        time=T + timedelta(seconds=1),
        amount=2190000,
        op_amount=2190000,
        currency="UAH",
        cp_iban=USD_FOP_IBAN,  # immediate partner → honest
        desc="З доларового рахунку ФОП для переказу на картку",
    )
    res2 = await strategy.detect_and_pair(conn, leg2)
    # Should pair with leg1.
    assert res2.special_category == "transfer"
    assert res2.related_transaction_id == leg1.id
    assert (
        res2.metadata_block["pair"]["iban_evidence"] == "unilateral"
    )  # income honest, expense transitive
    _leg2_id = await insert_transaction(
        conn,
        id=leg2.id,
        user_id=user_id,
        account_id=uah_fop_id,
        time=T + timedelta(seconds=1),
        amount_cents=2190000,
        mcc=MCC_TRANSFER,
        direction="income",
        counterparty_iban=USD_FOP_IBAN,
        description=leg2.description,
        # Mirror what the orchestrator would persist from TransferResult:
        related_transaction_id=res2.related_transaction_id,
        special_category=res2.special_category,
    )

    # Leg 3: UAH FOP expense (direct, honest IBAN → black card)
    leg3 = _tx(
        user_id=user_id,
        account_id=uah_fop_id,
        direction="expense",
        time=T + timedelta(seconds=2),
        amount=5000,
        cp_iban=UAH_BLACK_IBAN,
        desc="На чорну картку",
    )
    res3 = await strategy.detect_and_pair(conn, leg3)
    assert res3.special_category is None  # leg4 not yet in DB
    _leg3_id = await insert_transaction(
        conn,
        id=leg3.id,
        user_id=user_id,
        account_id=uah_fop_id,
        time=T + timedelta(seconds=2),
        amount_cents=5000,
        mcc=MCC_TRANSFER,
        direction="expense",
        counterparty_iban=UAH_BLACK_IBAN,
        description=leg3.description,
    )

    # Leg 4: UAH card income (direct, honest IBAN → UAH FOP)
    leg4 = _tx(
        user_id=user_id,
        account_id=black_id,
        direction="income",
        time=T + timedelta(seconds=2),
        amount=5000,
        cp_iban=UAH_FOP_IBAN,
        desc="З гривневого рахунку ФОП",
    )
    res4 = await strategy.detect_and_pair(conn, leg4)
    assert res4.special_category == "transfer"
    assert res4.related_transaction_id == leg3.id
    # Insert leg4 with the orchestrator-equivalent state from res4.
    await insert_transaction(
        conn,
        id=leg4.id,
        user_id=user_id,
        account_id=black_id,
        time=T + timedelta(seconds=2),
        amount_cents=5000,
        mcc=MCC_TRANSFER,
        direction="income",
        counterparty_iban=UAH_FOP_IBAN,
        description=leg4.description,
        related_transaction_id=res4.related_transaction_id,
        special_category=res4.special_category,
    )

    # Final state: all 4 in DB, 2 distinct pairs.
    leg1_row = await get_transaction(conn, leg1.id)
    leg2_row = await get_transaction(conn, leg2.id)
    assert str(leg1_row["special_category"]) == "transfer"
    assert str(leg2_row["special_category"]) == "transfer"
    assert leg1_row["related_transaction_id"] == leg2.id
    assert leg2_row["related_transaction_id"] == leg1.id

    # Verify two distinct pairs.
    pair1 = {leg1.id, leg2.id}
    pair2 = {leg3.id, leg4.id}
    assert pair1 != pair2


async def test_203_process_4_leg_chain_reverse_order(conn):
    """Case 203: reverse arrival (4,3,2,1) → both pairs eventually claimed.

    Mirrors the orchestrator pattern (detect_and_pair → INSERT row with
    special_category/related_transaction_id from result) — earlier arrivals
    record unpaired_*_description anomalies; later arrivals find them and the
    auto-resolve sweep clears the anomalies on the successful claim.
    """
    user_id = await insert_user(conn)
    usd_fop_id, uah_fop_id, black_id = await _build_chain_accounts(conn, user_id)
    strategy = _make_strategy()
    anomaly_repo = AnomalyRepo()

    # Leg 4 arrives first: no partner yet → unpaired_from_description anomaly.
    leg4 = _tx(
        user_id=user_id,
        account_id=black_id,
        direction="income",
        time=T + timedelta(seconds=2),
        amount=5000,
        cp_iban=UAH_FOP_IBAN,
        desc="З гривневого рахунку ФОП",
    )
    res4_first = await strategy.detect_and_pair(conn, leg4)
    assert res4_first.special_category is None
    await insert_transaction(
        conn,
        id=leg4.id,
        user_id=user_id,
        account_id=black_id,
        time=T + timedelta(seconds=2),
        amount_cents=5000,
        mcc=MCC_TRANSFER,
        direction="income",
        counterparty_iban=UAH_FOP_IBAN,
        description=leg4.description,
    )
    for anom in res4_first.anomalies:
        await anomaly_repo.record_anomaly(
            conn,
            anom.transaction_id,
            anom.candidate_ids,
            anom.reason_code,
            anom.reason_detail,
        )

    # Leg 3 arrives: finds leg 4 → claim pair 3+4.
    leg3 = _tx(
        user_id=user_id,
        account_id=uah_fop_id,
        direction="expense",
        time=T + timedelta(seconds=2),
        amount=5000,
        cp_iban=UAH_BLACK_IBAN,
        desc="На чорну картку",
    )
    res3 = await strategy.detect_and_pair(conn, leg3)
    assert res3.special_category == "transfer"
    assert res3.related_transaction_id == leg4.id
    await insert_transaction(
        conn,
        id=leg3.id,
        user_id=user_id,
        account_id=uah_fop_id,
        time=T + timedelta(seconds=2),
        amount_cents=5000,
        mcc=MCC_TRANSFER,
        direction="expense",
        counterparty_iban=UAH_BLACK_IBAN,
        description=leg3.description,
        special_category="transfer",
        related_transaction_id=leg4.id,
    )

    # Leg 2 arrives: no partner yet → unpaired_to_description anomaly.
    leg2 = _tx(
        user_id=user_id,
        account_id=uah_fop_id,
        direction="income",
        time=T + timedelta(seconds=1),
        amount=2190000,
        op_amount=2190000,
        cp_iban=USD_FOP_IBAN,
        desc="З доларового рахунку ФОП для переказу на картку",
    )
    res2_first = await strategy.detect_and_pair(conn, leg2)
    assert res2_first.special_category is None
    await insert_transaction(
        conn,
        id=leg2.id,
        user_id=user_id,
        account_id=uah_fop_id,
        time=T + timedelta(seconds=1),
        amount_cents=2190000,
        operation_amount_cents=2190000,
        mcc=MCC_TRANSFER,
        direction="income",
        counterparty_iban=USD_FOP_IBAN,
        description=leg2.description,
    )
    for anom in res2_first.anomalies:
        await anomaly_repo.record_anomaly(
            conn,
            anom.transaction_id,
            anom.candidate_ids,
            anom.reason_code,
            anom.reason_detail,
        )

    # Leg 1 arrives: finds leg 2 via two-clause amount predicate → claim pair 1+2.
    leg1 = _tx(
        user_id=user_id,
        account_id=usd_fop_id,
        direction="expense",
        time=T,
        amount=50000,
        op_amount=2190000,
        currency="UAH",
        cp_iban=UAH_BLACK_IBAN,
        desc="На гривневий рахунок ФОП для переказу на картку",
    )
    res1 = await strategy.detect_and_pair(conn, leg1)
    assert res1.special_category == "transfer"
    assert res1.related_transaction_id == leg2.id
    await insert_transaction(
        conn,
        id=leg1.id,
        user_id=user_id,
        account_id=usd_fop_id,
        time=T,
        amount_cents=50000,
        operation_amount_cents=2190000,
        mcc=MCC_TRANSFER,
        direction="expense",
        counterparty_iban=UAH_BLACK_IBAN,
        description=leg1.description,
        currency_code="USD",
        special_category="transfer",
        related_transaction_id=leg2.id,
    )

    # Final state: all 4 in DB, two claimed pairs, zero remaining anomalies
    # (auto-resolve cleared the unpaired_*_description records on each claim).
    r1 = await get_transaction(conn, leg1.id)
    r2 = await get_transaction(conn, leg2.id)
    r3 = await get_transaction(conn, leg3.id)
    r4 = await get_transaction(conn, leg4.id)
    assert str(r1["special_category"]) == "transfer"
    assert str(r2["special_category"]) == "transfer"
    assert str(r3["special_category"]) == "transfer"
    assert str(r4["special_category"]) == "transfer"
    assert r1["related_transaction_id"] == leg2.id
    assert r2["related_transaction_id"] == leg1.id
    assert r3["related_transaction_id"] == leg4.id
    assert r4["related_transaction_id"] == leg3.id

    remaining_anomalies = await count_anomalies(conn, user_id)
    assert remaining_anomalies == 0


async def test_204_process_4_leg_chain_out_of_order(conn):
    """Case 204: process legs out of order (2,4,1,3) → 2 pairs claimed, 0 anomalies."""
    user_id = await insert_user(conn)
    usd_fop_id, uah_fop_id, black_id = await _build_chain_accounts(conn, user_id)
    strategy = _make_strategy()
    anomaly_repo = AnomalyRepo()

    # Process leg 2 first: no partner → unpaired_from_description anomaly.
    leg2 = _tx(
        user_id=user_id,
        account_id=uah_fop_id,
        direction="income",
        time=T + timedelta(seconds=1),
        amount=2190000,
        op_amount=2190000,
        cp_iban=USD_FOP_IBAN,
        desc="З доларового рахунку ФОП для переказу на картку",
    )
    res2_first = await strategy.detect_and_pair(conn, leg2)
    assert res2_first.special_category is None
    await insert_transaction(
        conn,
        id=leg2.id,
        user_id=user_id,
        account_id=uah_fop_id,
        time=T + timedelta(seconds=1),
        amount_cents=2190000,
        operation_amount_cents=2190000,
        mcc=MCC_TRANSFER,
        direction="income",
        counterparty_iban=USD_FOP_IBAN,
        description=leg2.description,
    )
    for anom in res2_first.anomalies:
        await anomaly_repo.record_anomaly(
            conn,
            anom.transaction_id,
            anom.candidate_ids,
            anom.reason_code,
            anom.reason_detail,
        )

    # Process leg 4: no partner for pair2 yet.
    leg4 = _tx(
        user_id=user_id,
        account_id=black_id,
        direction="income",
        time=T + timedelta(seconds=2),
        amount=5000,
        cp_iban=UAH_FOP_IBAN,
        desc="З гривневого рахунку ФОП",
    )
    res4_first = await strategy.detect_and_pair(conn, leg4)
    assert res4_first.special_category is None
    await insert_transaction(
        conn,
        id=leg4.id,
        user_id=user_id,
        account_id=black_id,
        time=T + timedelta(seconds=2),
        amount_cents=5000,
        mcc=MCC_TRANSFER,
        direction="income",
        counterparty_iban=UAH_FOP_IBAN,
        description=leg4.description,
    )
    for anom in res4_first.anomalies:
        await anomaly_repo.record_anomaly(
            conn,
            anom.transaction_id,
            anom.candidate_ids,
            anom.reason_code,
            anom.reason_detail,
        )

    # Process leg 1: finds leg 2 as partner (two-clause amount predicate).
    leg1 = _tx(
        user_id=user_id,
        account_id=usd_fop_id,
        direction="expense",
        time=T,
        amount=50000,
        op_amount=2190000,
        currency="UAH",
        cp_iban=UAH_BLACK_IBAN,
        desc="На гривневий рахунок ФОП для переказу на картку",
    )
    res1 = await strategy.detect_and_pair(conn, leg1)
    assert res1.special_category == "transfer"
    assert res1.related_transaction_id == leg2.id
    await insert_transaction(
        conn,
        id=leg1.id,
        user_id=user_id,
        account_id=usd_fop_id,
        time=T,
        amount_cents=50000,
        operation_amount_cents=2190000,
        mcc=MCC_TRANSFER,
        direction="expense",
        counterparty_iban=UAH_BLACK_IBAN,
        description=leg1.description,
        currency_code="USD",
        special_category="transfer",
        related_transaction_id=leg2.id,
    )

    # Process leg 3: finds leg 4 as partner.
    leg3 = _tx(
        user_id=user_id,
        account_id=uah_fop_id,
        direction="expense",
        time=T + timedelta(seconds=2),
        amount=5000,
        cp_iban=UAH_BLACK_IBAN,
        desc="На чорну картку",
    )
    res3 = await strategy.detect_and_pair(conn, leg3)
    assert res3.special_category == "transfer"
    assert res3.related_transaction_id == leg4.id

    # Final: 2 pairs claimed, anomalies from earlier unpaired legs auto-resolved.
    assert await count_anomalies(conn, user_id) == 0
