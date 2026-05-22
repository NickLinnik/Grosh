"""Integration tests for FOP cross-currency amount asymmetry (two-clause predicate).

Test cases per `references/consumer-transfer-detection-test-suite.md` §15.
Skipped at module level until detector.py lands (Slice 17 task T13).
The corresponding implementation task removes this marker.
"""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from tests.integration.conftest import (
    get_transaction,
    insert_account,
    insert_transaction,
    insert_user,
)

pytestmark = pytest.mark.asyncio


try:
    from grosh_shared.normalized import NormalizedTransaction

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
UAH_BLACK_IBAN = "UA444000000000000000000444"
UAH_WHITE_IBAN = "UA555000000000000000000555"

# Real cross-currency pair amounts: 50000 USD-cents ↔ 2190000 UAH-cents
USD_AMOUNT = 50000
UAH_AMOUNT = 2190000


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
    amount,
    op_amount=None,
    currency="UAH",
    cp_iban=None,
    desc=None,
    time=None,
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
        mcc=MCC_TRANSFER,
        cashback_amount_cents=0,
        balance_cents=None,
        hold=False,
        direction=direction,
        counterparty_iban=cp_iban,
        rate_source=None,
        metadata=None,
    )


# ---------------------------------------------------------------------------
# §15 Amount asymmetry tests (cases 209–212)
# ---------------------------------------------------------------------------


async def test_209_expense_first_income_second_finds_partner(conn):
    """Case 209: USD FOP expense first; UAH FOP income arrives second, finds partner.

    Clause 2: cand.op_amount = incoming.amount
    USD expense: amount=50000, op_amount=2190000 (seeded in DB, unclaimed).
    UAH income incoming: amount=2190000, op_amount=2190000.
    Clause 1: cand.amount (50000) != incoming.op_amount (2190000) → miss.
    Clause 2: cand.op_amount (2190000) == incoming.amount (2190000) → hit.
    """
    user_id = await insert_user(conn)
    usd_fop_id = await insert_account(
        conn, user_id=user_id, type="fop", currency_code="USD", iban=USD_FOP_IBAN
    )
    uah_fop_id = await insert_account(
        conn, user_id=user_id, type="fop", currency_code="UAH", iban=UAH_FOP_IBAN
    )

    # USD FOP expense in DB first (unclaimed).
    expense_id = await insert_transaction(
        conn,
        user_id=user_id,
        account_id=usd_fop_id,
        time=T,
        amount_cents=USD_AMOUNT,
        operation_amount_cents=UAH_AMOUNT,
        mcc=MCC_TRANSFER,
        direction="expense",
        currency_code="USD",
    )

    strategy = _make_strategy()
    # UAH FOP income arrives as incoming.
    income_tx = _tx(
        user_id=user_id,
        account_id=uah_fop_id,
        direction="income",
        amount=UAH_AMOUNT,
        op_amount=UAH_AMOUNT,
        time=T + timedelta(seconds=1),
    )

    result = await strategy.detect_and_pair(conn, income_tx)
    assert result.special_category == "transfer"
    assert result.related_transaction_id == expense_id

    expense_row = await get_transaction(conn, expense_id)
    assert str(expense_row["special_category"]) == "transfer"
    assert expense_row["related_transaction_id"] == income_tx.id


async def test_210_income_first_expense_second_finds_partner(conn):
    """Case 210: UAH FOP income first; USD FOP expense arrives second, finds partner.

    Clause 1: cand.amount = incoming.op_amount
    UAH income: amount=2190000 (seeded in DB, unclaimed).
    USD expense incoming: amount=50000, op_amount=2190000.
    Clause 1: cand.amount (2190000) == incoming.op_amount (2190000) → hit.
    """
    user_id = await insert_user(conn)
    uah_fop_id = await insert_account(
        conn, user_id=user_id, type="fop", currency_code="UAH", iban=UAH_FOP_IBAN
    )
    usd_fop_id = await insert_account(
        conn, user_id=user_id, type="fop", currency_code="USD", iban=USD_FOP_IBAN
    )

    # UAH FOP income in DB first (unclaimed).
    income_id = await insert_transaction(
        conn,
        user_id=user_id,
        account_id=uah_fop_id,
        time=T,
        amount_cents=UAH_AMOUNT,
        operation_amount_cents=UAH_AMOUNT,
        mcc=MCC_TRANSFER,
        direction="income",
    )

    strategy = _make_strategy()
    # USD FOP expense arrives as incoming.
    expense_tx = _tx(
        user_id=user_id,
        account_id=usd_fop_id,
        direction="expense",
        amount=USD_AMOUNT,
        op_amount=UAH_AMOUNT,
        currency="UAH",
        time=T + timedelta(seconds=1),
    )

    result = await strategy.detect_and_pair(conn, expense_tx)
    assert result.special_category == "transfer"
    assert result.related_transaction_id == income_id

    income_row = await get_transaction(conn, income_id)
    assert str(income_row["special_category"]) == "transfer"
    assert income_row["related_transaction_id"] == expense_tx.id


async def test_211_same_currency_direct_pair_either_order(conn):
    """Case 211: same-currency pair (amount=op_amount=5000) — order irrelevant."""
    user_id = await insert_user(conn)
    fop_id = await insert_account(
        conn, user_id=user_id, type="fop", currency_code="UAH", iban=UAH_FOP_IBAN
    )
    black_id = await insert_account(
        conn, user_id=user_id, type="black", currency_code="UAH", iban=UAH_BLACK_IBAN
    )

    # Expense in DB first.
    expense_id = await insert_transaction(
        conn,
        user_id=user_id,
        account_id=fop_id,
        time=T,
        amount_cents=5000,
        operation_amount_cents=5000,
        mcc=MCC_TRANSFER,
        direction="expense",
    )

    strategy = _make_strategy()
    income_tx = _tx(
        user_id=user_id,
        account_id=black_id,
        direction="income",
        amount=5000,
        op_amount=5000,
        time=T + timedelta(seconds=1),
    )

    result = await strategy.detect_and_pair(conn, income_tx)
    assert result.special_category == "transfer"
    assert result.related_transaction_id == expense_id


async def test_212_asymmetric_pair_bucket_locked_picks_correct_partner(conn):
    """Case 212: asymmetric pair + false candidate; bucket-locked picks right partner.

    ADR §3: 227 white-card-income rows have asymmetric pattern AND unilateral IBAN.
    Incoming (UAH FOP income) has 2 candidates under two-clause predicate:
    - Partner A (UAH FOP expense): unilateral evidence (income IBAN → white card).
    - Partner B (USD FOP expense, multi-hop): transitive IBAN → none (suppressed).
    Bucket-locked picks unilateral. Claim succeeds with the right partner.
    """
    user_id = await insert_user(conn)
    uah_fop_id = await insert_account(
        conn, user_id=user_id, type="fop", currency_code="UAH", iban=UAH_FOP_IBAN
    )
    usd_fop_id = await insert_account(
        conn, user_id=user_id, type="fop", currency_code="USD", iban=USD_FOP_IBAN
    )
    white_id = await insert_account(
        conn, user_id=user_id, type="white", currency_code="UAH", iban=UAH_WHITE_IBAN
    )

    # Incoming: UAH white income, amount=2190000.
    # Partner A: UAH FOP expense, amount=2190000, op_amount=2190000,
    #   cp_iban=UAH_WHITE_IBAN → unilateral evidence.
    # Partner B: USD FOP expense, amount=50000, op_amount=2190000,
    #   multi-hop desc → transitive IBAN → none evidence.

    partner_a_id = await insert_transaction(
        conn,
        user_id=user_id,
        account_id=uah_fop_id,
        time=T - timedelta(seconds=1),
        amount_cents=UAH_AMOUNT,
        operation_amount_cents=UAH_AMOUNT,
        mcc=MCC_TRANSFER,
        direction="expense",
        counterparty_iban=UAH_WHITE_IBAN,  # honest, points at incoming's white account
    )
    _partner_b_id = await insert_transaction(
        conn,
        user_id=user_id,
        account_id=usd_fop_id,
        time=T - timedelta(seconds=1),
        amount_cents=USD_AMOUNT,
        operation_amount_cents=UAH_AMOUNT,  # matches incoming.amount → clause 2
        mcc=MCC_TRANSFER,
        direction="expense",
        counterparty_iban=UAH_BLACK_IBAN,  # chain-end IBAN → transitive evidence
        description="На гривневий рахунок ФОП для переказу на картку",
        currency_code="USD",
    )

    strategy = _make_strategy()
    # Incoming: UAH white income, amount=2190000.
    income_tx = _tx(
        user_id=user_id,
        account_id=white_id,
        direction="income",
        amount=UAH_AMOUNT,
        op_amount=UAH_AMOUNT,
        cp_iban=UAH_FOP_IBAN,  # honest, points at UAH FOP → contributes to evidence
        time=T,
    )

    result = await strategy.detect_and_pair(conn, income_tx)
    # Both incoming and partner_a have honest IBANs pointing at each other →
    # partner_a evidence is bilateral. partner_b has transitive IBAN (multi-hop
    # expense), and after the consistency filter it's classified as none.
    # Bucket-locked picks bilateral (partner_a); single candidate → claim.
    assert result.special_category == "transfer"
    assert result.related_transaction_id == partner_a_id
    assert result.metadata_block["pair"]["iban_evidence"] == "bilateral"
