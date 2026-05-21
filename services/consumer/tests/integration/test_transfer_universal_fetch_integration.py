"""Integration tests for universal candidate fetch.

TransferQueryRepo.find_universal_candidates test cases.

Test cases per `references/consumer-transfer-detection-test-suite.md` §9.
Skipped at module level until transfer_repo.py lands (Slice 17 task T12).
The corresponding implementation task removes this marker.
"""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from grosh_shared.normalized import NormalizedTransaction

from grosh_consumer.repositories.account_repo import AccountRepo
from grosh_consumer.repositories.anomaly_repo import AnomalyRepo
from grosh_consumer.sources.monobank.transfer.detector import (
    MonobankTransferDetection,
)
from grosh_consumer.sources.monobank.transfer.repo import TransferQueryRepo
from tests.integration.conftest import (
    get_transaction,
    insert_account,
    insert_transaction,
    insert_user,
)

pytestmark = pytest.mark.asyncio

T = datetime(2026, 6, 1, 12, 0, 0, tzinfo=UTC)
T_PLUS_1S = T + timedelta(seconds=1)
T_MINUS_1S = T - timedelta(seconds=1)
T_PLUS_2S = T + timedelta(seconds=2)
T_MINUS_2S = T - timedelta(seconds=2)
T_PLUS_2S_1MS = T + timedelta(seconds=2, milliseconds=1)
T_PLUS_3S = T + timedelta(seconds=3)

WINDOW_SECONDS = 2
MCC_TRANSFER = "4829"
MCC_GROCERY = "5411"

UAH_FOP_IBAN = "UA111000000000000000000111"
USD_FOP_IBAN = "UA222000000000000000000222"
EUR_FOP_IBAN = "UA333000000000000000000333"
UAH_BLACK_IBAN = "UA444000000000000000000444"
UAH_WHITE_IBAN = "UA555000000000000000000555"
EUR_CARD_IBAN = "UA666000000000000000000666"
EXTERNAL_IBAN = "UA999000000000000000000999"


def _make_repo():
    return TransferQueryRepo()


# ---------------------------------------------------------------------------
# §9.1 Filter behavior (cases 150–157)
# ---------------------------------------------------------------------------


async def test_150_returns_zero_when_no_match(conn):
    """Case 150: no candidate rows at all — fetch returns empty list."""
    user_id = await insert_user(conn)
    fop_id = await insert_account(
        conn,
        user_id=user_id,
        type="fop",
        currency_code="UAH",
        iban=UAH_FOP_IBAN,
    )
    repo = _make_repo()

    incoming = NormalizedTransaction(
        id=uuid4(),
        source="monobank",
        source_id="src-test",
        user_id=user_id,
        account_id=fop_id,
        time=T,
        amount_cents=5000,
        operation_amount_cents=5000,
        operation_currency_code="UAH",
        description=None,
        mcc=MCC_TRANSFER,
        cashback_amount_cents=0,
        balance_cents=None,
        hold=False,
        direction="expense",
        counterparty_iban=None,
        rate_source=None,
        metadata=None,
    )

    candidates = await repo.find_universal_candidates(conn, incoming)
    assert candidates == []


async def test_151_returns_one_matching_candidate(conn):
    """Case 151: single qualifying candidate row returns 1 result."""
    user_id = await insert_user(conn)
    fop_id = await insert_account(
        conn,
        user_id=user_id,
        type="fop",
        currency_code="UAH",
        iban=UAH_FOP_IBAN,
    )
    black_id = await insert_account(
        conn,
        user_id=user_id,
        type="black",
        currency_code="UAH",
        iban=UAH_BLACK_IBAN,
    )
    repo = _make_repo()

    partner_id = await insert_transaction(
        conn,
        user_id=user_id,
        account_id=black_id,
        time=T_PLUS_1S,
        amount_cents=5000,
        mcc=MCC_TRANSFER,
        direction="income",
    )

    incoming = NormalizedTransaction(
        id=uuid4(),
        source="monobank",
        source_id="src-test",
        user_id=user_id,
        account_id=fop_id,
        time=T,
        amount_cents=5000,
        operation_amount_cents=5000,
        operation_currency_code="UAH",
        description=None,
        mcc=MCC_TRANSFER,
        cashback_amount_cents=0,
        balance_cents=None,
        hold=False,
        direction="expense",
        counterparty_iban=None,
        rate_source=None,
        metadata=None,
    )

    candidates = await repo.find_universal_candidates(conn, incoming)
    assert len(candidates) == 1
    assert candidates[0].id == partner_id


async def test_152_returns_multiple_matching_candidates(conn):
    """Case 152: two qualifying candidates both returned."""
    user_id = await insert_user(conn)
    fop_id = await insert_account(
        conn,
        user_id=user_id,
        type="fop",
        currency_code="UAH",
        iban=UAH_FOP_IBAN,
    )
    black_id = await insert_account(
        conn,
        user_id=user_id,
        type="black",
        currency_code="UAH",
        iban=UAH_BLACK_IBAN,
    )
    white_id = await insert_account(
        conn,
        user_id=user_id,
        type="white",
        currency_code="UAH",
        iban=UAH_WHITE_IBAN,
    )
    repo = _make_repo()

    p1 = await insert_transaction(
        conn,
        user_id=user_id,
        account_id=black_id,
        time=T_PLUS_1S,
        amount_cents=5000,
        mcc=MCC_TRANSFER,
        direction="income",
    )
    p2 = await insert_transaction(
        conn,
        user_id=user_id,
        account_id=white_id,
        time=T_PLUS_1S,
        amount_cents=5000,
        mcc=MCC_TRANSFER,
        direction="income",
    )

    incoming = NormalizedTransaction(
        id=uuid4(),
        source="monobank",
        source_id="src-test",
        user_id=user_id,
        account_id=fop_id,
        time=T,
        amount_cents=5000,
        operation_amount_cents=5000,
        operation_currency_code="UAH",
        description=None,
        mcc=MCC_TRANSFER,
        cashback_amount_cents=0,
        balance_cents=None,
        hold=False,
        direction="expense",
        counterparty_iban=None,
        rate_source=None,
        metadata=None,
    )

    candidates = await repo.find_universal_candidates(conn, incoming)
    ids = {c.id for c in candidates}
    assert ids == {p1, p2}


async def test_153_excludes_claimed_rows(conn):
    """Case 153: candidate with related_transaction_id IS NOT NULL excluded."""
    user_id = await insert_user(conn)
    fop_id = await insert_account(
        conn,
        user_id=user_id,
        type="fop",
        currency_code="UAH",
        iban=UAH_FOP_IBAN,
    )
    black_id = await insert_account(
        conn,
        user_id=user_id,
        type="black",
        currency_code="UAH",
        iban=UAH_BLACK_IBAN,
    )
    repo = _make_repo()

    # Create a dummy tx to use as related_transaction_id
    claimed_by_id = await insert_transaction(
        conn,
        user_id=user_id,
        account_id=fop_id,
        time=T - timedelta(seconds=10),
        amount_cents=5000,
        mcc=MCC_TRANSFER,
        direction="expense",
    )

    await insert_transaction(
        conn,
        user_id=user_id,
        account_id=black_id,
        time=T_PLUS_1S,
        amount_cents=5000,
        mcc=MCC_TRANSFER,
        direction="income",
        related_transaction_id=claimed_by_id,
        special_category="transfer",
    )

    incoming = NormalizedTransaction(
        id=uuid4(),
        source="monobank",
        source_id="src-test",
        user_id=user_id,
        account_id=fop_id,
        time=T,
        amount_cents=5000,
        operation_amount_cents=5000,
        operation_currency_code="UAH",
        description=None,
        mcc=MCC_TRANSFER,
        cashback_amount_cents=0,
        balance_cents=None,
        hold=False,
        direction="expense",
        counterparty_iban=None,
        rate_source=None,
        metadata=None,
    )

    candidates = await repo.find_universal_candidates(conn, incoming)
    assert candidates == []


async def test_154_excludes_wrong_direction(conn):
    """Case 154: candidate with same direction as incoming excluded."""
    user_id = await insert_user(conn)
    fop_id = await insert_account(
        conn,
        user_id=user_id,
        type="fop",
        currency_code="UAH",
        iban=UAH_FOP_IBAN,
    )
    black_id = await insert_account(
        conn,
        user_id=user_id,
        type="black",
        currency_code="UAH",
        iban=UAH_BLACK_IBAN,
    )
    repo = _make_repo()

    # Same direction as incoming (expense): should be excluded.
    await insert_transaction(
        conn,
        user_id=user_id,
        account_id=black_id,
        time=T_PLUS_1S,
        amount_cents=5000,
        mcc=MCC_TRANSFER,
        direction="expense",  # same as incoming
    )

    incoming = NormalizedTransaction(
        id=uuid4(),
        source="monobank",
        source_id="src-test",
        user_id=user_id,
        account_id=fop_id,
        time=T,
        amount_cents=5000,
        operation_amount_cents=5000,
        operation_currency_code="UAH",
        description=None,
        mcc=MCC_TRANSFER,
        cashback_amount_cents=0,
        balance_cents=None,
        hold=False,
        direction="expense",
        counterparty_iban=None,
        rate_source=None,
        metadata=None,
    )

    candidates = await repo.find_universal_candidates(conn, incoming)
    assert candidates == []


async def test_155_excludes_wrong_mcc(conn):
    """Case 155: candidate with MCC 5411 (not 4829) excluded."""
    user_id = await insert_user(conn)
    fop_id = await insert_account(
        conn,
        user_id=user_id,
        type="fop",
        currency_code="UAH",
        iban=UAH_FOP_IBAN,
    )
    black_id = await insert_account(
        conn,
        user_id=user_id,
        type="black",
        currency_code="UAH",
        iban=UAH_BLACK_IBAN,
    )
    repo = _make_repo()

    await insert_transaction(
        conn,
        user_id=user_id,
        account_id=black_id,
        time=T_PLUS_1S,
        amount_cents=5000,
        mcc=MCC_GROCERY,  # wrong MCC
        direction="income",
    )

    incoming = NormalizedTransaction(
        id=uuid4(),
        source="monobank",
        source_id="src-test",
        user_id=user_id,
        account_id=fop_id,
        time=T,
        amount_cents=5000,
        operation_amount_cents=5000,
        operation_currency_code="UAH",
        description=None,
        mcc=MCC_TRANSFER,
        cashback_amount_cents=0,
        balance_cents=None,
        hold=False,
        direction="expense",
        counterparty_iban=None,
        rate_source=None,
        metadata=None,
    )

    candidates = await repo.find_universal_candidates(conn, incoming)
    assert candidates == []


async def test_156_excludes_same_account(conn):
    """Case 156: candidate on incoming's own account excluded."""
    user_id = await insert_user(conn)
    fop_id = await insert_account(
        conn,
        user_id=user_id,
        type="fop",
        currency_code="UAH",
        iban=UAH_FOP_IBAN,
    )
    repo = _make_repo()

    # Candidate on the SAME account as incoming — should be excluded.
    await insert_transaction(
        conn,
        user_id=user_id,
        account_id=fop_id,  # same account
        time=T_PLUS_1S,
        amount_cents=5000,
        mcc=MCC_TRANSFER,
        direction="income",
    )

    incoming = NormalizedTransaction(
        id=uuid4(),
        source="monobank",
        source_id="src-test",
        user_id=user_id,
        account_id=fop_id,
        time=T,
        amount_cents=5000,
        operation_amount_cents=5000,
        operation_currency_code="UAH",
        description=None,
        mcc=MCC_TRANSFER,
        cashback_amount_cents=0,
        balance_cents=None,
        hold=False,
        direction="expense",
        counterparty_iban=None,
        rate_source=None,
        metadata=None,
    )

    candidates = await repo.find_universal_candidates(conn, incoming)
    assert candidates == []


async def test_157_excludes_wrong_user(conn):
    """Case 157: candidate belonging to different user_id excluded."""
    user_id = await insert_user(conn)
    other_user_id = await insert_user(conn)

    fop_id = await insert_account(
        conn,
        user_id=user_id,
        type="fop",
        currency_code="UAH",
        iban=UAH_FOP_IBAN,
    )
    other_black_id = await insert_account(
        conn,
        user_id=other_user_id,
        type="black",
        currency_code="UAH",
        iban=UAH_BLACK_IBAN,
    )
    repo = _make_repo()

    await insert_transaction(
        conn,
        user_id=other_user_id,
        account_id=other_black_id,
        time=T_PLUS_1S,
        amount_cents=5000,
        mcc=MCC_TRANSFER,
        direction="income",
    )

    incoming = NormalizedTransaction(
        id=uuid4(),
        source="monobank",
        source_id="src-test",
        user_id=user_id,
        account_id=fop_id,
        time=T,
        amount_cents=5000,
        operation_amount_cents=5000,
        operation_currency_code="UAH",
        description=None,
        mcc=MCC_TRANSFER,
        cashback_amount_cents=0,
        balance_cents=None,
        hold=False,
        direction="expense",
        counterparty_iban=None,
        rate_source=None,
        metadata=None,
    )

    candidates = await repo.find_universal_candidates(conn, incoming)
    assert candidates == []


# ---------------------------------------------------------------------------
# §9.2 Time window boundaries (cases 158–162)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "candidate_time,expected_count",
    [
        (T + timedelta(seconds=2), 1),  # 158: exactly +2s → returned
        (T - timedelta(seconds=2), 1),  # 159: exactly -2s → returned
        (T + timedelta(seconds=2, milliseconds=1), 0),  # 160: +2.001s → not returned
        (T - timedelta(seconds=2, milliseconds=1), 0),  # 161: -2.001s → not returned
        (T + timedelta(seconds=3), 0),  # 162: +3s → not returned
    ],
    ids=[
        "158_plus_2s",
        "159_minus_2s",
        "160_plus_2s_1ms",
        "161_minus_2s_1ms",
        "162_plus_3s",
    ],
)
async def test_time_window_boundary(conn, candidate_time, expected_count):
    """Cases 158–162: time window boundary verification (±2s inclusive)."""
    user_id = await insert_user(conn)
    fop_id = await insert_account(
        conn,
        user_id=user_id,
        type="fop",
        currency_code="UAH",
        iban=UAH_FOP_IBAN,
    )
    black_id = await insert_account(
        conn,
        user_id=user_id,
        type="black",
        currency_code="UAH",
        iban=UAH_BLACK_IBAN,
    )
    repo = _make_repo()

    await insert_transaction(
        conn,
        user_id=user_id,
        account_id=black_id,
        time=candidate_time,
        amount_cents=5000,
        mcc=MCC_TRANSFER,
        direction="income",
    )

    incoming = NormalizedTransaction(
        id=uuid4(),
        source="monobank",
        source_id="src-test",
        user_id=user_id,
        account_id=fop_id,
        time=T,
        amount_cents=5000,
        operation_amount_cents=5000,
        operation_currency_code="UAH",
        description=None,
        mcc=MCC_TRANSFER,
        cashback_amount_cents=0,
        balance_cents=None,
        hold=False,
        direction="expense",
        counterparty_iban=None,
        rate_source=None,
        metadata=None,
    )

    candidates = await repo.find_universal_candidates(conn, incoming)
    assert len(candidates) == expected_count


# ---------------------------------------------------------------------------
# §9.3 Two-clause amount predicate (cases 163–167b)
# ---------------------------------------------------------------------------


async def test_163_same_currency_identity_both_clauses(conn):
    """Case 163: same-currency, both clauses match harmlessly (amount=op_amount)."""
    user_id = await insert_user(conn)
    fop_id = await insert_account(
        conn,
        user_id=user_id,
        type="fop",
        currency_code="UAH",
        iban=UAH_FOP_IBAN,
    )
    black_id = await insert_account(
        conn,
        user_id=user_id,
        type="black",
        currency_code="UAH",
        iban=UAH_BLACK_IBAN,
    )
    repo = _make_repo()

    partner_id = await insert_transaction(
        conn,
        user_id=user_id,
        account_id=black_id,
        time=T_PLUS_1S,
        amount_cents=5000,
        operation_amount_cents=5000,
        mcc=MCC_TRANSFER,
        direction="income",
    )

    incoming = NormalizedTransaction(
        id=uuid4(),
        source="monobank",
        source_id="src-test",
        user_id=user_id,
        account_id=fop_id,
        time=T,
        amount_cents=5000,
        operation_amount_cents=5000,
        operation_currency_code="UAH",
        description=None,
        mcc=MCC_TRANSFER,
        cashback_amount_cents=0,
        balance_cents=None,
        hold=False,
        direction="expense",
        counterparty_iban=None,
        rate_source=None,
        metadata=None,
    )

    candidates = await repo.find_universal_candidates(conn, incoming)
    assert len(candidates) == 1
    assert candidates[0].id == partner_id


async def test_164_asymmetric_expense_incoming_clause_1(conn):
    """Case 164: USD FOP expense incoming, UAH FOP income candidate found via clause 1.

    incoming: amount=50000 USD-cents, op_amount=2190000 UAH-cents.
    candidate: amount=2190000 UAH, op_amount=2190000 UAH.
    Clause 1: cand.amount (2190000) == incoming.op_amount (2190000) → match.
    """
    user_id = await insert_user(conn)
    usd_fop_id = await insert_account(
        conn,
        user_id=user_id,
        type="fop",
        currency_code="USD",
        iban=USD_FOP_IBAN,
    )
    uah_fop_id = await insert_account(
        conn,
        user_id=user_id,
        type="fop",
        currency_code="UAH",
        iban=UAH_FOP_IBAN,
    )
    repo = _make_repo()

    partner_id = await insert_transaction(
        conn,
        user_id=user_id,
        account_id=uah_fop_id,
        time=T_PLUS_1S,
        amount_cents=2190000,
        operation_amount_cents=2190000,
        mcc=MCC_TRANSFER,
        direction="income",
        currency_code="UAH",
    )

    incoming = NormalizedTransaction(
        id=uuid4(),
        source="monobank",
        source_id="src-test",
        user_id=user_id,
        account_id=usd_fop_id,
        time=T,
        amount_cents=50000,  # USD-cents
        operation_amount_cents=2190000,  # UAH-cents (what partner received)
        operation_currency_code="UAH",
        description=None,
        mcc=MCC_TRANSFER,
        cashback_amount_cents=0,
        balance_cents=None,
        hold=False,
        direction="expense",
        counterparty_iban=None,
        rate_source=None,
        metadata=None,
    )

    candidates = await repo.find_universal_candidates(conn, incoming)
    assert len(candidates) == 1
    assert candidates[0].id == partner_id


async def test_165_asymmetric_income_incoming_clause_2(conn):
    """Case 165: UAH FOP income incoming; USD FOP expense candidate found via clause 2.

    This is the case that requires the second clause.
    incoming: amount=2190000 UAH, op_amount=2190000 UAH.
    candidate: amount=50000 USD, op_amount=2190000 UAH.
    Clause 1: cand.amount (50000) != incoming.op_amount (2190000) → no match.
    Clause 2: cand.op_amount (2190000) == incoming.amount (2190000) → match.
    """
    user_id = await insert_user(conn)
    uah_fop_id = await insert_account(
        conn,
        user_id=user_id,
        type="fop",
        currency_code="UAH",
        iban=UAH_FOP_IBAN,
    )
    usd_fop_id = await insert_account(
        conn,
        user_id=user_id,
        type="fop",
        currency_code="USD",
        iban=USD_FOP_IBAN,
    )
    repo = _make_repo()

    partner_id = await insert_transaction(
        conn,
        user_id=user_id,
        account_id=usd_fop_id,
        time=T_MINUS_1S,
        amount_cents=50000,  # USD-cents
        operation_amount_cents=2190000,  # UAH-cents
        mcc=MCC_TRANSFER,
        direction="expense",
        currency_code="USD",
    )

    incoming = NormalizedTransaction(
        id=uuid4(),
        source="monobank",
        source_id="src-test",
        user_id=user_id,
        account_id=uah_fop_id,
        time=T,
        amount_cents=2190000,
        operation_amount_cents=2190000,
        operation_currency_code="UAH",
        description=None,
        mcc=MCC_TRANSFER,
        cashback_amount_cents=0,
        balance_cents=None,
        hold=False,
        direction="income",
        counterparty_iban=None,
        rate_source=None,
        metadata=None,
    )

    candidates = await repo.find_universal_candidates(conn, incoming)
    assert len(candidates) == 1
    assert candidates[0].id == partner_id


async def test_166_no_match_either_clause(conn):
    """Case 166: candidate amount doesn't satisfy either clause → excluded."""
    user_id = await insert_user(conn)
    fop_id = await insert_account(
        conn,
        user_id=user_id,
        type="fop",
        currency_code="UAH",
        iban=UAH_FOP_IBAN,
    )
    black_id = await insert_account(
        conn,
        user_id=user_id,
        type="black",
        currency_code="UAH",
        iban=UAH_BLACK_IBAN,
    )
    repo = _make_repo()

    # Completely different amount — neither clause matches.
    await insert_transaction(
        conn,
        user_id=user_id,
        account_id=black_id,
        time=T_PLUS_1S,
        amount_cents=99999,
        operation_amount_cents=99999,
        mcc=MCC_TRANSFER,
        direction="income",
    )

    incoming = NormalizedTransaction(
        id=uuid4(),
        source="monobank",
        source_id="src-test",
        user_id=user_id,
        account_id=fop_id,
        time=T,
        amount_cents=5000,
        operation_amount_cents=5000,
        operation_currency_code="UAH",
        description=None,
        mcc=MCC_TRANSFER,
        cashback_amount_cents=0,
        balance_cents=None,
        hold=False,
        direction="expense",
        counterparty_iban=None,
        rate_source=None,
        metadata=None,
    )

    candidates = await repo.find_universal_candidates(conn, incoming)
    assert candidates == []


async def test_167_null_op_amount_falls_back_to_amount(conn):
    """Case 167: incoming.operation_amount_cents IS NULL → fallback to amount_cents.

    The fetch SQL uses COALESCE(op_amount, amount_cents) for the first clause.
    Candidate with amount matching incoming.amount_cents should be returned.
    """
    user_id = await insert_user(conn)
    fop_id = await insert_account(
        conn,
        user_id=user_id,
        type="fop",
        currency_code="UAH",
        iban=UAH_FOP_IBAN,
    )
    black_id = await insert_account(
        conn,
        user_id=user_id,
        type="black",
        currency_code="UAH",
        iban=UAH_BLACK_IBAN,
    )
    repo = _make_repo()

    partner_id = await insert_transaction(
        conn,
        user_id=user_id,
        account_id=black_id,
        time=T_PLUS_1S,
        amount_cents=7000,
        mcc=MCC_TRANSFER,
        direction="income",
    )

    incoming = NormalizedTransaction(
        id=uuid4(),
        source="monobank",
        source_id="src-test",
        user_id=user_id,
        account_id=fop_id,
        time=T,
        amount_cents=7000,
        operation_amount_cents=None,  # NULL op_amount → fallback to amount
        operation_currency_code="UAH",
        description=None,
        mcc=MCC_TRANSFER,
        cashback_amount_cents=0,
        balance_cents=None,
        hold=False,
        direction="expense",
        counterparty_iban=None,
        rate_source=None,
        metadata=None,
    )

    candidates = await repo.find_universal_candidates(conn, incoming)
    assert len(candidates) == 1
    assert candidates[0].id == partner_id


async def test_167b_end_to_end_claim_under_null_op_amount(conn):
    """Case 167b: full detect_and_pair under NULL operation_amount_cents.

    Verifies the fallback holds across the full algorithm pass:
    - fetch finds partner via COALESCE fallback
    - pair claims (both legs get special_category='transfer')
    - metadata.layer.transfer.pair written with correct iban_evidence
    """
    user_id = await insert_user(conn)
    fop_id = await insert_account(
        conn,
        user_id=user_id,
        type="fop",
        currency_code="UAH",
        iban=UAH_FOP_IBAN,
    )
    black_id = await insert_account(
        conn,
        user_id=user_id,
        type="black",
        currency_code="UAH",
        iban=UAH_BLACK_IBAN,
    )

    partner_id = await insert_transaction(
        conn,
        user_id=user_id,
        account_id=black_id,
        time=T_PLUS_1S,
        amount_cents=7000,
        operation_amount_cents=None,  # partner also has NULL op_amount
        mcc=MCC_TRANSFER,
        direction="income",
    )

    repo = _make_repo()
    anomaly_repo = AnomalyRepo()
    account_repo = AccountRepo()
    strategy = MonobankTransferDetection(repo, account_repo, anomaly_repo)

    incoming = NormalizedTransaction(
        id=uuid4(),
        source="monobank",
        source_id="src-test",
        user_id=user_id,
        account_id=fop_id,
        time=T,
        amount_cents=7000,
        operation_amount_cents=None,
        operation_currency_code="UAH",
        description=None,
        mcc=MCC_TRANSFER,
        cashback_amount_cents=0,
        balance_cents=None,
        hold=False,
        direction="expense",
        counterparty_iban=None,
        rate_source=None,
        metadata=None,
    )

    result = await strategy.detect_and_pair(conn, incoming)
    assert result.special_category == "transfer"
    assert result.related_transaction_id == partner_id

    partner_row = await get_transaction(conn, partner_id)
    assert str(partner_row["special_category"]) == "transfer"

    # metadata.layer.transfer.pair must be written with some iban_evidence value
    transfer_meta = result.metadata_block
    assert transfer_meta is not None
    assert "pair" in transfer_meta
    assert transfer_meta["pair"]["iban_evidence"] in ("bilateral", "unilateral", "none")


# ---------------------------------------------------------------------------
# §9.4 Returned row shape (case 168)
# ---------------------------------------------------------------------------


async def test_168_returned_candidate_row_shape(conn):
    """Case 168: CandidateRow carries the fields the decision module needs."""
    user_id = await insert_user(conn)
    fop_id = await insert_account(
        conn,
        user_id=user_id,
        type="fop",
        currency_code="UAH",
        iban=UAH_FOP_IBAN,
    )
    black_id = await insert_account(
        conn,
        user_id=user_id,
        type="black",
        currency_code="UAH",
        iban=UAH_BLACK_IBAN,
    )
    repo = _make_repo()

    await insert_transaction(
        conn,
        user_id=user_id,
        account_id=black_id,
        time=T_PLUS_1S,
        amount_cents=5000,
        operation_amount_cents=5000,
        mcc=MCC_TRANSFER,
        direction="income",
        counterparty_iban=UAH_FOP_IBAN,
        description="З гривневого рахунку ФОП",
    )

    incoming = NormalizedTransaction(
        id=uuid4(),
        source="monobank",
        source_id="src-test",
        user_id=user_id,
        account_id=fop_id,
        time=T,
        amount_cents=5000,
        operation_amount_cents=5000,
        operation_currency_code="UAH",
        description=None,
        mcc=MCC_TRANSFER,
        cashback_amount_cents=0,
        balance_cents=None,
        hold=False,
        direction="expense",
        counterparty_iban=None,
        rate_source=None,
        metadata=None,
    )

    candidates = await repo.find_universal_candidates(conn, incoming)
    assert len(candidates) == 1
    row = candidates[0]

    # Required fields for the decision module.
    assert hasattr(row, "id")
    assert hasattr(row, "account_id")
    assert hasattr(row, "direction")
    assert hasattr(row, "counterparty_iban")
    assert hasattr(row, "description")
    assert hasattr(row, "amount_cents")
    assert hasattr(row, "operation_amount_cents")
    assert hasattr(row, "time")
