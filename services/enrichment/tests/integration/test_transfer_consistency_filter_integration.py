"""Integration tests for IBAN consistency filter (post-fetch hard filter).

Test cases per `references/consumer-transfer-detection-test-suite.md` §10.
Skipped at module level until detector.py lands (Slice 17 task T13).
The corresponding implementation task removes this marker.
"""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from tests.integration.conftest import (
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
EXTERNAL_IBAN = "UA999000000000000000000999"


def _make_strategy():
    return MonobankTransferDetection(
        TransferQueryRepo(),
        AccountRepo(),
        AnomalyRepo(),
    )


def _make_incoming(
    *, user_id, account_id, direction, amount_cents=5000, cp_iban=None, desc=None
):
    return NormalizedTransaction(
        id=uuid4(),
        source="monobank",
        source_id="src-test",
        user_id=user_id,
        account_id=account_id,
        time=T,
        amount_cents=amount_cents,
        operation_amount_cents=amount_cents,
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


# ---------------------------------------------------------------------------
# §10 Consistency filter cases (169–175)
# ---------------------------------------------------------------------------


async def test_169_both_null_vacuous_pass_claims(conn):
    """Case 169: both cp_iban NULL → consistency filter vacuously passes → claim."""
    user_id = await insert_user(conn)
    fop_id = await insert_account(
        conn, user_id=user_id, type="fop", currency_code="UAH", iban=UAH_FOP_IBAN
    )
    black_id = await insert_account(
        conn, user_id=user_id, type="black", currency_code="UAH", iban=UAH_BLACK_IBAN
    )

    partner_id = await insert_transaction(
        conn,
        user_id=user_id,
        account_id=black_id,
        time=T + timedelta(seconds=1),
        amount_cents=5000,
        mcc=MCC_TRANSFER,
        direction="income",
        counterparty_iban=None,
        description="Переказ на картку",
    )

    strategy = _make_strategy()
    incoming = _make_incoming(
        user_id=user_id,
        account_id=fop_id,
        direction="expense",
        cp_iban=None,
        desc="Переказ на картку",
    )

    result = await strategy.detect_and_pair(conn, incoming)
    assert result.special_category == "transfer"
    assert result.related_transaction_id == partner_id
    assert result.metadata_block is not None
    assert result.metadata_block["pair"]["iban_evidence"] == "none"


async def test_170_both_honest_pointing_at_each_other_bilateral(conn):
    """Case 170: both honest cp_ibans pointing at each other → bilateral claim."""
    user_id = await insert_user(conn)
    fop_id = await insert_account(
        conn, user_id=user_id, type="fop", currency_code="UAH", iban=UAH_FOP_IBAN
    )
    black_id = await insert_account(
        conn, user_id=user_id, type="black", currency_code="UAH", iban=UAH_BLACK_IBAN
    )

    partner_id = await insert_transaction(
        conn,
        user_id=user_id,
        account_id=black_id,
        time=T + timedelta(seconds=1),
        amount_cents=5000,
        mcc=MCC_TRANSFER,
        direction="income",
        counterparty_iban=UAH_FOP_IBAN,  # points at incoming's account
        description="З гривневого рахунку ФОП",
    )

    strategy = _make_strategy()
    incoming = _make_incoming(
        user_id=user_id,
        account_id=fop_id,
        direction="expense",
        cp_iban=UAH_BLACK_IBAN,  # points at partner's account
        desc="На чорну картку",
    )

    result = await strategy.detect_and_pair(conn, incoming)
    assert result.special_category == "transfer"
    assert result.related_transaction_id == partner_id
    assert result.metadata_block["pair"]["iban_evidence"] == "bilateral"


async def test_171_incoming_honest_pointing_at_candidate_null_unilateral(conn):
    """Case 171: incoming honest pointing at candidate; candidate null → unilateral."""
    user_id = await insert_user(conn)
    fop_id = await insert_account(
        conn, user_id=user_id, type="fop", currency_code="UAH", iban=UAH_FOP_IBAN
    )
    black_id = await insert_account(
        conn, user_id=user_id, type="black", currency_code="UAH", iban=UAH_BLACK_IBAN
    )

    partner_id = await insert_transaction(
        conn,
        user_id=user_id,
        account_id=black_id,
        time=T + timedelta(seconds=1),
        amount_cents=5000,
        mcc=MCC_TRANSFER,
        direction="income",
        counterparty_iban=None,  # null
        description="З гривневого рахунку ФОП",
    )

    strategy = _make_strategy()
    incoming = _make_incoming(
        user_id=user_id,
        account_id=fop_id,
        direction="expense",
        cp_iban=UAH_BLACK_IBAN,  # honest, points at partner
        desc="На чорну картку",
    )

    result = await strategy.detect_and_pair(conn, incoming)
    assert result.special_category == "transfer"
    assert result.related_transaction_id == partner_id
    assert result.metadata_block["pair"]["iban_evidence"] == "unilateral"


async def test_172_incoming_honest_pointing_wrong_account_drops_candidate(conn):
    """Case 172: incoming honest cp_iban points at WRONG account → candidate dropped."""
    user_id = await insert_user(conn)
    fop_id = await insert_account(
        conn, user_id=user_id, type="fop", currency_code="UAH", iban=UAH_FOP_IBAN
    )
    black_id = await insert_account(
        conn, user_id=user_id, type="black", currency_code="UAH", iban=UAH_BLACK_IBAN
    )
    # A third account: incoming's IBAN points at this, not at the candidate.
    _white_id = await insert_account(
        conn, user_id=user_id, type="white", currency_code="UAH", iban=UAH_WHITE_IBAN
    )

    await insert_transaction(
        conn,
        user_id=user_id,
        account_id=black_id,
        time=T + timedelta(seconds=1),
        amount_cents=5000,
        mcc=MCC_TRANSFER,
        direction="income",
        counterparty_iban=None,
    )

    strategy = _make_strategy()
    incoming = _make_incoming(
        user_id=user_id,
        account_id=fop_id,
        direction="expense",
        cp_iban=UAH_WHITE_IBAN,  # honest, but points at white (not black candidate)
    )

    result = await strategy.detect_and_pair(conn, incoming)
    # Consistency filter drops the candidate; no claim.
    assert result.special_category is None
    assert result.related_transaction_id is None


async def test_173_candidate_honest_pointing_wrong_account_drops(conn):
    """Case 173: candidate honest cp_iban points at WRONG account → dropped."""
    user_id = await insert_user(conn)
    fop_id = await insert_account(
        conn, user_id=user_id, type="fop", currency_code="UAH", iban=UAH_FOP_IBAN
    )
    black_id = await insert_account(
        conn, user_id=user_id, type="black", currency_code="UAH", iban=UAH_BLACK_IBAN
    )
    _white_id = await insert_account(
        conn, user_id=user_id, type="white", currency_code="UAH", iban=UAH_WHITE_IBAN
    )

    # Candidate's cp_iban points at white, not at incoming's fop account.
    await insert_transaction(
        conn,
        user_id=user_id,
        account_id=black_id,
        time=T + timedelta(seconds=1),
        amount_cents=5000,
        mcc=MCC_TRANSFER,
        direction="income",
        counterparty_iban=UAH_WHITE_IBAN,  # points at white, not fop
    )

    strategy = _make_strategy()
    incoming = _make_incoming(
        user_id=user_id,
        account_id=fop_id,
        direction="expense",
        cp_iban=None,
    )

    result = await strategy.detect_and_pair(conn, incoming)
    assert result.special_category is None
    assert result.related_transaction_id is None


@pytest.mark.parametrize(
    "incoming_status",
    ["null", "transitive", "honest"],
    ids=["174a_null", "174b_transitive", "174c_honest"],
)
async def test_174_unlinked_candidate_always_dropped(conn, incoming_status):
    """Cases 174a/b/c: unlinked cp_iban on candidate → always dropped."""
    user_id = await insert_user(conn)
    fop_id = await insert_account(
        conn, user_id=user_id, type="fop", currency_code="UAH", iban=UAH_FOP_IBAN
    )
    black_id = await insert_account(
        conn, user_id=user_id, type="black", currency_code="UAH", iban=UAH_BLACK_IBAN
    )

    # Candidate has an unlinked cp_iban (not owned by this user).
    await insert_transaction(
        conn,
        user_id=user_id,
        account_id=black_id,
        time=T + timedelta(seconds=1),
        amount_cents=5000,
        mcc=MCC_TRANSFER,
        direction="income",
        counterparty_iban=EXTERNAL_IBAN,  # external — no matching own account
    )

    # Set incoming cp_iban based on the parametrized status.
    if incoming_status == "null":
        cp_iban = None
        desc = None
    elif incoming_status == "transitive":
        # multi-hop expense description triggers transitive rule
        cp_iban = UAH_BLACK_IBAN  # will be suppressed as transitive
        desc = "На гривневий рахунок ФОП для переказу на картку"
    else:  # honest
        cp_iban = UAH_BLACK_IBAN
        desc = "На чорну картку"

    strategy = _make_strategy()
    incoming = _make_incoming(
        user_id=user_id,
        account_id=fop_id,
        direction="expense",
        cp_iban=cp_iban,
        desc=desc,
    )

    result = await strategy.detect_and_pair(conn, incoming)
    # External candidate is always dropped by the consistency filter.
    assert result.special_category is None
    assert result.related_transaction_id is None


async def test_175_incoming_transitive_candidate_honest_pointing_at_incoming(conn):
    """Case 175: multi-hop expense (transitive); honest candidate → unilateral claim."""
    user_id = await insert_user(conn)
    usd_fop_id = await insert_account(
        conn, user_id=user_id, type="fop", currency_code="USD", iban=USD_FOP_IBAN
    )
    uah_fop_id = await insert_account(
        conn, user_id=user_id, type="fop", currency_code="UAH", iban=UAH_FOP_IBAN
    )

    partner_id = await insert_transaction(
        conn,
        user_id=user_id,
        account_id=uah_fop_id,
        time=T + timedelta(seconds=1),
        amount_cents=2190000,
        operation_amount_cents=2190000,
        mcc=MCC_TRANSFER,
        direction="income",
        counterparty_iban=USD_FOP_IBAN,  # honest, points at incoming's account
        currency_code="UAH",
    )

    strategy = _make_strategy()
    # Multi-hop expense: cp_iban points at chain end (card), not immediate partner.
    # This triggers the directional transitive rule → cp_iban_status=transitive.
    incoming = NormalizedTransaction(
        id=uuid4(),
        source="monobank",
        source_id="src-test",
        user_id=user_id,
        account_id=usd_fop_id,
        time=T,
        amount_cents=50000,
        operation_amount_cents=2190000,
        operation_currency_code="UAH",
        description="На гривневий рахунок ФОП для переказу на картку",
        mcc=MCC_TRANSFER,
        cashback_amount_cents=0,
        balance_cents=None,
        hold=False,
        direction="expense",
        counterparty_iban=UAH_BLACK_IBAN,  # chain-end IBAN → classified as transitive
        rate_source=None,
        metadata=None,
    )

    result = await strategy.detect_and_pair(conn, incoming)
    # Transitive IBAN suppressed, candidate honest → passes consistency → unilateral.
    assert result.special_category == "transfer"
    assert result.related_transaction_id == partner_id
    assert result.metadata_block["pair"]["iban_evidence"] == "unilateral"
