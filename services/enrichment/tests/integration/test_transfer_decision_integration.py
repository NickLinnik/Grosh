"""Integration tests for count-and-decide branch (end-to-end via detect_and_pair).

Test cases per `references/consumer-transfer-detection-test-suite.md` §11.
Skipped at module level until detector.py lands (Slice 17 task T13).
The corresponding implementation task removes this marker.
"""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from grosh_shared.normalized import NormalizedTransaction

from grosh_enrichment.repositories.account_repo import AccountRepo
from grosh_enrichment.repositories.anomaly_repo import AnomalyRepo
from grosh_enrichment.sources.monobank.transfer.detector import (
    MonobankTransferDetection,
)
from grosh_enrichment.sources.monobank.transfer.repo import TransferQueryRepo
from tests.integration.conftest import (
    get_transaction,
    get_transfer_metadata,
    insert_account,
    insert_transaction,
    insert_user,
)

pytestmark = pytest.mark.asyncio

T = datetime(2026, 6, 1, 12, 0, 0, tzinfo=UTC)
MCC_TRANSFER = "4829"

UAH_FOP_IBAN = "UA111000000000000000000111"
USD_FOP_IBAN = "UA222000000000000000000222"
UAH_BLACK_IBAN = "UA444000000000000000000444"
UAH_WHITE_IBAN = "UA555000000000000000000555"


def _make_strategy():
    return MonobankTransferDetection(
        TransferQueryRepo(),
        AccountRepo(),
        AnomalyRepo(),
    )


def _incoming(
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
# §11.1 count == 0 (cases 176–178)
# ---------------------------------------------------------------------------


async def test_176_no_candidates_no_transfer_prefix_no_anomaly(conn):
    """Case 176: no candidates, description 'Олена К.' → no claim, no anomaly."""
    user_id = await insert_user(conn)
    black_id = await insert_account(
        conn, user_id=user_id, type="black", currency_code="UAH", iban=UAH_BLACK_IBAN
    )

    strategy = _make_strategy()
    incoming = _incoming(
        user_id=user_id,
        account_id=black_id,
        direction="expense",
        desc="Олена К.",
    )

    result = await strategy.detect_and_pair(conn, incoming)
    assert result.special_category is None
    assert result.related_transaction_id is None
    assert result.anomalies == []


async def test_177_no_candidates_z_prefix_unpaired_from_description(conn):
    """Case 177: no candidates, 'З Чорної картки' → unpaired_from_description."""
    user_id = await insert_user(conn)
    white_id = await insert_account(
        conn, user_id=user_id, type="white", currency_code="UAH", iban=UAH_WHITE_IBAN
    )

    strategy = _make_strategy()
    incoming = _incoming(
        user_id=user_id,
        account_id=white_id,
        direction="income",
        desc="З Чорної картки",
    )

    result = await strategy.detect_and_pair(conn, incoming)
    assert result.special_category is None
    assert result.related_transaction_id is None
    assert len(result.anomalies) == 1
    assert result.anomalies[0].reason_code == "unpaired_from_description"


async def test_178_no_candidates_na_prefix_unpaired_to_description(conn):
    """Case 178: no candidates, 'На білу картку' → unpaired_to_description."""
    user_id = await insert_user(conn)
    black_id = await insert_account(
        conn, user_id=user_id, type="black", currency_code="UAH", iban=UAH_BLACK_IBAN
    )

    strategy = _make_strategy()
    incoming = _incoming(
        user_id=user_id,
        account_id=black_id,
        direction="expense",
        desc="На білу картку",
    )

    result = await strategy.detect_and_pair(conn, incoming)
    assert result.special_category is None
    assert result.related_transaction_id is None
    assert len(result.anomalies) == 1
    assert result.anomalies[0].reason_code == "unpaired_to_description"


# ---------------------------------------------------------------------------
# §11.2 count == 1 (cases 179–182)
# ---------------------------------------------------------------------------


async def test_179_single_bilateral_candidate_claims_and_writes_partner_metadata(conn):
    """Case 179: single bilateral candidate → claim; partner row gets pair block."""
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
        counterparty_iban=UAH_FOP_IBAN,
        description="З гривневого рахунку ФОП",
    )

    strategy = _make_strategy()
    incoming = _incoming(
        user_id=user_id,
        account_id=fop_id,
        direction="expense",
        cp_iban=UAH_BLACK_IBAN,
        desc="На чорну картку",
    )

    result = await strategy.detect_and_pair(conn, incoming)
    assert result.special_category == "transfer"
    assert result.related_transaction_id == partner_id
    assert result.anomalies == []

    partner_row = await get_transaction(conn, partner_id)
    assert str(partner_row["special_category"]) == "transfer"
    assert partner_row["related_transaction_id"] == incoming.id

    # Partner metadata updated with pair block.
    partner_meta = await get_transfer_metadata(conn, partner_id)
    assert partner_meta is not None
    assert "pair" in partner_meta
    assert partner_meta["pair"]["iban_evidence"] == "bilateral"
    assert partner_meta["pair"]["description_decisive"] is False


async def test_180_single_candidate_descriptions_inconsistent_canary_claim(conn):
    """Case 180: single candidate, descriptions inconsistent → claim + canary."""
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

    partner_id = await insert_transaction(
        conn,
        user_id=user_id,
        account_id=black_id,
        time=T + timedelta(seconds=1),
        amount_cents=5000,
        mcc=MCC_TRANSFER,
        direction="income",
        counterparty_iban=None,
        # Description says "from white" but incoming is on fop — inconsistency.
        description="З Білої картки",
    )

    strategy = _make_strategy()
    incoming = _incoming(
        user_id=user_id,
        account_id=fop_id,
        direction="expense",
        cp_iban=None,
        desc="Переказ на картку",
    )

    result = await strategy.detect_and_pair(conn, incoming)
    # Pair still claims (canary semantics).
    assert result.special_category == "transfer"
    assert result.related_transaction_id == partner_id
    # Canary anomaly recorded.
    assert any(
        a.reason_code == "description_consistency_mismatch" for a in result.anomalies
    )


async def test_181_single_unilateral_candidate_claims(conn):
    """Case 181: single candidate, iban_evidence=unilateral → claim; pair block set."""
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
        counterparty_iban=UAH_FOP_IBAN,  # honest → unilateral (incoming cp_iban=null)
    )

    strategy = _make_strategy()
    incoming = _incoming(
        user_id=user_id,
        account_id=fop_id,
        direction="expense",
        cp_iban=None,  # null → one side honest = unilateral
    )

    result = await strategy.detect_and_pair(conn, incoming)
    assert result.special_category == "transfer"
    assert result.related_transaction_id == partner_id
    assert result.metadata_block["pair"]["iban_evidence"] == "unilateral"


async def test_182_single_none_evidence_candidate_claims(conn):
    """Case 182: single candidate, iban_evidence=none → claim; pair block set."""
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
        counterparty_iban=None,  # null → none evidence
    )

    strategy = _make_strategy()
    incoming = _incoming(
        user_id=user_id,
        account_id=fop_id,
        direction="expense",
        cp_iban=None,  # null → none evidence
    )

    result = await strategy.detect_and_pair(conn, incoming)
    assert result.special_category == "transfer"
    assert result.related_transaction_id == partner_id
    assert result.metadata_block["pair"]["iban_evidence"] == "none"


# ---------------------------------------------------------------------------
# §11.3 count > 1 — bucket-locked behavior (cases 183–191)
# ---------------------------------------------------------------------------


async def test_183_two_bilateral_both_valid_ambiguous(conn):
    """Case 183: two bilateral; both pass description filter → ambiguous_pair_match."""
    user_id = await insert_user(conn)
    fop_id = await insert_account(
        conn, user_id=user_id, type="fop", currency_code="UAH", iban=UAH_FOP_IBAN
    )
    black_id = await insert_account(
        conn, user_id=user_id, type="black", currency_code="UAH", iban=UAH_BLACK_IBAN
    )
    white_id = await insert_account(
        conn, user_id=user_id, type="white", currency_code="UAH", iban=UAH_WHITE_IBAN
    )

    p1 = await insert_transaction(
        conn,
        user_id=user_id,
        account_id=black_id,
        time=T + timedelta(seconds=1),
        amount_cents=5000,
        mcc=MCC_TRANSFER,
        direction="income",
        counterparty_iban=UAH_FOP_IBAN,
        description="З гривневого рахунку ФОП",
    )
    p2 = await insert_transaction(
        conn,
        user_id=user_id,
        account_id=white_id,
        time=T + timedelta(seconds=1),
        amount_cents=5000,
        mcc=MCC_TRANSFER,
        direction="income",
        counterparty_iban=UAH_FOP_IBAN,
        description="З гривневого рахунку ФОП",
    )

    strategy = _make_strategy()
    incoming = _incoming(
        user_id=user_id,
        account_id=fop_id,
        direction="expense",
        cp_iban=None,  # incoming null so both candidates pass consistency
        desc="Переказ на картку",
    )

    result = await strategy.detect_and_pair(conn, incoming)
    assert result.special_category is None
    assert result.related_transaction_id is None
    assert any(a.reason_code == "ambiguous_pair_match" for a in result.anomalies)
    anom = next(a for a in result.anomalies if a.reason_code == "ambiguous_pair_match")
    assert set(anom.candidate_ids) == {p1, p2}


# Test case 184 (bucket-locked bilateral with description tiebreaker, 2 candidates)
# is covered by unit test_131 in test_transfer_decision.py. At the integration
# level the scenario is structurally impossible: bilateral evidence requires
# incoming.cp_iban to resolve to the candidate's account, and incoming has only
# ONE cp_iban — so at most one candidate is bilateral; the consistency filter
# drops the rest. The unit test exercises the decision module directly with
# pre-scored candidates, which is the right level for this assertion.


async def test_185_two_bilateral_description_eliminates_all_ambiguous(conn):
    """Case 185: two bilateral candidates, description eliminates both → ambiguous."""
    user_id = await insert_user(conn)
    fop_id = await insert_account(
        conn, user_id=user_id, type="fop", currency_code="UAH", iban=UAH_FOP_IBAN
    )
    black_id = await insert_account(
        conn, user_id=user_id, type="black", currency_code="UAH", iban=UAH_BLACK_IBAN
    )
    white_id = await insert_account(
        conn, user_id=user_id, type="white", currency_code="UAH", iban=UAH_WHITE_IBAN
    )

    _p1 = await insert_transaction(
        conn,
        user_id=user_id,
        account_id=black_id,
        time=T + timedelta(seconds=1),
        amount_cents=5000,
        mcc=MCC_TRANSFER,
        direction="income",
        counterparty_iban=UAH_FOP_IBAN,
        # "from white" but account is black — inconsistent with expense "to black"
        description="З Білої картки",
    )
    _p2 = await insert_transaction(
        conn,
        user_id=user_id,
        account_id=white_id,
        time=T + timedelta(seconds=1),
        amount_cents=5000,
        mcc=MCC_TRANSFER,
        direction="income",
        counterparty_iban=UAH_FOP_IBAN,
        description="З Білої картки",
    )

    strategy = _make_strategy()
    # Expense says "to black" — inconsistent with both candidates' descriptions
    incoming = _incoming(
        user_id=user_id,
        account_id=fop_id,
        direction="expense",
        cp_iban=None,
        desc="На чорну картку",
    )

    result = await strategy.detect_and_pair(conn, incoming)
    assert result.special_category is None
    assert any(a.reason_code == "ambiguous_pair_match" for a in result.anomalies)


async def test_186_bilateral_plus_unilateral_picks_bilateral(conn):
    """Case 186: 1 bilateral + 2 unilateral → bucket-locked picks bilateral."""
    user_id = await insert_user(conn)
    fop_id = await insert_account(
        conn, user_id=user_id, type="fop", currency_code="UAH", iban=UAH_FOP_IBAN
    )
    black_id = await insert_account(
        conn, user_id=user_id, type="black", currency_code="UAH", iban=UAH_BLACK_IBAN
    )
    white_id = await insert_account(
        conn, user_id=user_id, type="white", currency_code="UAH", iban=UAH_WHITE_IBAN
    )
    # A third account for the second unilateral candidate
    usd_fop_id = await insert_account(
        conn, user_id=user_id, type="fop", currency_code="USD", iban=USD_FOP_IBAN
    )

    # Bilateral: income on black, cp_iban=fop; incoming will have cp_iban=black
    bilateral_id = await insert_transaction(
        conn,
        user_id=user_id,
        account_id=black_id,
        time=T + timedelta(seconds=1),
        amount_cents=5000,
        mcc=MCC_TRANSFER,
        direction="income",
        counterparty_iban=UAH_FOP_IBAN,
    )
    # Unilateral 1: income on white, null cp_iban
    await insert_transaction(
        conn,
        user_id=user_id,
        account_id=white_id,
        time=T + timedelta(seconds=1),
        amount_cents=5000,
        mcc=MCC_TRANSFER,
        direction="income",
        counterparty_iban=None,
    )
    # Unilateral 2: income on usd_fop, cp_iban points at fop (honest, so unilateral)
    await insert_transaction(
        conn,
        user_id=user_id,
        account_id=usd_fop_id,
        time=T + timedelta(seconds=1),
        amount_cents=5000,
        mcc=MCC_TRANSFER,
        direction="income",
        counterparty_iban=UAH_FOP_IBAN,
    )

    strategy = _make_strategy()
    incoming = _incoming(
        user_id=user_id,
        account_id=fop_id,
        direction="expense",
        cp_iban=UAH_BLACK_IBAN,  # honest, points at black → bilateral
    )

    result = await strategy.detect_and_pair(conn, incoming)
    assert result.special_category == "transfer"
    assert result.related_transaction_id == bilateral_id
    assert result.metadata_block["pair"]["iban_evidence"] == "bilateral"


async def test_187_two_unilateral_description_narrows_to_one(conn):
    """Case 187: 0 bilateral + 2 unilateral; description narrows to 1 → claim."""
    user_id = await insert_user(conn)
    fop_id = await insert_account(
        conn, user_id=user_id, type="fop", currency_code="UAH", iban=UAH_FOP_IBAN
    )
    black_id = await insert_account(
        conn, user_id=user_id, type="black", currency_code="UAH", iban=UAH_BLACK_IBAN
    )
    white_id = await insert_account(
        conn, user_id=user_id, type="white", currency_code="UAH", iban=UAH_WHITE_IBAN
    )

    # Both have cp_iban pointing at fop → unilateral (incoming has null cp_iban).
    # Income desc names the SOURCE (expense) account; only the candidate whose
    # income desc is consistent with expense=UAH FOP survives the description filter.
    survivor = await insert_transaction(
        conn,
        user_id=user_id,
        account_id=black_id,
        time=T + timedelta(seconds=1),
        amount_cents=5000,
        mcc=MCC_TRANSFER,
        direction="income",
        counterparty_iban=UAH_FOP_IBAN,
        description="З гривневого рахунку ФОП",  # source = UAH FOP → consistent
    )
    await insert_transaction(
        conn,
        user_id=user_id,
        account_id=white_id,
        time=T + timedelta(seconds=1),
        amount_cents=5000,
        mcc=MCC_TRANSFER,
        direction="income",
        counterparty_iban=UAH_FOP_IBAN,
        description="З Білої картки",  # source=white card → mismatch with fop
    )

    strategy = _make_strategy()
    incoming = _incoming(
        user_id=user_id,
        account_id=fop_id,
        direction="expense",
        cp_iban=None,
        desc="Переказ на картку",  # generic — no expense-side description constraint
    )

    result = await strategy.detect_and_pair(conn, incoming)
    assert result.special_category == "transfer"
    assert result.related_transaction_id == survivor
    assert result.metadata_block["pair"]["iban_evidence"] == "unilateral"
    assert result.metadata_block["pair"]["description_decisive"] is True


async def test_188_two_unilateral_description_eliminates_all_ambiguous(conn):
    """Case 188: 0 bilateral + 2 unilateral; description eliminates both → ambiguous."""
    user_id = await insert_user(conn)
    fop_id = await insert_account(
        conn, user_id=user_id, type="fop", currency_code="UAH", iban=UAH_FOP_IBAN
    )
    black_id = await insert_account(
        conn, user_id=user_id, type="black", currency_code="UAH", iban=UAH_BLACK_IBAN
    )
    white_id = await insert_account(
        conn, user_id=user_id, type="white", currency_code="UAH", iban=UAH_WHITE_IBAN
    )

    p1 = await insert_transaction(
        conn,
        user_id=user_id,
        account_id=black_id,
        time=T + timedelta(seconds=1),
        amount_cents=5000,
        mcc=MCC_TRANSFER,
        direction="income",
        counterparty_iban=UAH_FOP_IBAN,
        description="З Білої картки",  # says white but account is black
    )
    p2 = await insert_transaction(
        conn,
        user_id=user_id,
        account_id=white_id,
        time=T + timedelta(seconds=1),
        amount_cents=5000,
        mcc=MCC_TRANSFER,
        direction="income",
        counterparty_iban=UAH_FOP_IBAN,
        description="З Чорної картки",  # says black but account is white
    )

    strategy = _make_strategy()
    incoming = _incoming(
        user_id=user_id,
        account_id=fop_id,
        direction="expense",
        cp_iban=None,
        desc="На чорну картку",
    )

    result = await strategy.detect_and_pair(conn, incoming)
    assert result.special_category is None
    assert any(a.reason_code == "ambiguous_pair_match" for a in result.anomalies)
    anom = next(a for a in result.anomalies if a.reason_code == "ambiguous_pair_match")
    assert set(anom.candidate_ids) == {p1, p2}


async def test_189_two_none_description_narrows_to_one_claims(conn):
    """Case 189: 0 bilateral + 0 unilateral + 2 none; narrows to 1 → claim."""
    user_id = await insert_user(conn)
    fop_id = await insert_account(
        conn, user_id=user_id, type="fop", currency_code="UAH", iban=UAH_FOP_IBAN
    )
    black_id = await insert_account(
        conn, user_id=user_id, type="black", currency_code="UAH", iban=UAH_BLACK_IBAN
    )
    white_id = await insert_account(
        conn, user_id=user_id, type="white", currency_code="UAH", iban=UAH_WHITE_IBAN
    )

    # Both have null cp_iban → none evidence. Income desc names the SOURCE
    # (expense) account; only the candidate whose income desc is consistent
    # with expense=UAH FOP survives the description filter.
    survivor = await insert_transaction(
        conn,
        user_id=user_id,
        account_id=black_id,
        time=T + timedelta(seconds=1),
        amount_cents=5000,
        mcc=MCC_TRANSFER,
        direction="income",
        counterparty_iban=None,
        description="З гривневого рахунку ФОП",  # source = UAH FOP → consistent
    )
    await insert_transaction(
        conn,
        user_id=user_id,
        account_id=white_id,
        time=T + timedelta(seconds=1),
        amount_cents=5000,
        mcc=MCC_TRANSFER,
        direction="income",
        counterparty_iban=None,
        description="З Білої картки",  # source=white card → mismatch with fop
    )

    strategy = _make_strategy()
    incoming = _incoming(
        user_id=user_id,
        account_id=fop_id,
        direction="expense",
        cp_iban=None,
        desc="Переказ на картку",  # generic — no expense-side description constraint
    )

    result = await strategy.detect_and_pair(conn, incoming)
    assert result.special_category == "transfer"
    assert result.related_transaction_id == survivor
    assert result.metadata_block["pair"]["iban_evidence"] == "none"
    assert result.metadata_block["pair"]["description_decisive"] is True


async def test_190_three_none_description_eliminates_all_description_account_mismatch(
    conn,
):
    """Case 190: 3 none-evidence candidates, all eliminated by description filter."""
    user_id = await insert_user(conn)
    fop_id = await insert_account(
        conn, user_id=user_id, type="fop", currency_code="UAH", iban=UAH_FOP_IBAN
    )
    black_id = await insert_account(
        conn, user_id=user_id, type="black", currency_code="UAH", iban=UAH_BLACK_IBAN
    )
    white_id = await insert_account(
        conn, user_id=user_id, type="white", currency_code="UAH", iban=UAH_WHITE_IBAN
    )
    usd_fop_id = await insert_account(
        conn, user_id=user_id, type="fop", currency_code="USD", iban=USD_FOP_IBAN
    )

    # Three none-evidence candidates, each description inconsistent with incoming
    _p1 = await insert_transaction(
        conn,
        user_id=user_id,
        account_id=black_id,
        time=T + timedelta(seconds=1),
        amount_cents=5000,
        mcc=MCC_TRANSFER,
        direction="income",
        counterparty_iban=None,
        description="З Білої картки",  # says white, account is black
    )
    _p2 = await insert_transaction(
        conn,
        user_id=user_id,
        account_id=white_id,
        time=T + timedelta(seconds=1),
        amount_cents=5000,
        mcc=MCC_TRANSFER,
        direction="income",
        counterparty_iban=None,
        description="З Чорної картки",  # says black, account is white
    )
    _p3 = await insert_transaction(
        conn,
        user_id=user_id,
        account_id=usd_fop_id,
        time=T + timedelta(seconds=1),
        amount_cents=5000,
        mcc=MCC_TRANSFER,
        direction="income",
        counterparty_iban=None,
        description="З Білої картки",  # says white, account is usd_fop
    )

    strategy = _make_strategy()
    incoming = _incoming(
        user_id=user_id,
        account_id=fop_id,
        direction="expense",
        cp_iban=None,
        desc="На чорну картку",
    )

    result = await strategy.detect_and_pair(conn, incoming)
    assert result.special_category is None
    assert any(
        a.reason_code == "description_account_mismatch" for a in result.anomalies
    )


async def test_191_two_none_both_pass_description_ambiguous(conn):
    """Case 191: 2 none-evidence, both pass description → ambiguous_pair_match."""
    user_id = await insert_user(conn)
    fop_id = await insert_account(
        conn, user_id=user_id, type="fop", currency_code="UAH", iban=UAH_FOP_IBAN
    )
    black_id = await insert_account(
        conn, user_id=user_id, type="black", currency_code="UAH", iban=UAH_BLACK_IBAN
    )
    white_id = await insert_account(
        conn, user_id=user_id, type="white", currency_code="UAH", iban=UAH_WHITE_IBAN
    )

    # Both have null cp_iban and descriptions that pass validation
    p1 = await insert_transaction(
        conn,
        user_id=user_id,
        account_id=black_id,
        time=T + timedelta(seconds=1),
        amount_cents=5000,
        mcc=MCC_TRANSFER,
        direction="income",
        counterparty_iban=None,
        description="Переказ на картку",  # generic → no constraint → always passes
    )
    p2 = await insert_transaction(
        conn,
        user_id=user_id,
        account_id=white_id,
        time=T + timedelta(seconds=1),
        amount_cents=5000,
        mcc=MCC_TRANSFER,
        direction="income",
        counterparty_iban=None,
        description="Переказ на картку",
    )

    strategy = _make_strategy()
    incoming = _incoming(
        user_id=user_id,
        account_id=fop_id,
        direction="expense",
        cp_iban=None,
        desc="Переказ на картку",
    )

    result = await strategy.detect_and_pair(conn, incoming)
    assert result.special_category is None
    assert any(a.reason_code == "ambiguous_pair_match" for a in result.anomalies)
    anom = next(a for a in result.anomalies if a.reason_code == "ambiguous_pair_match")
    assert set(anom.candidate_ids) == {p1, p2}
