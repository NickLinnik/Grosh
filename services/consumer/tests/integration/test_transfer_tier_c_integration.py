"""Integration tests for transfer detection Tier C (amount fallback) against Postgres.

§13 of the transfer detection test specification.
"""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from tests.helpers import make_event
from tests.integration.conftest import (
    get_transaction,
    insert_account,
    insert_transaction,
    insert_user,
)

pytestmark = pytest.mark.asyncio

T = datetime(2025, 6, 1, 12, 0, 0, tzinfo=UTC)
MCC_TRANSFER = "4829"
MCC_GROCERY = "5411"


# ---------------------------------------------------------------------------
# §13.1 Same-currency card→card
# ---------------------------------------------------------------------------


async def test_same_currency_card_to_card_paired(conn, transfer_service):
    """§123: Black expense + white income, amounts match, descriptions validate.

    Tier C cross-match: expense.operation_amount_cents = income.amount_cents.
    Paired.
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

    # Pre-existing income on white card
    partner_id = await insert_transaction(
        conn,
        user_id=user_id,
        account_id=white_id,
        time=T + timedelta(seconds=1),
        amount_cents=5000,
        operation_amount_cents=5000,
        mcc=MCC_TRANSFER,
        direction="income",
        counterparty_iban=None,
        description="З Чорної картки",
    )

    new_tx_id = uuid4()
    event = make_event(
        id=new_tx_id,
        user_id=user_id,
        account_id=black_id,
        time=T,
        mcc=MCC_TRANSFER,
        amount_cents=5000,
        operation_amount_cents=5000,
        direction="expense",
        counterparty_iban=None,
        description="Переказ на картку",
    )

    result = await transfer_service.detect_and_pair(conn, event)

    assert result.special_category == "transfer"
    assert result.related_transaction_id == partner_id
    assert result.anomalies == []

    partner_row = await get_transaction(conn, partner_id)
    assert str(partner_row["special_category"]) == "transfer"
    assert partner_row["related_transaction_id"] == new_tx_id


async def test_amounts_match_description_validation_fails_description_account_mismatch(
    conn, transfer_service
):
    """§124: White income desc "З Білої картки" but expense is on black card.

    type=white constraint doesn't match type=black. Not paired.
    Anomaly: description_account_mismatch.
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

    partner_id = await insert_transaction(
        conn,
        user_id=user_id,
        account_id=white_id,
        time=T + timedelta(seconds=1),
        amount_cents=5000,
        mcc=MCC_TRANSFER,
        direction="income",
        counterparty_iban=None,
        # Implies source is white card — but expense is on black card → mismatch
        description="З Білої картки",
    )

    event = make_event(
        user_id=user_id,
        account_id=black_id,
        time=T,
        mcc=MCC_TRANSFER,
        amount_cents=5000,
        direction="expense",
        counterparty_iban=None,
        description="Переказ на картку",
    )

    result = await transfer_service.detect_and_pair(conn, event)

    assert result.special_category is None
    assert result.related_transaction_id is None
    assert len(result.anomalies) == 1
    assert result.anomalies[0].reason_code == "description_account_mismatch"
    assert partner_id in result.anomalies[0].candidate_ids


async def test_amounts_match_two_valid_candidates_ambiguous_amount_match(
    conn, transfer_service
):
    """§125: Black expense at T; two white accounts both have matching income at T+1s.

    Both validate. → ambiguous_amount_match anomaly.
    """
    user_id = await insert_user(conn)
    black_id = await insert_account(
        conn,
        user_id=user_id,
        type="black",
        currency_code="UAH",
        iban=None,
    )
    white1_id = await insert_account(
        conn,
        user_id=user_id,
        type="white",
        currency_code="UAH",
        iban=None,
    )
    white2_id = await insert_account(
        conn,
        user_id=user_id,
        type="white",
        currency_code="UAH",
        iban=None,
    )

    p1 = await insert_transaction(
        conn,
        user_id=user_id,
        account_id=white1_id,
        time=T + timedelta(seconds=1),
        amount_cents=5000,
        mcc=MCC_TRANSFER,
        direction="income",
        counterparty_iban=None,
        description="З Чорної картки",
    )
    p2 = await insert_transaction(
        conn,
        user_id=user_id,
        account_id=white2_id,
        time=T + timedelta(seconds=1),
        amount_cents=5000,
        mcc=MCC_TRANSFER,
        direction="income",
        counterparty_iban=None,
        description="З Чорної картки",
    )

    event = make_event(
        user_id=user_id,
        account_id=black_id,
        time=T,
        mcc=MCC_TRANSFER,
        amount_cents=5000,
        direction="expense",
        counterparty_iban=None,
        description="Переказ на картку",
    )

    result = await transfer_service.detect_and_pair(conn, event)

    assert result.special_category is None
    assert result.related_transaction_id is None
    assert len(result.anomalies) == 1
    assert result.anomalies[0].reason_code == "ambiguous_amount_match"
    assert set(result.anomalies[0].candidate_ids) == {p1, p2}


# ---------------------------------------------------------------------------
# §13.2 FX card→card
# ---------------------------------------------------------------------------


async def test_fx_uah_to_eur_operation_amount_cross_match(conn, transfer_service):
    """§126: UAH→EUR transfer: operation_amount_cents cross-matches income amount_cents.

    UAH black expense: amount=78400 UAH, operation_amount=1750 EUR.
    EUR black income:  amount=1750 EUR, operation_amount=78400 UAH.
    Tier C: SELECT WHERE amount_cents = incoming.operation_amount_cents (1750). Match.
    """
    user_id = await insert_user(conn)
    uah_black_id = await insert_account(
        conn,
        user_id=user_id,
        type="black",
        currency_code="UAH",
        iban=None,
    )
    eur_black_id = await insert_account(
        conn,
        user_id=user_id,
        type="black",
        currency_code="EUR",
        iban=None,
    )

    partner_id = await insert_transaction(
        conn,
        user_id=user_id,
        account_id=eur_black_id,
        time=T + timedelta(seconds=1),
        amount_cents=1750,
        operation_amount_cents=78400,
        mcc=MCC_TRANSFER,
        direction="income",
        counterparty_iban=None,
        currency_code="EUR",
        operation_currency_code="UAH",
        description="З Чорної картки",
    )

    new_tx_id = uuid4()
    event = make_event(
        id=new_tx_id,
        user_id=user_id,
        account_id=uah_black_id,
        time=T,
        mcc=MCC_TRANSFER,
        amount_cents=78400,
        operation_amount_cents=1750,
        operation_currency_code="EUR",
        direction="expense",
        counterparty_iban=None,
        description="Переказ на картку",
    )

    result = await transfer_service.detect_and_pair(conn, event)

    assert result.special_category == "transfer"
    assert result.related_transaction_id == partner_id
    assert result.anomalies == []


# ---------------------------------------------------------------------------
# §13.3 Same-account exclusion
# ---------------------------------------------------------------------------


async def test_same_account_excluded_from_tier_c(conn, transfer_service):
    """§128: Two transactions on the same account cannot pair via Tier C.

    Even if amounts, time, and descriptions match — same account_id is excluded.
    """
    user_id = await insert_user(conn)
    black_id = await insert_account(
        conn,
        user_id=user_id,
        type="black",
        currency_code="UAH",
        iban=None,
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
        description="З Чорної картки",
    )

    event = make_event(
        user_id=user_id,
        account_id=black_id,
        time=T,
        mcc=MCC_TRANSFER,
        amount_cents=5000,
        direction="expense",
        counterparty_iban=None,
        description="Переказ на картку",
    )

    result = await transfer_service.detect_and_pair(conn, event)

    assert result.special_category is None
    assert result.related_transaction_id is None


# ---------------------------------------------------------------------------
# §13.4 Edge cases
# ---------------------------------------------------------------------------


async def test_candidate_with_non_null_iban_excluded_from_tier_c_index(
    conn, transfer_service
):
    """§129: Tier C partial index excludes rows with counterparty_iban IS NOT NULL.

    Incoming tx has no IBAN, candidate has a non-NULL counterparty_iban.
    Candidate is NOT in the Tier C index → not found.
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
        iban="UA555000000000000000000000005",
    )

    await insert_transaction(
        conn,
        user_id=user_id,
        account_id=white_id,
        time=T + timedelta(seconds=1),
        amount_cents=5000,
        mcc=MCC_TRANSFER,
        direction="income",
        # non-NULL counterparty_iban excludes this from Tier C index
        counterparty_iban="UA111000000000000000000000001",
        description="З Чорної картки",
    )

    event = make_event(
        user_id=user_id,
        account_id=black_id,
        time=T,
        mcc=MCC_TRANSFER,
        amount_cents=5000,
        direction="expense",
        counterparty_iban=None,
        description="Переказ на картку",
    )

    result = await transfer_service.detect_and_pair(conn, event)

    # Tier C finds no candidate. "Переказ на картку" is a known description,
    # no "З "/"На " prefix for unpaired anomaly logic — actually it does NOT
    # start with "З " or "На " but parse_description returns a non-None constraint.
    # Since constraint is not None, Tier C attempts the query but finds nothing.
    # No candidates at all → unpaired_to_description (expense with known desc).
    assert result.related_transaction_id is None


async def test_person_name_description_no_tier_c_no_anomaly(conn, transfer_service):
    """§130: External P2P — person name in description. Tier C skipped. No anomaly."""
    user_id = await insert_user(conn)
    black_id = await insert_account(
        conn,
        user_id=user_id,
        type="black",
        currency_code="UAH",
        iban=None,
    )

    event = make_event(
        user_id=user_id,
        account_id=black_id,
        time=T,
        mcc=MCC_TRANSFER,
        amount_cents=5000,
        direction="expense",
        counterparty_iban=None,
        description="Олена К.",
    )

    result = await transfer_service.detect_and_pair(conn, event)

    assert result.special_category is None
    assert result.related_transaction_id is None
    assert result.anomalies == []


async def test_unrecognized_z_prefix_unpaired_from_description(conn, transfer_service):
    """§131: Income desc "З нового рахунку" — "З " prefix but not in known patterns.

    No Tier C attempt. Anomaly unpaired_from_description recorded.
    """
    user_id = await insert_user(conn)
    black_id = await insert_account(
        conn,
        user_id=user_id,
        type="black",
        currency_code="UAH",
        iban=None,
    )

    event = make_event(
        user_id=user_id,
        account_id=black_id,
        time=T,
        mcc=MCC_TRANSFER,
        amount_cents=5000,
        direction="income",
        counterparty_iban=None,
        description="З нового рахунку",
    )

    result = await transfer_service.detect_and_pair(conn, event)

    assert result.special_category is None
    assert result.related_transaction_id is None
    assert len(result.anomalies) == 1
    assert result.anomalies[0].reason_code == "unpaired_from_description"
    assert result.anomalies[0].candidate_ids == []


async def test_unrecognized_na_prefix_unpaired_to_description(conn, transfer_service):
    """§132: Expense desc "На невідому картку" — "На " prefix but not in known patterns.

    No Tier C attempt. Anomaly unpaired_to_description recorded.
    """
    user_id = await insert_user(conn)
    black_id = await insert_account(
        conn,
        user_id=user_id,
        type="black",
        currency_code="UAH",
        iban=None,
    )

    event = make_event(
        user_id=user_id,
        account_id=black_id,
        time=T,
        mcc=MCC_TRANSFER,
        amount_cents=5000,
        direction="expense",
        counterparty_iban=None,
        description="На невідому картку",
    )

    result = await transfer_service.detect_and_pair(conn, event)

    assert result.special_category is None
    assert result.related_transaction_id is None
    assert len(result.anomalies) == 1
    assert result.anomalies[0].reason_code == "unpaired_to_description"
    assert result.anomalies[0].candidate_ids == []


async def test_different_user_account_not_matched_by_tier_c(conn, transfer_service):
    """§133: Amount match within window but candidate is on a different user's account.

    Tier C filters by user_id. Cross-user candidates are excluded.
    """
    user1_id = await insert_user(conn)
    user2_id = await insert_user(conn)

    black1_id = await insert_account(
        conn,
        user_id=user1_id,
        type="black",
        currency_code="UAH",
        iban=None,
    )
    white2_id = await insert_account(
        conn,
        user_id=user2_id,
        type="white",
        currency_code="UAH",
        iban=None,
    )

    # Candidate belongs to user2
    await insert_transaction(
        conn,
        user_id=user2_id,
        account_id=white2_id,
        time=T + timedelta(seconds=1),
        amount_cents=5000,
        mcc=MCC_TRANSFER,
        direction="income",
        counterparty_iban=None,
        description="З Чорної картки",
    )

    event = make_event(
        user_id=user1_id,
        account_id=black1_id,
        time=T,
        mcc=MCC_TRANSFER,
        amount_cents=5000,
        direction="expense",
        counterparty_iban=None,
        description="Переказ на картку",
    )

    result = await transfer_service.detect_and_pair(conn, event)

    assert result.special_category is None
    assert result.related_transaction_id is None


# ---------------------------------------------------------------------------
# §13.5 Zero-amount transactions
# ---------------------------------------------------------------------------


async def test_zero_amount_check_transactions_do_not_false_match(
    conn, transfer_service
):
    """§134: MCC 4829 + amount=0 (card verification hold) — raw_type='check'.

    Tier C requires opposite type (income vs expense). 'check' doesn't qualify.
    Both insert normally. No pairing.
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

    # Pre-existing check on white card
    await insert_transaction(
        conn,
        user_id=user_id,
        account_id=white_id,
        time=T + timedelta(seconds=1),
        amount_cents=0,
        mcc=MCC_TRANSFER,
        direction="zero",
        counterparty_iban=None,
        description=None,
    )

    # Incoming check on black card
    event = make_event(
        user_id=user_id,
        account_id=black_id,
        time=T,
        mcc=MCC_TRANSFER,
        amount_cents=0,
        direction="zero",
        counterparty_iban=None,
        description=None,
    )

    result = await transfer_service.detect_and_pair(conn, event)

    # MCC 4829 passes the MCC filter, but _opposite_direction('zero') → 'zero',
    # and the candidate is also 'zero' — they are the same direction, not opposite.
    # Actually: _opposite_direction('zero') returns 'zero', so the query looks for
    # direction = 'zero'. The candidate IS 'zero'. BUT
    # description is None → parse_description returns None → not a known
    # transfer description → Tier C is skipped entirely before the query.
    # No anomaly (no "З "/"На " prefix on None description).
    assert result.special_category is None
    assert result.related_transaction_id is None
    assert result.anomalies == []
