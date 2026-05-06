"""Integration tests for transfer detection Tier A (IBAN match) against real Postgres.

§11 of the transfer detection test specification.
"""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from tests.helpers import make_event
from tests.integration.conftest import (
    count_anomalies,
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
# §11.1 Basic pairing
# ---------------------------------------------------------------------------


async def test_fop_to_card_iban_match_single_partner_paired(conn, transfer_service):
    """§109: FOP expense with IBAN pointing at own black card; income partner exists.

    After detect_and_pair():
    - partner (existing income) gets related_transaction_id = new tx id,
      special_category = 'transfer'.
    - result carries special_category='transfer', related_transaction_id = partner id.
    - zero anomalies.
    The new tx is NOT inserted by detect_and_pair(); tests verify result fields only.
    """
    user_id = await insert_user(conn)
    fop_id = await insert_account(
        conn,
        user_id=user_id,
        type="fop",
        currency_code="UAH",
        iban="UA111000000000000000000000001",
    )
    black_id = await insert_account(
        conn,
        user_id=user_id,
        type="black",
        currency_code="UAH",
        iban="UA444000000000000000000000004",
    )

    partner_id = await insert_transaction(
        conn,
        user_id=user_id,
        account_id=black_id,
        time=T + timedelta(seconds=1),
        amount_cents=5000,
        mcc=MCC_TRANSFER,
        direction="income",
        description="З гривневого рахунку ФОП",
    )

    new_tx_id = uuid4()
    event = make_event(
        id=new_tx_id,
        user_id=user_id,
        account_id=fop_id,
        time=T,
        mcc=MCC_TRANSFER,
        amount_cents=5000,
        direction="expense",
        counterparty_iban="UA444000000000000000000000004",
        description="На чорну картку",
    )

    result = await transfer_service.detect_and_pair(conn, event)

    assert result.special_category == "transfer"
    assert result.related_transaction_id == partner_id
    assert result.anomalies == []

    partner_row = await get_transaction(conn, partner_id)
    assert str(partner_row["special_category"]) == "transfer"
    assert partner_row["related_transaction_id"] == new_tx_id

    assert await count_anomalies(conn, user_id) == 0


async def test_iban_matches_own_account_no_partner_no_anomaly(conn, transfer_service):
    """§110: IBAN points at own account but no partner within ±2s.

    Inserted as expense, no anomaly.
    """
    user_id = await insert_user(conn)
    fop_id = await insert_account(
        conn,
        user_id=user_id,
        type="fop",
        currency_code="UAH",
        iban="UA111000000000000000000000001",
    )
    await insert_account(
        conn,
        user_id=user_id,
        type="black",
        currency_code="UAH",
        iban="UA444000000000000000000000004",
    )

    event = make_event(
        user_id=user_id,
        account_id=fop_id,
        time=T,
        mcc=MCC_TRANSFER,
        amount_cents=5000,
        direction="expense",
        counterparty_iban="UA444000000000000000000000004",
        description="На чорну картку",
    )

    result = await transfer_service.detect_and_pair(conn, event)

    assert result.special_category is None
    assert result.related_transaction_id is None
    assert result.anomalies == []


async def test_iban_partner_at_exactly_plus_2s_paired(conn, transfer_service):
    """§111: Partner at exactly +2s is within window — paired (inclusive boundary)."""
    user_id = await insert_user(conn)
    fop_id = await insert_account(
        conn,
        user_id=user_id,
        type="fop",
        currency_code="UAH",
        iban="UA111000000000000000000000001",
    )
    black_id = await insert_account(
        conn,
        user_id=user_id,
        type="black",
        currency_code="UAH",
        iban="UA444000000000000000000000004",
    )

    partner_id = await insert_transaction(
        conn,
        user_id=user_id,
        account_id=black_id,
        time=T + timedelta(seconds=2),
        amount_cents=5000,
        mcc=MCC_TRANSFER,
        direction="income",
        description="З гривневого рахунку ФОП",
    )

    event = make_event(
        user_id=user_id,
        account_id=fop_id,
        time=T,
        mcc=MCC_TRANSFER,
        amount_cents=5000,
        direction="expense",
        counterparty_iban="UA444000000000000000000000004",
        description="На чорну картку",
    )

    result = await transfer_service.detect_and_pair(conn, event)

    assert result.special_category == "transfer"
    assert result.related_transaction_id == partner_id


async def test_iban_partner_at_2001ms_not_paired(conn, transfer_service):
    """§112: Partner at +2.001s is outside window — not paired (exclusive above 2s)."""
    user_id = await insert_user(conn)
    fop_id = await insert_account(
        conn,
        user_id=user_id,
        type="fop",
        currency_code="UAH",
        iban="UA111000000000000000000000001",
    )
    black_id = await insert_account(
        conn,
        user_id=user_id,
        type="black",
        currency_code="UAH",
        iban="UA444000000000000000000000004",
    )

    await insert_transaction(
        conn,
        user_id=user_id,
        account_id=black_id,
        time=T + timedelta(seconds=2, milliseconds=1),
        amount_cents=5000,
        mcc=MCC_TRANSFER,
        direction="income",
        description="З гривневого рахунку ФОП",
    )

    event = make_event(
        user_id=user_id,
        account_id=fop_id,
        time=T,
        mcc=MCC_TRANSFER,
        amount_cents=5000,
        direction="expense",
        counterparty_iban="UA444000000000000000000000004",
        description="На чорну картку",
    )

    result = await transfer_service.detect_and_pair(conn, event)

    assert result.special_category is None
    assert result.related_transaction_id is None


# ---------------------------------------------------------------------------
# §11.2 Ambiguity
# ---------------------------------------------------------------------------


async def test_two_unclaimed_partners_ambiguous_iban_match(conn, transfer_service):
    """§113: Two unclaimed income partners on target account → ambiguous_iban_match."""
    user_id = await insert_user(conn)
    fop_id = await insert_account(
        conn,
        user_id=user_id,
        type="fop",
        currency_code="UAH",
        iban="UA111000000000000000000000001",
    )
    black_id = await insert_account(
        conn,
        user_id=user_id,
        type="black",
        currency_code="UAH",
        iban="UA444000000000000000000000004",
    )

    p1 = await insert_transaction(
        conn,
        user_id=user_id,
        account_id=black_id,
        time=T + timedelta(seconds=1),
        amount_cents=5000,
        mcc=MCC_TRANSFER,
        direction="income",
        description="З гривневого рахунку ФОП",
    )
    p2 = await insert_transaction(
        conn,
        user_id=user_id,
        account_id=black_id,
        time=T + timedelta(seconds=1),
        amount_cents=5000,
        mcc=MCC_TRANSFER,
        direction="income",
        description="З гривневого рахунку ФОП",
    )

    event = make_event(
        user_id=user_id,
        account_id=fop_id,
        time=T,
        mcc=MCC_TRANSFER,
        amount_cents=5000,
        direction="expense",
        counterparty_iban="UA444000000000000000000000004",
        description="На чорну картку",
    )

    result = await transfer_service.detect_and_pair(conn, event)

    assert result.special_category is None
    assert result.related_transaction_id is None
    assert len(result.anomalies) == 1
    anomaly = result.anomalies[0]
    assert anomaly.reason_code == "ambiguous_iban_match"
    assert set(anomaly.candidate_ids) == {p1, p2}


# ---------------------------------------------------------------------------
# §11.3 Description canary
# ---------------------------------------------------------------------------


async def test_tier_a_consistent_descriptions_no_anomaly(conn, transfer_service):
    """§114: Tier A pair with consistent descriptions → no anomaly recorded."""
    user_id = await insert_user(conn)
    fop_id = await insert_account(
        conn,
        user_id=user_id,
        type="fop",
        currency_code="UAH",
        iban="UA111000000000000000000000001",
    )
    black_id = await insert_account(
        conn,
        user_id=user_id,
        type="black",
        currency_code="UAH",
        iban="UA444000000000000000000000004",
    )

    await insert_transaction(
        conn,
        user_id=user_id,
        account_id=black_id,
        time=T + timedelta(seconds=1),
        amount_cents=5000,
        mcc=MCC_TRANSFER,
        direction="income",
        description="З гривневого рахунку ФОП",
    )

    event = make_event(
        user_id=user_id,
        account_id=fop_id,
        time=T,
        mcc=MCC_TRANSFER,
        amount_cents=5000,
        direction="expense",
        counterparty_iban="UA444000000000000000000000004",
        description="На чорну картку",
    )

    result = await transfer_service.detect_and_pair(conn, event)

    assert result.special_category == "transfer"
    assert result.anomalies == []


async def test_tier_a_inconsistent_descriptions_paired_with_canary(
    conn, transfer_service
):
    """§115: Tier A pair with inconsistent descriptions → paired + canary anomaly.

    Expense says "На білу картку" (white), but IBAN points to black card.
    IBAN is deterministic — pair proceeds. description_consistency_mismatch recorded.
    """
    user_id = await insert_user(conn)
    fop_id = await insert_account(
        conn,
        user_id=user_id,
        type="fop",
        currency_code="UAH",
        iban="UA111000000000000000000000001",
    )
    black_id = await insert_account(
        conn,
        user_id=user_id,
        type="black",
        currency_code="UAH",
        iban="UA444000000000000000000000004",
    )

    partner_id = await insert_transaction(
        conn,
        user_id=user_id,
        account_id=black_id,
        time=T + timedelta(seconds=1),
        amount_cents=5000,
        mcc=MCC_TRANSFER,
        direction="income",
        description="З гривневого рахунку ФОП",
    )

    new_tx_id = uuid4()
    event = make_event(
        id=new_tx_id,
        user_id=user_id,
        account_id=fop_id,
        time=T,
        mcc=MCC_TRANSFER,
        amount_cents=5000,
        direction="expense",
        counterparty_iban="UA444000000000000000000000004",
        description="На білу картку",
    )

    result = await transfer_service.detect_and_pair(conn, event)

    assert result.special_category == "transfer"
    assert result.related_transaction_id == partner_id
    assert len(result.anomalies) == 1
    assert result.anomalies[0].reason_code == "description_consistency_mismatch"
    assert result.anomalies[0].candidate_ids == [partner_id]


# ---------------------------------------------------------------------------
# §11.4 Index utilization (filtering)
# ---------------------------------------------------------------------------


async def test_only_mcc_4829_transactions_are_candidates(conn, transfer_service):
    """§116: Partner with MCC 5411 on target account is not a candidate."""
    user_id = await insert_user(conn)
    fop_id = await insert_account(
        conn,
        user_id=user_id,
        type="fop",
        currency_code="UAH",
        iban="UA111000000000000000000000001",
    )
    black_id = await insert_account(
        conn,
        user_id=user_id,
        type="black",
        currency_code="UAH",
        iban="UA444000000000000000000000004",
    )

    await insert_transaction(
        conn,
        user_id=user_id,
        account_id=black_id,
        time=T + timedelta(seconds=1),
        amount_cents=5000,
        mcc=MCC_GROCERY,
        direction="income",
        description="З гривневого рахунку ФОП",
    )

    event = make_event(
        user_id=user_id,
        account_id=fop_id,
        time=T,
        mcc=MCC_TRANSFER,
        amount_cents=5000,
        direction="expense",
        counterparty_iban="UA444000000000000000000000004",
        description="На чорну картку",
    )

    result = await transfer_service.detect_and_pair(conn, event)

    assert result.special_category is None
    assert result.related_transaction_id is None
    assert result.anomalies == []


async def test_already_claimed_partner_excluded(conn, transfer_service):
    """§117: Partner with related_transaction_id already set is excluded."""
    user_id = await insert_user(conn)
    fop_id = await insert_account(
        conn,
        user_id=user_id,
        type="fop",
        currency_code="UAH",
        iban="UA111000000000000000000000001",
    )
    black_id = await insert_account(
        conn,
        user_id=user_id,
        type="black",
        currency_code="UAH",
        iban="UA444000000000000000000000004",
    )

    # Insert a dummy transaction to serve as the existing pair
    dummy_id = await insert_transaction(
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
        time=T + timedelta(seconds=1),
        amount_cents=5000,
        mcc=MCC_TRANSFER,
        direction="income",
        related_transaction_id=dummy_id,
        special_category="transfer",
    )

    event = make_event(
        user_id=user_id,
        account_id=fop_id,
        time=T,
        mcc=MCC_TRANSFER,
        amount_cents=5000,
        direction="expense",
        counterparty_iban="UA444000000000000000000000004",
        description="На чорну картку",
    )

    result = await transfer_service.detect_and_pair(conn, event)

    assert result.special_category is None
    assert result.related_transaction_id is None
