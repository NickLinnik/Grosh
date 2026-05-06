"""Integration tests for transfer detection Tier B (reverse IBAN) against real Postgres.

§12 of the transfer detection test specification.
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


# ---------------------------------------------------------------------------
# §12 Basic pairing
# ---------------------------------------------------------------------------


async def test_card_income_tier_b_pairs_with_fop_expense(conn, transfer_service):
    """§118: Card income has no IBAN; existing FOP expense has counterparty_iban = card.

    Tier A skipped (no IBAN on incoming). Tier B finds FOP expense. Paired.
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
        account_id=fop_id,
        time=T - timedelta(seconds=1),
        amount_cents=5000,
        mcc=MCC_TRANSFER,
        direction="expense",
        counterparty_iban="UA444000000000000000000000004",
        description="На чорну картку",
    )

    new_tx_id = uuid4()
    event = make_event(
        id=new_tx_id,
        user_id=user_id,
        account_id=black_id,
        time=T,
        mcc=MCC_TRANSFER,
        amount_cents=5000,
        direction="income",
        counterparty_iban=None,
        description="З гривневого рахунку ФОП",
    )

    result = await transfer_service.detect_and_pair(conn, event)

    assert result.special_category == "transfer"
    assert result.related_transaction_id == partner_id
    assert result.anomalies == []

    partner_row = await get_transaction(conn, partner_id)
    assert str(partner_row["special_category"]) == "transfer"
    assert partner_row["related_transaction_id"] == new_tx_id


async def test_multiple_fop_expenses_pointing_at_card_ambiguous_reverse_iban(
    conn, transfer_service
):
    """§119: Two FOP expenses from different accounts both point at card IBAN.

    Card income triggers Tier B. Ambiguity → ambiguous_reverse_iban anomaly.
    """
    user_id = await insert_user(conn)
    fop1_id = await insert_account(
        conn,
        user_id=user_id,
        type="fop",
        currency_code="UAH",
        iban="UA111000000000000000000000001",
    )
    fop2_id = await insert_account(
        conn,
        user_id=user_id,
        type="fop",
        currency_code="UAH",
        iban="UA222000000000000000000000002",
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
        account_id=fop1_id,
        time=T - timedelta(seconds=1),
        amount_cents=5000,
        mcc=MCC_TRANSFER,
        direction="expense",
        counterparty_iban="UA444000000000000000000000004",
        description="На чорну картку",
    )
    p2 = await insert_transaction(
        conn,
        user_id=user_id,
        account_id=fop2_id,
        time=T - timedelta(seconds=1),
        amount_cents=5000,
        mcc=MCC_TRANSFER,
        direction="expense",
        counterparty_iban="UA444000000000000000000000004",
        description="На чорну картку",
    )

    event = make_event(
        user_id=user_id,
        account_id=black_id,
        time=T,
        mcc=MCC_TRANSFER,
        amount_cents=5000,
        direction="income",
        counterparty_iban=None,
        description="З гривневого рахунку ФОП",
    )

    result = await transfer_service.detect_and_pair(conn, event)

    assert result.special_category is None
    assert result.related_transaction_id is None
    assert len(result.anomalies) == 1
    anomaly = result.anomalies[0]
    assert anomaly.reason_code == "ambiguous_reverse_iban"
    assert set(anomaly.candidate_ids) == {p1, p2}


async def test_tier_b_partner_outside_time_window_no_match(conn, transfer_service):
    """§120: FOP expense pointing at card is outside ±2s — no Tier B match."""
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
        account_id=fop_id,
        time=T - timedelta(seconds=5),
        amount_cents=5000,
        mcc=MCC_TRANSFER,
        direction="expense",
        counterparty_iban="UA444000000000000000000000004",
        description="На чорну картку",
    )

    event = make_event(
        user_id=user_id,
        account_id=black_id,
        time=T,
        mcc=MCC_TRANSFER,
        amount_cents=5000,
        direction="income",
        counterparty_iban=None,
        description="З гривневого рахунку ФОП",
    )

    result = await transfer_service.detect_and_pair(conn, event)

    # Falls through Tier B (no match) and into Tier C.
    # Tier C: known description "З гривневого рахунку ФОП" but no amount candidate
    # (FOP expense has counterparty_iban != NULL so excluded from Tier C index).
    # So result is unpaired_from_description anomaly from Tier C.
    assert result.special_category is None
    assert result.related_transaction_id is None


async def test_tier_b_consistent_descriptions_no_anomaly(conn, transfer_service):
    """§121: Tier B pair with consistent descriptions → no anomaly."""
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
        account_id=fop_id,
        time=T - timedelta(seconds=1),
        amount_cents=5000,
        mcc=MCC_TRANSFER,
        direction="expense",
        counterparty_iban="UA444000000000000000000000004",
        description="На чорну картку",
    )

    event = make_event(
        user_id=user_id,
        account_id=black_id,
        time=T,
        mcc=MCC_TRANSFER,
        amount_cents=5000,
        direction="income",
        counterparty_iban=None,
        description="З гривневого рахунку ФОП",
    )

    result = await transfer_service.detect_and_pair(conn, event)

    assert result.special_category == "transfer"
    assert result.anomalies == []

    assert await count_anomalies(conn, user_id) == 0


async def test_tier_b_inconsistent_descriptions_paired_with_canary(
    conn, transfer_service
):
    """§122: Tier B pair with inconsistent descriptions → paired + canary anomaly.

    Card income desc "З Білої картки" but FOP expense account is type=fop, not white.
    IBAN-based reverse match is still valid (Tier B is IBAN-confirmed).
    Pair proceeds, description_consistency_mismatch anomaly recorded.
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
        account_id=fop_id,
        time=T - timedelta(seconds=1),
        amount_cents=5000,
        mcc=MCC_TRANSFER,
        direction="expense",
        counterparty_iban="UA444000000000000000000000004",
        description="На чорну картку",
    )

    new_tx_id = uuid4()
    event = make_event(
        id=new_tx_id,
        user_id=user_id,
        account_id=black_id,
        time=T,
        mcc=MCC_TRANSFER,
        amount_cents=5000,
        direction="income",
        counterparty_iban=None,
        # "З Білої картки" implies the expense account is type=white,
        # but the FOP account is type=fop — mismatch.
        description="З Білої картки",
    )

    result = await transfer_service.detect_and_pair(conn, event)

    assert result.special_category == "transfer"
    assert result.related_transaction_id == partner_id
    assert len(result.anomalies) == 1
    assert result.anomalies[0].reason_code == "description_consistency_mismatch"
    assert result.anomalies[0].candidate_ids == [partner_id]
