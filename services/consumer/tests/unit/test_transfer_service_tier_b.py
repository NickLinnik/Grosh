"""Unit tests — Tier B (reverse IBAN) — §5 (tests 72-78)."""

from uuid import uuid4

import pytest

from grosh_consumer.sources.monobank.transfer import MonobankTransferDetection
from tests.helpers import make_event
from tests.unit.transfer_fixtures import (
    MCC_TRANSFER,
    T_MINUS_1S,
    T_MINUS_5S,
    UAH_BLACK_IBAN,
    USER_ID,
    T,
    seed_accounts,
)

pytestmark = pytest.mark.asyncio


def make_service(tx_repo, acc_repo, anomaly_repo):
    return MonobankTransferDetection(
        transaction_repo=tx_repo,
        account_repo=acc_repo,
        anomaly_repo=anomaly_repo,
    )


# ---------------------------------------------------------------------------
# §5.1  Reverse IBAN lookup
# ---------------------------------------------------------------------------


async def test_72_reverse_iban_finds_partner_and_pairs(tx_repo, acc_repo, anomaly_repo):
    """Card income (no IBAN) pairs with existing FOP expense pointing at card IBAN."""
    accounts = seed_accounts(acc_repo)

    # FOP expense already stored with counterparty_iban = UAH_BLACK.iban
    partner = tx_repo.add_transaction(
        user_id=USER_ID,
        account_id=accounts["uah_fop"]["id"],
        direction="expense",
        mcc=MCC_TRANSFER,
        time=T_MINUS_1S,
        counterparty_iban=UAH_BLACK_IBAN,
    )

    # Card income arrives — no counterparty_iban on it
    tx = make_event(
        user_id=USER_ID,
        account_id=accounts["uah_black"]["id"],
        direction="income",
        mcc=MCC_TRANSFER,
        counterparty_iban=None,
        time=T,
    )
    result = await make_service(tx_repo, acc_repo, anomaly_repo).detect_and_pair(
        None, tx
    )

    assert result.special_category == "transfer"
    assert result.related_transaction_id == partner["id"]


async def test_73_two_expenses_point_at_card_iban_ambiguous(
    tx_repo, acc_repo, anomaly_repo
):
    """Two unclaimed FOP expenses both have counterparty_iban pointing at black card."""
    accounts = seed_accounts(acc_repo)

    p1 = tx_repo.add_transaction(
        user_id=USER_ID,
        account_id=accounts["uah_fop"]["id"],
        direction="expense",
        mcc=MCC_TRANSFER,
        time=T_MINUS_1S,
        counterparty_iban=UAH_BLACK_IBAN,
    )
    p2 = tx_repo.add_transaction(
        user_id=USER_ID,
        account_id=accounts["usd_fop"]["id"],
        direction="expense",
        mcc=MCC_TRANSFER,
        time=T_MINUS_1S,
        counterparty_iban=UAH_BLACK_IBAN,
    )

    tx = make_event(
        user_id=USER_ID,
        account_id=accounts["uah_black"]["id"],
        direction="income",
        mcc=MCC_TRANSFER,
        counterparty_iban=None,
        time=T,
    )
    result = await make_service(tx_repo, acc_repo, anomaly_repo).detect_and_pair(
        None, tx
    )

    assert result.special_category is None
    assert result.related_transaction_id is None
    assert len(result.anomalies) == 1
    assert result.anomalies[0].reason_code == "ambiguous_reverse_iban"
    assert set(result.anomalies[0].candidate_ids) == {p1["id"], p2["id"]}


async def test_74_already_claimed_expense_skipped_falls_through(
    tx_repo, acc_repo, anomaly_repo
):
    """Existing expense with matching counterparty_iban is already claimed
    — not a Tier B candidate."""
    accounts = seed_accounts(acc_repo)

    # This expense is already claimed
    tx_repo.add_transaction(
        user_id=USER_ID,
        account_id=accounts["uah_fop"]["id"],
        direction="expense",
        special_category="transfer",
        mcc=MCC_TRANSFER,
        time=T_MINUS_1S,
        counterparty_iban=UAH_BLACK_IBAN,
        related_transaction_id=uuid4(),
    )

    # Card income with no IBAN — Tier B finds nothing, falls through to Tier C
    tx = make_event(
        user_id=USER_ID,
        account_id=accounts["uah_black"]["id"],
        direction="income",
        mcc=MCC_TRANSFER,
        counterparty_iban=None,
        time=T,
    )
    result = await make_service(tx_repo, acc_repo, anomaly_repo).detect_and_pair(
        None, tx
    )

    # Tier C also won't match (no description or amount match set up)
    assert result.special_category is None
    assert result.related_transaction_id is None


async def test_75_same_type_with_iban_not_a_tier_b_candidate(
    tx_repo, acc_repo, anomaly_repo
):
    """Tier B requires opposite direction — same-direction tx is excluded."""
    accounts = seed_accounts(acc_repo)

    # This is income (same direction as incoming) — should not match
    tx_repo.add_transaction(
        user_id=USER_ID,
        account_id=accounts["uah_fop"]["id"],
        direction="income",
        mcc=MCC_TRANSFER,
        time=T_MINUS_1S,
        counterparty_iban=UAH_BLACK_IBAN,
    )

    tx = make_event(
        user_id=USER_ID,
        account_id=accounts["uah_black"]["id"],
        direction="income",
        mcc=MCC_TRANSFER,
        counterparty_iban=None,
        time=T,
    )
    result = await make_service(tx_repo, acc_repo, anomaly_repo).detect_and_pair(
        None, tx
    )
    assert result.special_category is None
    assert result.related_transaction_id is None


async def test_76_tier_b_candidate_outside_time_window_no_match(
    tx_repo, acc_repo, anomaly_repo
):
    """Existing expense with matching IBAN but at T-5s — outside ±2s window."""
    accounts = seed_accounts(acc_repo)

    tx_repo.add_transaction(
        user_id=USER_ID,
        account_id=accounts["uah_fop"]["id"],
        direction="expense",
        mcc=MCC_TRANSFER,
        time=T_MINUS_5S,
        counterparty_iban=UAH_BLACK_IBAN,
    )

    tx = make_event(
        user_id=USER_ID,
        account_id=accounts["uah_black"]["id"],
        direction="income",
        mcc=MCC_TRANSFER,
        counterparty_iban=None,
        time=T,
    )
    result = await make_service(tx_repo, acc_repo, anomaly_repo).detect_and_pair(
        None, tx
    )
    assert result.special_category is None
    assert result.related_transaction_id is None


# ---------------------------------------------------------------------------
# §5.2  Description validation (canary on Tier B)
# ---------------------------------------------------------------------------


async def test_77_tier_b_consistent_descriptions_no_anomaly(
    tx_repo, acc_repo, anomaly_repo
):
    """Tier B pair with consistent descriptions — paired, zero anomalies."""
    accounts = seed_accounts(acc_repo)

    partner = tx_repo.add_transaction(
        user_id=USER_ID,
        account_id=accounts["uah_fop"]["id"],
        direction="expense",
        mcc=MCC_TRANSFER,
        time=T_MINUS_1S,
        counterparty_iban=UAH_BLACK_IBAN,
        description="На чорну картку",
    )

    tx = make_event(
        user_id=USER_ID,
        account_id=accounts["uah_black"]["id"],
        direction="income",
        mcc=MCC_TRANSFER,
        counterparty_iban=None,
        description="З гривневого рахунку ФОП",
        time=T,
    )
    result = await make_service(tx_repo, acc_repo, anomaly_repo).detect_and_pair(
        None, tx
    )

    assert result.special_category == "transfer"
    assert result.related_transaction_id == partner["id"]
    assert result.anomalies == []


async def test_78_tier_b_inconsistent_descriptions_paired_with_canary(
    tx_repo, acc_repo, anomaly_repo
):
    """Tier B pair with inconsistent descriptions — paired +
    description_consistency_mismatch."""
    accounts = seed_accounts(acc_repo)

    # Partner expense says "На білу картку" but income account is black
    partner = tx_repo.add_transaction(
        user_id=USER_ID,
        account_id=accounts["uah_fop"]["id"],
        direction="expense",
        mcc=MCC_TRANSFER,
        time=T_MINUS_1S,
        counterparty_iban=UAH_BLACK_IBAN,
        description="На білу картку",
    )

    tx = make_event(
        user_id=USER_ID,
        account_id=accounts["uah_black"]["id"],
        direction="income",
        mcc=MCC_TRANSFER,
        counterparty_iban=None,
        description="З гривневого рахунку ФОП",
        time=T,
    )
    result = await make_service(tx_repo, acc_repo, anomaly_repo).detect_and_pair(
        None, tx
    )

    assert result.special_category == "transfer"
    assert result.related_transaction_id == partner["id"]
    assert len(result.anomalies) == 1
    assert result.anomalies[0].reason_code == "description_consistency_mismatch"
