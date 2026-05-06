"""Unit tests — Tier A (IBAN match) — §4 (tests 53-71)."""

import pytest

from grosh_consumer.sources.monobank.transfer import MonobankTransferDetection
from tests.helpers import make_event
from tests.unit.transfer_fixtures import (
    MCC_GROCERY,
    MCC_TRANSFER,
    T_MINUS_1S,
    T_MINUS_2S,
    T_MINUS_3S,
    T_PLUS_1S,
    T_PLUS_2S,
    T_PLUS_3S,
    UAH_BLACK_IBAN,
    UAH_FOP_IBAN,
    USD_FOP_IBAN,
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
# §4.1  Basic IBAN matching
# ---------------------------------------------------------------------------


async def test_53_expense_with_own_iban_partner_exists_pairs(
    tx_repo, acc_repo, anomaly_repo
):
    accounts = seed_accounts(acc_repo)
    partner = tx_repo.add_transaction(
        user_id=USER_ID,
        account_id=accounts["uah_black"]["id"],
        direction="income",
        mcc=MCC_TRANSFER,
        time=T_PLUS_1S,
        amount_cents=5000,
        operation_amount_cents=5000,
    )

    tx = make_event(
        user_id=USER_ID,
        account_id=accounts["uah_fop"]["id"],
        direction="expense",
        mcc=MCC_TRANSFER,
        counterparty_iban=UAH_BLACK_IBAN,
        amount_cents=5000,
        operation_amount_cents=5000,
        time=T,
    )
    result = await make_service(tx_repo, acc_repo, anomaly_repo).detect_and_pair(
        None, tx
    )

    assert result.special_category == "transfer"
    assert result.related_transaction_id == partner["id"]
    assert tx_repo.get_by_id(partner["id"])["special_category"] == "transfer"
    assert tx_repo.get_by_id(partner["id"])["related_transaction_id"] == tx.id


async def test_54_expense_with_own_iban_no_partner_inserts_as_expense(
    tx_repo, acc_repo, anomaly_repo
):
    accounts = seed_accounts(acc_repo)
    tx = make_event(
        user_id=USER_ID,
        account_id=accounts["uah_fop"]["id"],
        direction="expense",
        mcc=MCC_TRANSFER,
        counterparty_iban=UAH_BLACK_IBAN,
        time=T,
    )
    result = await make_service(tx_repo, acc_repo, anomaly_repo).detect_and_pair(
        None, tx
    )

    assert result.special_category is None
    assert result.related_transaction_id is None
    assert result.anomalies == []


async def test_54b_tier_a_fallthrough_to_tier_b_on_zero_candidates(
    tx_repo, acc_repo, anomaly_repo
):
    """Tier A finds 0 candidates on target account → falls through to Tier B.

    Simulates the 3-hop transfer: USD FOP expense has counterparty_iban
    pointing to the black card (final destination), but the actual partner
    is on UAH FOP (findable via Tier B reverse IBAN lookup).
    """
    accounts = seed_accounts(acc_repo)

    # UAH FOP income with counterparty_iban pointing at USD FOP — Tier B candidate
    partner = tx_repo.add_transaction(
        user_id=USER_ID,
        account_id=accounts["uah_fop"]["id"],
        direction="income",
        mcc=MCC_TRANSFER,
        time=T_MINUS_1S,
        amount_cents=4390000,
        counterparty_iban=USD_FOP_IBAN,
        description="З доларового рахунку ФОП",
    )

    # USD FOP expense — counterparty_iban points to black card (wrong hop)
    tx = make_event(
        user_id=USER_ID,
        account_id=accounts["usd_fop"]["id"],
        direction="expense",
        mcc=MCC_TRANSFER,
        counterparty_iban=UAH_BLACK_IBAN,
        time=T,
        amount_cents=100000,
        operation_amount_cents=4390000,
        description="На гривневий рахунок ФОП",
    )

    result = await make_service(tx_repo, acc_repo, anomaly_repo).detect_and_pair(
        None, tx
    )

    # Tier A: black card is own account, 0 unclaimed income → fallthrough
    # Tier B: UAH FOP income has counterparty_iban = USD_FOP.iban → match
    assert result.special_category == "transfer"
    assert result.related_transaction_id == partner["id"]


async def test_55_income_with_own_iban_partner_exists_pairs(
    tx_repo, acc_repo, anomaly_repo
):
    accounts = seed_accounts(acc_repo)
    partner = tx_repo.add_transaction(
        user_id=USER_ID,
        account_id=accounts["uah_fop"]["id"],
        direction="expense",
        mcc=MCC_TRANSFER,
        time=T_MINUS_1S,
        amount_cents=5000,
        operation_amount_cents=5000,
    )

    tx = make_event(
        user_id=USER_ID,
        account_id=accounts["usd_fop"]["id"],
        direction="income",
        mcc=MCC_TRANSFER,
        counterparty_iban=UAH_FOP_IBAN,
        amount_cents=5000,
        operation_amount_cents=5000,
        time=T,
    )
    result = await make_service(tx_repo, acc_repo, anomaly_repo).detect_and_pair(
        None, tx
    )

    assert result.special_category == "transfer"
    assert result.related_transaction_id == partner["id"]


async def test_56_external_iban_skips_tier_a_falls_through(
    tx_repo, acc_repo, anomaly_repo
):
    accounts = seed_accounts(acc_repo)
    tx = make_event(
        user_id=USER_ID,
        account_id=accounts["uah_fop"]["id"],
        direction="expense",
        mcc=MCC_TRANSFER,
        counterparty_iban="UA999000000000000000000000099",
        time=T,
    )
    result = await make_service(tx_repo, acc_repo, anomaly_repo).detect_and_pair(
        None, tx
    )

    # External IBAN → not a transfer
    assert result.special_category is None
    assert result.related_transaction_id is None


# ---------------------------------------------------------------------------
# §4.2  Time window enforcement
# ---------------------------------------------------------------------------


async def test_57_partner_at_plus_2s_paired(tx_repo, acc_repo, anomaly_repo):
    accounts = seed_accounts(acc_repo)
    partner = tx_repo.add_transaction(
        user_id=USER_ID,
        account_id=accounts["uah_black"]["id"],
        direction="income",
        mcc=MCC_TRANSFER,
        time=T_PLUS_2S,
    )
    tx = make_event(
        user_id=USER_ID,
        account_id=accounts["uah_fop"]["id"],
        direction="expense",
        mcc=MCC_TRANSFER,
        counterparty_iban=UAH_BLACK_IBAN,
        time=T,
    )
    result = await make_service(tx_repo, acc_repo, anomaly_repo).detect_and_pair(
        None, tx
    )
    assert result.special_category == "transfer"
    assert result.related_transaction_id == partner["id"]


async def test_58_partner_at_minus_2s_paired(tx_repo, acc_repo, anomaly_repo):
    accounts = seed_accounts(acc_repo)
    partner = tx_repo.add_transaction(
        user_id=USER_ID,
        account_id=accounts["uah_black"]["id"],
        direction="income",
        mcc=MCC_TRANSFER,
        time=T_MINUS_2S,
    )
    tx = make_event(
        user_id=USER_ID,
        account_id=accounts["uah_fop"]["id"],
        direction="expense",
        mcc=MCC_TRANSFER,
        counterparty_iban=UAH_BLACK_IBAN,
        time=T,
    )
    result = await make_service(tx_repo, acc_repo, anomaly_repo).detect_and_pair(
        None, tx
    )
    assert result.special_category == "transfer"
    assert result.related_transaction_id == partner["id"]


async def test_59_partner_at_plus_3s_no_match(tx_repo, acc_repo, anomaly_repo):
    accounts = seed_accounts(acc_repo)
    tx_repo.add_transaction(
        user_id=USER_ID,
        account_id=accounts["uah_black"]["id"],
        direction="income",
        mcc=MCC_TRANSFER,
        time=T_PLUS_3S,
    )
    tx = make_event(
        user_id=USER_ID,
        account_id=accounts["uah_fop"]["id"],
        direction="expense",
        mcc=MCC_TRANSFER,
        counterparty_iban=UAH_BLACK_IBAN,
        time=T,
    )
    result = await make_service(tx_repo, acc_repo, anomaly_repo).detect_and_pair(
        None, tx
    )
    assert result.special_category is None
    assert result.related_transaction_id is None


async def test_60_partner_at_minus_3s_no_match(tx_repo, acc_repo, anomaly_repo):
    accounts = seed_accounts(acc_repo)
    tx_repo.add_transaction(
        user_id=USER_ID,
        account_id=accounts["uah_black"]["id"],
        direction="income",
        mcc=MCC_TRANSFER,
        time=T_MINUS_3S,
    )
    tx = make_event(
        user_id=USER_ID,
        account_id=accounts["uah_fop"]["id"],
        direction="expense",
        mcc=MCC_TRANSFER,
        counterparty_iban=UAH_BLACK_IBAN,
        time=T,
    )
    result = await make_service(tx_repo, acc_repo, anomaly_repo).detect_and_pair(
        None, tx
    )
    assert result.special_category is None
    assert result.related_transaction_id is None


# ---------------------------------------------------------------------------
# §4.3  Type matching
# ---------------------------------------------------------------------------


async def test_61_expense_only_matches_income_partner(tx_repo, acc_repo, anomaly_repo):
    accounts = seed_accounts(acc_repo)
    income_partner = tx_repo.add_transaction(
        user_id=USER_ID,
        account_id=accounts["uah_black"]["id"],
        direction="income",
        mcc=MCC_TRANSFER,
        time=T_PLUS_1S,
    )
    # A same-type expense on the target account — should NOT be a candidate
    tx_repo.add_transaction(
        user_id=USER_ID,
        account_id=accounts["uah_black"]["id"],
        direction="expense",
        mcc=MCC_TRANSFER,
        time=T_PLUS_1S,
    )
    tx = make_event(
        user_id=USER_ID,
        account_id=accounts["uah_fop"]["id"],
        direction="expense",
        mcc=MCC_TRANSFER,
        counterparty_iban=UAH_BLACK_IBAN,
        time=T,
    )
    result = await make_service(tx_repo, acc_repo, anomaly_repo).detect_and_pair(
        None, tx
    )
    assert result.special_category == "transfer"
    assert result.related_transaction_id == income_partner["id"]


async def test_62_income_only_matches_expense_partner(tx_repo, acc_repo, anomaly_repo):
    accounts = seed_accounts(acc_repo)
    expense_partner = tx_repo.add_transaction(
        user_id=USER_ID,
        account_id=accounts["uah_fop"]["id"],
        direction="expense",
        mcc=MCC_TRANSFER,
        time=T_MINUS_1S,
    )
    # A same-type income on the target account — should NOT be a candidate
    tx_repo.add_transaction(
        user_id=USER_ID,
        account_id=accounts["uah_fop"]["id"],
        direction="income",
        mcc=MCC_TRANSFER,
        time=T_MINUS_1S,
    )
    tx = make_event(
        user_id=USER_ID,
        account_id=accounts["uah_black"]["id"],
        direction="income",
        mcc=MCC_TRANSFER,
        counterparty_iban=UAH_FOP_IBAN,
        time=T,
    )
    result = await make_service(tx_repo, acc_repo, anomaly_repo).detect_and_pair(
        None, tx
    )
    assert result.special_category == "transfer"
    assert result.related_transaction_id == expense_partner["id"]


# ---------------------------------------------------------------------------
# §4.4  MCC filtering
# ---------------------------------------------------------------------------


async def test_63_partner_with_grocery_mcc_not_a_candidate(
    tx_repo, acc_repo, anomaly_repo
):
    accounts = seed_accounts(acc_repo)
    tx_repo.add_transaction(
        user_id=USER_ID,
        account_id=accounts["uah_black"]["id"],
        direction="income",
        mcc=MCC_GROCERY,
        time=T_PLUS_1S,
    )
    tx = make_event(
        user_id=USER_ID,
        account_id=accounts["uah_fop"]["id"],
        direction="expense",
        mcc=MCC_TRANSFER,
        counterparty_iban=UAH_BLACK_IBAN,
        time=T,
    )
    result = await make_service(tx_repo, acc_repo, anomaly_repo).detect_and_pair(
        None, tx
    )
    assert result.special_category is None
    assert result.related_transaction_id is None


async def test_64_incoming_non_transfer_mcc_skips_detection(
    tx_repo, acc_repo, anomaly_repo
):
    accounts = seed_accounts(acc_repo)
    tx_repo.add_transaction(
        user_id=USER_ID,
        account_id=accounts["uah_black"]["id"],
        direction="income",
        mcc=MCC_TRANSFER,
        time=T_PLUS_1S,
    )
    tx = make_event(
        user_id=USER_ID,
        account_id=accounts["uah_fop"]["id"],
        direction="expense",
        mcc=MCC_GROCERY,
        counterparty_iban=UAH_BLACK_IBAN,
        time=T,
    )
    result = await make_service(tx_repo, acc_repo, anomaly_repo).detect_and_pair(
        None, tx
    )
    assert result.special_category is None
    assert result.related_transaction_id is None
    assert result.anomalies == []


# ---------------------------------------------------------------------------
# §4.5  Claim exclusion
# ---------------------------------------------------------------------------


async def test_65_already_claimed_partner_not_a_candidate(
    tx_repo, acc_repo, anomaly_repo
):
    from uuid import uuid4

    accounts = seed_accounts(acc_repo)
    tx_repo.add_transaction(
        user_id=USER_ID,
        account_id=accounts["uah_black"]["id"],
        direction="income",
        special_category="transfer",
        mcc=MCC_TRANSFER,
        time=T_PLUS_1S,
        related_transaction_id=uuid4(),  # already claimed
    )
    tx = make_event(
        user_id=USER_ID,
        account_id=accounts["uah_fop"]["id"],
        direction="expense",
        mcc=MCC_TRANSFER,
        counterparty_iban=UAH_BLACK_IBAN,
        time=T,
    )
    result = await make_service(tx_repo, acc_repo, anomaly_repo).detect_and_pair(
        None, tx
    )
    assert result.special_category is None
    assert result.related_transaction_id is None


# ---------------------------------------------------------------------------
# §4.6  Ambiguity
# ---------------------------------------------------------------------------


async def test_66_two_unclaimed_partners_records_ambiguous_iban_match(
    tx_repo, acc_repo, anomaly_repo
):
    accounts = seed_accounts(acc_repo)
    p1 = tx_repo.add_transaction(
        user_id=USER_ID,
        account_id=accounts["uah_black"]["id"],
        direction="income",
        mcc=MCC_TRANSFER,
        time=T_PLUS_1S,
    )
    p2 = tx_repo.add_transaction(
        user_id=USER_ID,
        account_id=accounts["uah_black"]["id"],
        direction="income",
        mcc=MCC_TRANSFER,
        time=T_PLUS_1S,
    )
    tx = make_event(
        user_id=USER_ID,
        account_id=accounts["uah_fop"]["id"],
        direction="expense",
        mcc=MCC_TRANSFER,
        counterparty_iban=UAH_BLACK_IBAN,
        time=T,
    )
    result = await make_service(tx_repo, acc_repo, anomaly_repo).detect_and_pair(
        None, tx
    )

    assert result.special_category is None
    assert result.related_transaction_id is None
    assert len(result.anomalies) == 1
    anomaly = result.anomalies[0]
    assert anomaly.reason_code == "ambiguous_iban_match"
    assert set(anomaly.candidate_ids) == {p1["id"], p2["id"]}


async def test_67_two_candidates_one_outside_window_pairs_with_valid(
    tx_repo, acc_repo, anomaly_repo
):
    from datetime import timedelta

    accounts = seed_accounts(acc_repo)
    in_window = tx_repo.add_transaction(
        user_id=USER_ID,
        account_id=accounts["uah_black"]["id"],
        direction="income",
        mcc=MCC_TRANSFER,
        time=T_PLUS_1S,
    )
    tx_repo.add_transaction(
        user_id=USER_ID,
        account_id=accounts["uah_black"]["id"],
        direction="income",
        mcc=MCC_TRANSFER,
        time=T + timedelta(seconds=10),
    )
    tx = make_event(
        user_id=USER_ID,
        account_id=accounts["uah_fop"]["id"],
        direction="expense",
        mcc=MCC_TRANSFER,
        counterparty_iban=UAH_BLACK_IBAN,
        time=T,
    )
    result = await make_service(tx_repo, acc_repo, anomaly_repo).detect_and_pair(
        None, tx
    )
    assert result.special_category == "transfer"
    assert result.related_transaction_id == in_window["id"]


# ---------------------------------------------------------------------------
# §4.7  Description validation (canary on Tier A)
# ---------------------------------------------------------------------------


async def test_68_consistent_descriptions_paired_no_anomaly(
    tx_repo, acc_repo, anomaly_repo
):
    accounts = seed_accounts(acc_repo)
    partner = tx_repo.add_transaction(
        user_id=USER_ID,
        account_id=accounts["uah_black"]["id"],
        direction="income",
        mcc=MCC_TRANSFER,
        time=T_PLUS_1S,
        description="З гривневого рахунку ФОП",
    )
    tx = make_event(
        user_id=USER_ID,
        account_id=accounts["uah_fop"]["id"],
        direction="expense",
        mcc=MCC_TRANSFER,
        counterparty_iban=UAH_BLACK_IBAN,
        description="На чорну картку",
        time=T,
    )
    result = await make_service(tx_repo, acc_repo, anomaly_repo).detect_and_pair(
        None, tx
    )

    assert result.special_category == "transfer"
    assert result.related_transaction_id == partner["id"]
    assert result.anomalies == []


async def test_69_inconsistent_expense_desc_paired_with_canary_anomaly(
    tx_repo, acc_repo, anomaly_repo
):
    accounts = seed_accounts(acc_repo)
    partner = tx_repo.add_transaction(
        user_id=USER_ID,
        account_id=accounts["uah_black"]["id"],
        direction="income",
        mcc=MCC_TRANSFER,
        time=T_PLUS_1S,
        description="З гривневого рахунку ФОП",
    )
    # Expense says "white" but IBAN points to black card
    tx = make_event(
        user_id=USER_ID,
        account_id=accounts["uah_fop"]["id"],
        direction="expense",
        mcc=MCC_TRANSFER,
        counterparty_iban=UAH_BLACK_IBAN,
        description="На білу картку",
        time=T,
    )
    result = await make_service(tx_repo, acc_repo, anomaly_repo).detect_and_pair(
        None, tx
    )

    # Tier A still claims the pair (IBAN is authoritative)
    assert result.special_category == "transfer"
    assert result.related_transaction_id == partner["id"]
    assert len(result.anomalies) == 1
    assert result.anomalies[0].reason_code == "description_consistency_mismatch"


async def test_70_inconsistent_income_desc_paired_with_canary_anomaly(
    tx_repo, acc_repo, anomaly_repo
):
    accounts = seed_accounts(acc_repo)
    # Partner income says "З Білої картки" but expense account is FOP (not white)
    partner = tx_repo.add_transaction(
        user_id=USER_ID,
        account_id=accounts["uah_black"]["id"],
        direction="income",
        mcc=MCC_TRANSFER,
        time=T_PLUS_1S,
        description="З Білої картки",
    )
    tx = make_event(
        user_id=USER_ID,
        account_id=accounts["uah_fop"]["id"],
        direction="expense",
        mcc=MCC_TRANSFER,
        counterparty_iban=UAH_BLACK_IBAN,
        description="На чорну картку",
        time=T,
    )
    result = await make_service(tx_repo, acc_repo, anomaly_repo).detect_and_pair(
        None, tx
    )

    assert result.special_category == "transfer"
    assert result.related_transaction_id == partner["id"]
    assert len(result.anomalies) == 1
    assert result.anomalies[0].reason_code == "description_consistency_mismatch"


async def test_71_both_descriptions_wrong_single_canary_anomaly(
    tx_repo, acc_repo, anomaly_repo
):
    accounts = seed_accounts(acc_repo)
    # Both sides wrong: income says "white card" (expense is FOP),
    # expense says "white" (income is black)
    partner = tx_repo.add_transaction(
        user_id=USER_ID,
        account_id=accounts["uah_black"]["id"],
        direction="income",
        mcc=MCC_TRANSFER,
        time=T_PLUS_1S,
        description="З Білої картки",
    )
    tx = make_event(
        user_id=USER_ID,
        account_id=accounts["uah_fop"]["id"],
        direction="expense",
        mcc=MCC_TRANSFER,
        counterparty_iban=UAH_BLACK_IBAN,
        description="На білу картку",
        time=T,
    )
    result = await make_service(tx_repo, acc_repo, anomaly_repo).detect_and_pair(
        None, tx
    )

    # Still paired; exactly one anomaly record per incoming transaction
    assert result.special_category == "transfer"
    assert result.related_transaction_id == partner["id"]
    assert len(result.anomalies) == 1
    assert result.anomalies[0].reason_code == "description_consistency_mismatch"
