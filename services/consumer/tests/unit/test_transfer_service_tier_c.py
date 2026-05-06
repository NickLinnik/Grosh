"""Unit tests — Tier C (amount + description fallback) — §6 (tests 79-89)."""

import pytest

from grosh_consumer.sources.monobank.transfer import MonobankTransferDetection
from tests.helpers import make_event
from tests.unit.transfer_fixtures import (
    MCC_TRANSFER,
    T_MINUS_2S,
    T_PLUS_1S,
    T_PLUS_3S,
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
# §6.1  Operation amount cross-match
# ---------------------------------------------------------------------------


async def test_79_same_currency_card_to_card_paired(tx_repo, acc_repo, anomaly_repo):
    """Black→white same-currency transfer: amounts match, descriptions validate."""
    accounts = seed_accounts(acc_repo)

    partner = tx_repo.add_transaction(
        user_id=USER_ID,
        account_id=accounts["uah_white"]["id"],
        direction="income",
        mcc=MCC_TRANSFER,
        time=T_PLUS_1S,
        amount_cents=5000,
        operation_amount_cents=5000,
        counterparty_iban=None,
        description="З Чорної картки",
    )

    tx = make_event(
        user_id=USER_ID,
        account_id=accounts["uah_black"]["id"],
        direction="expense",
        mcc=MCC_TRANSFER,
        counterparty_iban=None,
        description="Переказ на картку",
        amount_cents=5000,
        operation_amount_cents=5000,
        time=T,
    )
    result = await make_service(tx_repo, acc_repo, anomaly_repo).detect_and_pair(
        None, tx
    )

    assert result.special_category == "transfer"
    assert result.related_transaction_id == partner["id"]


async def test_80_fx_card_to_card_operation_amount_cross_match(
    tx_repo, acc_repo, anomaly_repo
):
    """UAH expense → EUR income: cross-match on operation_amount_cents."""
    accounts = seed_accounts(acc_repo)

    # EUR card income: amount_cents=1750 EUR, operation_amount_cents=78400 UAH
    partner = tx_repo.add_transaction(
        user_id=USER_ID,
        account_id=accounts["eur_card"]["id"],
        direction="income",
        mcc=MCC_TRANSFER,
        time=T_PLUS_1S,
        amount_cents=1750,
        operation_amount_cents=78400,
        counterparty_iban=None,
        description="З Чорної картки",
    )

    # UAH black expense: amount_cents=78400 UAH, operation_amount_cents=1750 EUR
    tx = make_event(
        user_id=USER_ID,
        account_id=accounts["uah_black"]["id"],
        direction="expense",
        mcc=MCC_TRANSFER,
        counterparty_iban=None,
        description="Переказ на картку",
        amount_cents=78400,
        operation_amount_cents=1750,
        time=T,
    )
    result = await make_service(tx_repo, acc_repo, anomaly_repo).detect_and_pair(
        None, tx
    )

    # Tier C: expense.operation_amount_cents (1750) == partner.amount_cents (1750)
    assert result.special_category == "transfer"
    assert result.related_transaction_id == partner["id"]


async def test_81_amount_matches_but_description_fails_mismatch_anomaly(
    tx_repo, acc_repo, anomaly_repo
):
    """Amount matches within window but description validation fails."""
    accounts = seed_accounts(acc_repo)

    # Income says "З Білої картки" but expense account is black, not white
    bad_candidate = tx_repo.add_transaction(
        user_id=USER_ID,
        account_id=accounts["eur_card"]["id"],
        direction="income",
        mcc=MCC_TRANSFER,
        time=T_PLUS_1S,
        amount_cents=5000,
        operation_amount_cents=5000,
        counterparty_iban=None,
        description="З Білої картки",
    )

    tx = make_event(
        user_id=USER_ID,
        account_id=accounts["uah_black"]["id"],
        direction="expense",
        mcc=MCC_TRANSFER,
        counterparty_iban=None,
        description="Переказ на картку",
        amount_cents=5000,
        operation_amount_cents=5000,
        time=T,
    )
    result = await make_service(tx_repo, acc_repo, anomaly_repo).detect_and_pair(
        None, tx
    )

    assert result.special_category is None
    assert result.related_transaction_id is None
    assert len(result.anomalies) == 1
    assert result.anomalies[0].reason_code == "description_account_mismatch"
    assert bad_candidate["id"] in result.anomalies[0].candidate_ids


async def test_82_two_valid_candidates_ambiguous_amount_match(
    tx_repo, acc_repo, anomaly_repo
):
    """Two matching incomes, both valid descriptions — ambiguous_amount_match."""
    from uuid import uuid4

    accounts = seed_accounts(acc_repo)

    # Add a second white-type account
    extra_white_id = uuid4()
    acc_repo.add_account(
        id=extra_white_id,
        user_id=USER_ID,
        type="white",
        currency_code="UAH",
        iban=None,
    )

    p1 = tx_repo.add_transaction(
        user_id=USER_ID,
        account_id=accounts["uah_white"]["id"],
        direction="income",
        mcc=MCC_TRANSFER,
        time=T_PLUS_1S,
        amount_cents=5000,
        operation_amount_cents=5000,
        counterparty_iban=None,
        description="З Чорної картки",
    )
    p2 = tx_repo.add_transaction(
        user_id=USER_ID,
        account_id=extra_white_id,
        direction="income",
        mcc=MCC_TRANSFER,
        time=T_PLUS_1S,
        amount_cents=5000,
        operation_amount_cents=5000,
        counterparty_iban=None,
        description="З Чорної картки",
    )

    tx = make_event(
        user_id=USER_ID,
        account_id=accounts["uah_black"]["id"],
        direction="expense",
        mcc=MCC_TRANSFER,
        counterparty_iban=None,
        description="Переказ на картку",
        amount_cents=5000,
        operation_amount_cents=5000,
        time=T,
    )
    result = await make_service(tx_repo, acc_repo, anomaly_repo).detect_and_pair(
        None, tx
    )

    assert result.special_category is None
    assert result.related_transaction_id is None
    assert len(result.anomalies) == 1
    assert result.anomalies[0].reason_code == "ambiguous_amount_match"
    assert set(result.anomalies[0].candidate_ids) == {p1["id"], p2["id"]}


async def test_83_no_amount_match_income_with_z_prefix_unpaired_from(
    tx_repo, acc_repo, anomaly_repo
):
    """No candidates at all; income has 'З ' prefix — unpaired_from_description."""
    accounts = seed_accounts(acc_repo)

    tx = make_event(
        user_id=USER_ID,
        account_id=accounts["uah_black"]["id"],
        direction="income",
        mcc=MCC_TRANSFER,
        counterparty_iban=None,
        description="З Чорної картки",
        amount_cents=5000,
        operation_amount_cents=5000,
        time=T,
    )
    result = await make_service(tx_repo, acc_repo, anomaly_repo).detect_and_pair(
        None, tx
    )

    assert result.special_category is None
    assert result.related_transaction_id is None
    assert len(result.anomalies) == 1
    assert result.anomalies[0].reason_code == "unpaired_from_description"


async def test_84_no_amount_match_expense_with_na_prefix_unpaired_to(
    tx_repo, acc_repo, anomaly_repo
):
    """No candidates at all; expense has 'На ' prefix — unpaired_to_description."""
    accounts = seed_accounts(acc_repo)

    tx = make_event(
        user_id=USER_ID,
        account_id=accounts["uah_black"]["id"],
        direction="expense",
        mcc=MCC_TRANSFER,
        counterparty_iban=None,
        description="На білу картку",
        amount_cents=5000,
        operation_amount_cents=5000,
        time=T,
    )
    result = await make_service(tx_repo, acc_repo, anomaly_repo).detect_and_pair(
        None, tx
    )

    assert result.special_category is None
    assert result.related_transaction_id is None
    assert len(result.anomalies) == 1
    assert result.anomalies[0].reason_code == "unpaired_to_description"


# ---------------------------------------------------------------------------
# §6.2  Same-account exclusion
# ---------------------------------------------------------------------------


async def test_85_tier_c_excludes_same_account(tx_repo, acc_repo, anomaly_repo):
    """Two txs on the same account cannot pair via Tier C even if amounts match."""
    accounts = seed_accounts(acc_repo)

    tx_repo.add_transaction(
        user_id=USER_ID,
        account_id=accounts["uah_fop"]["id"],  # same account as incoming
        direction="income",
        mcc=MCC_TRANSFER,
        time=T_PLUS_1S,
        amount_cents=5000,
        operation_amount_cents=5000,
        counterparty_iban=None,
        description="З гривневого рахунку ФОП",
    )

    tx = make_event(
        user_id=USER_ID,
        account_id=accounts["uah_fop"]["id"],
        direction="expense",
        mcc=MCC_TRANSFER,
        counterparty_iban=None,
        description="На гривневий рахунок ФОП",
        amount_cents=5000,
        operation_amount_cents=5000,
        time=T,
    )
    result = await make_service(tx_repo, acc_repo, anomaly_repo).detect_and_pair(
        None, tx
    )

    # Same account_id excluded by Tier C query
    assert result.special_category is None
    assert result.related_transaction_id is None


# ---------------------------------------------------------------------------
# §6.3  Description not in allowed set
# ---------------------------------------------------------------------------


async def test_86_person_name_description_no_tier_c_no_anomaly(
    tx_repo, acc_repo, anomaly_repo
):
    """Non-transfer description → Tier C not entered, no anomaly."""
    accounts = seed_accounts(acc_repo)

    tx_repo.add_transaction(
        user_id=USER_ID,
        account_id=accounts["uah_white"]["id"],
        direction="income",
        mcc=MCC_TRANSFER,
        time=T_PLUS_1S,
        amount_cents=5000,
        operation_amount_cents=5000,
        counterparty_iban=None,
    )

    tx = make_event(
        user_id=USER_ID,
        account_id=accounts["uah_black"]["id"],
        direction="expense",
        mcc=MCC_TRANSFER,
        counterparty_iban=None,
        description="Олена К.",
        amount_cents=5000,
        operation_amount_cents=5000,
        time=T,
    )
    result = await make_service(tx_repo, acc_repo, anomaly_repo).detect_and_pair(
        None, tx
    )

    assert result.special_category is None
    assert result.related_transaction_id is None
    assert result.anomalies == []


async def test_87_unrecognized_z_prefix_unpaired_from_description(
    tx_repo, acc_repo, anomaly_repo
):
    """'З ' prefix but not in known set → unpaired_from_description, no
    Tier C attempt."""
    accounts = seed_accounts(acc_repo)

    tx = make_event(
        user_id=USER_ID,
        account_id=accounts["uah_black"]["id"],
        direction="income",
        mcc=MCC_TRANSFER,
        counterparty_iban=None,
        description="З невідомого рахунку",
        amount_cents=5000,
        operation_amount_cents=5000,
        time=T,
    )
    result = await make_service(tx_repo, acc_repo, anomaly_repo).detect_and_pair(
        None, tx
    )

    assert result.special_category is None
    assert result.related_transaction_id is None
    assert len(result.anomalies) == 1
    assert result.anomalies[0].reason_code == "unpaired_from_description"


# ---------------------------------------------------------------------------
# §6.4  Tier C time window
# ---------------------------------------------------------------------------


async def test_88_partner_at_boundary_2s_matched(tx_repo, acc_repo, anomaly_repo):
    """Partner exactly at ±2s boundary is matched (inclusive)."""
    accounts = seed_accounts(acc_repo)

    partner = tx_repo.add_transaction(
        user_id=USER_ID,
        account_id=accounts["uah_white"]["id"],
        direction="income",
        mcc=MCC_TRANSFER,
        time=T_MINUS_2S,
        amount_cents=5000,
        operation_amount_cents=5000,
        counterparty_iban=None,
        description="З Чорної картки",
    )

    tx = make_event(
        user_id=USER_ID,
        account_id=accounts["uah_black"]["id"],
        direction="expense",
        mcc=MCC_TRANSFER,
        counterparty_iban=None,
        description="Переказ на картку",
        amount_cents=5000,
        operation_amount_cents=5000,
        time=T,
    )
    result = await make_service(tx_repo, acc_repo, anomaly_repo).detect_and_pair(
        None, tx
    )
    assert result.special_category == "transfer"
    assert result.related_transaction_id == partner["id"]


async def test_89_partner_at_3s_no_match(tx_repo, acc_repo, anomaly_repo):
    """Partner at ±3s is outside window — no match."""
    accounts = seed_accounts(acc_repo)

    tx_repo.add_transaction(
        user_id=USER_ID,
        account_id=accounts["uah_white"]["id"],
        direction="income",
        mcc=MCC_TRANSFER,
        time=T_PLUS_3S,
        amount_cents=5000,
        operation_amount_cents=5000,
        counterparty_iban=None,
        description="З Чорної картки",
    )

    tx = make_event(
        user_id=USER_ID,
        account_id=accounts["uah_black"]["id"],
        direction="expense",
        mcc=MCC_TRANSFER,
        counterparty_iban=None,
        description="Переказ на картку",
        amount_cents=5000,
        operation_amount_cents=5000,
        time=T,
    )
    result = await make_service(tx_repo, acc_repo, anomaly_repo).detect_and_pair(
        None, tx
    )
    assert result.special_category is None
    assert result.related_transaction_id is None
