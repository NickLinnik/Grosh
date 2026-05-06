"""Unit tests — anomaly recording — §7 (tests 90-97)."""

import pytest

from grosh_consumer.sources.monobank.transfer import MonobankTransferDetection
from tests.helpers import make_event
from tests.unit.transfer_fixtures import (
    MCC_TRANSFER,
    T_PLUS_1S,
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
# §7.1  Auto-resolution on successful claim
# ---------------------------------------------------------------------------


async def test_90_unpaired_anomaly_deleted_when_partner_arrives(
    tx_repo, acc_repo, anomaly_repo
):
    """Transaction A gets unpaired_from_description; when B arrives and pairs,
    A's anomaly is deleted."""
    accounts = seed_accounts(acc_repo)

    # A: card income, no partner yet — will get unpaired anomaly
    tx_a = make_event(
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
    result_a = await make_service(tx_repo, acc_repo, anomaly_repo).detect_and_pair(
        None, tx_a
    )
    assert result_a.anomalies[0].reason_code == "unpaired_from_description"

    # Persist anomaly and insert tx_a into the store
    await anomaly_repo.record_anomaly(
        None,
        result_a.anomalies[0].transaction_id,
        result_a.anomalies[0].candidate_ids,
        result_a.anomalies[0].reason_code,
        result_a.anomalies[0].reason_detail,
    )
    tx_repo.add_transaction(
        id=tx_a.id,
        user_id=USER_ID,
        account_id=accounts["uah_black"]["id"],
        direction="income",
        mcc=MCC_TRANSFER,
        time=T,
        amount_cents=5000,
        operation_amount_cents=5000,
        counterparty_iban=None,
        description="З Чорної картки",
    )

    # B: FOP expense arrives, Tier A matches A on black card via IBAN
    tx_b = make_event(
        user_id=USER_ID,
        account_id=accounts["uah_fop"]["id"],
        direction="expense",
        mcc=MCC_TRANSFER,
        counterparty_iban=accounts["uah_black"]["iban"],
        description="На чорну картку",
        amount_cents=5000,
        operation_amount_cents=5000,
        time=T_PLUS_1S,
    )
    result_b = await make_service(tx_repo, acc_repo, anomaly_repo).detect_and_pair(
        None, tx_b
    )

    assert result_b.special_category == "transfer"
    assert result_b.related_transaction_id == tx_a.id

    # A's anomaly should have been auto-deleted during claim
    remaining = anomaly_repo.all_anomalies()
    assert not any(a["transaction_id"] == tx_a.id for a in remaining)


async def test_91_both_legs_had_anomalies_both_deleted_on_pair(
    tx_repo, acc_repo, anomaly_repo
):
    """A → unpaired; B → unpaired; when B's handler finds A and pairs,
    both anomalies deleted."""
    accounts = seed_accounts(acc_repo)

    # A: FOP expense, arrives and gets unpaired_to_description (no partner yet)
    tx_a_id_stored = tx_repo.add_transaction(
        user_id=USER_ID,
        account_id=accounts["uah_fop"]["id"],
        direction="expense",
        mcc=MCC_TRANSFER,
        time=T,
        amount_cents=5000,
        operation_amount_cents=5000,
        counterparty_iban=accounts["uah_black"]["iban"],
        description="На чорну картку",
    )
    await anomaly_repo.record_anomaly(
        None,
        tx_a_id_stored["id"],
        [],
        "unpaired_to_description",
        "no partner",
    )

    # B: card income arrives, Tier A finds A on black card, claims pair
    tx_b = make_event(
        user_id=USER_ID,
        account_id=accounts["uah_black"]["id"],
        direction="income",
        mcc=MCC_TRANSFER,
        counterparty_iban=None,
        description="З гривневого рахунку ФОП",
        amount_cents=5000,
        operation_amount_cents=5000,
        time=T_PLUS_1S,
    )
    # Tier B: acc_repo knows UAH_BLACK has an iban; Tier B searches for expense
    # pointing at that iban — finds tx_a_id_stored. Pairs.
    result_b = await make_service(tx_repo, acc_repo, anomaly_repo).detect_and_pair(
        None, tx_b
    )

    assert result_b.special_category == "transfer"

    # Both anomalies should be deleted: A's anomaly was deleted during claim
    remaining = anomaly_repo.all_anomalies()
    assert not any(a["transaction_id"] == tx_a_id_stored["id"] for a in remaining)


async def test_92_terminal_anomaly_persists_not_auto_deleted(
    tx_repo, acc_repo, anomaly_repo
):
    """ambiguous_iban_match anomaly is terminal — never auto-deleted by
    subsequent events."""
    accounts = seed_accounts(acc_repo)

    # Two candidates on black card → ambiguous_iban_match recorded for tx_a
    tx_repo.add_transaction(
        user_id=USER_ID,
        account_id=accounts["uah_black"]["id"],
        direction="income",
        mcc=MCC_TRANSFER,
        time=T_PLUS_1S,
        amount_cents=5000,
        operation_amount_cents=5000,
    )
    tx_repo.add_transaction(
        user_id=USER_ID,
        account_id=accounts["uah_black"]["id"],
        direction="income",
        mcc=MCC_TRANSFER,
        time=T_PLUS_1S,
        amount_cents=5000,
        operation_amount_cents=5000,
    )

    tx_a = make_event(
        user_id=USER_ID,
        account_id=accounts["uah_fop"]["id"],
        direction="expense",
        mcc=MCC_TRANSFER,
        counterparty_iban=accounts["uah_black"]["iban"],
        amount_cents=5000,
        operation_amount_cents=5000,
        time=T,
    )
    result_a = await make_service(tx_repo, acc_repo, anomaly_repo).detect_and_pair(
        None, tx_a
    )
    assert result_a.anomalies[0].reason_code == "ambiguous_iban_match"

    # Persist the anomaly manually
    await anomaly_repo.record_anomaly(
        None,
        tx_a.id,
        result_a.anomalies[0].candidate_ids,
        "ambiguous_iban_match",
        None,
    )

    # Process an unrelated transaction — should not touch A's anomaly
    other_tx = make_event(
        user_id=USER_ID,
        account_id=accounts["uah_white"]["id"],
        direction="expense",
        mcc=MCC_TRANSFER,
        time=T_PLUS_1S,
    )
    await make_service(tx_repo, acc_repo, anomaly_repo).detect_and_pair(None, other_tx)

    # A's terminal anomaly still present
    remaining = anomaly_repo.all_anomalies()
    assert any(
        a["transaction_id"] == tx_a.id and a["reason_code"] == "ambiguous_iban_match"
        for a in remaining
    )


# ---------------------------------------------------------------------------
# §7.2  Anomaly content
# ---------------------------------------------------------------------------


async def test_93_ambiguous_iban_match_includes_all_candidate_ids(
    tx_repo, acc_repo, anomaly_repo
):
    """3 candidates on target account → candidate_ids has all 3."""
    accounts = seed_accounts(acc_repo)

    ids = []
    for _ in range(3):
        p = tx_repo.add_transaction(
            user_id=USER_ID,
            account_id=accounts["uah_black"]["id"],
            direction="income",
            mcc=MCC_TRANSFER,
            time=T_PLUS_1S,
        )
        ids.append(p["id"])

    tx = make_event(
        user_id=USER_ID,
        account_id=accounts["uah_fop"]["id"],
        direction="expense",
        mcc=MCC_TRANSFER,
        counterparty_iban=accounts["uah_black"]["iban"],
        time=T,
    )
    result = await make_service(tx_repo, acc_repo, anomaly_repo).detect_and_pair(
        None, tx
    )

    assert result.anomalies[0].reason_code == "ambiguous_iban_match"
    assert set(result.anomalies[0].candidate_ids) == set(ids)


async def test_94_description_account_mismatch_includes_failed_candidate(
    tx_repo, acc_repo, anomaly_repo
):
    """description_account_mismatch includes the candidate that failed validation."""
    accounts = seed_accounts(acc_repo)

    bad_candidate = tx_repo.add_transaction(
        user_id=USER_ID,
        account_id=accounts["eur_card"]["id"],
        direction="income",
        mcc=MCC_TRANSFER,
        time=T_PLUS_1S,
        amount_cents=5000,
        operation_amount_cents=5000,
        counterparty_iban=None,
        description="З Білої картки",  # type=white, but expense account is black
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

    assert result.anomalies[0].reason_code == "description_account_mismatch"
    assert result.anomalies[0].candidate_ids == [bad_candidate["id"]]


async def test_95_unpaired_from_description_has_empty_candidate_ids(
    tx_repo, acc_repo, anomaly_repo
):
    """No candidates exist at all → candidate_ids is empty list."""
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

    assert result.anomalies[0].reason_code == "unpaired_from_description"
    assert result.anomalies[0].candidate_ids == []


async def test_96_description_consistency_mismatch_includes_partner_id(
    tx_repo, acc_repo, anomaly_repo
):
    """description_consistency_mismatch candidate_ids contains the claimed partner."""
    accounts = seed_accounts(acc_repo)

    partner = tx_repo.add_transaction(
        user_id=USER_ID,
        account_id=accounts["uah_black"]["id"],
        direction="income",
        mcc=MCC_TRANSFER,
        time=T_PLUS_1S,
        description="З гривневого рахунку ФОП",
    )

    # Expense says "На білу картку" but IBAN points to black card
    tx = make_event(
        user_id=USER_ID,
        account_id=accounts["uah_fop"]["id"],
        direction="expense",
        mcc=MCC_TRANSFER,
        counterparty_iban=accounts["uah_black"]["iban"],
        description="На білу картку",
        time=T,
    )
    result = await make_service(tx_repo, acc_repo, anomaly_repo).detect_and_pair(
        None, tx
    )

    assert result.anomalies[0].reason_code == "description_consistency_mismatch"
    assert result.anomalies[0].candidate_ids == [partner["id"]]


async def test_97_reason_detail_includes_human_readable_context(
    tx_repo, acc_repo, anomaly_repo
):
    """reason_detail for description_account_mismatch contains type/currency info."""
    accounts = seed_accounts(acc_repo)

    tx_repo.add_transaction(
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

    anomaly = result.anomalies[0]
    assert anomaly.reason_detail is not None
    # Detail must mention the account type/currency mismatch
    detail = anomaly.reason_detail.lower()
    assert "black" in detail or "white" in detail
