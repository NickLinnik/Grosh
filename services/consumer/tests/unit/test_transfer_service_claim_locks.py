"""Unit tests — claim lock simulation — §10 (tests 106-108)."""

from unittest.mock import AsyncMock, MagicMock

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


async def test_106_single_candidate_unclaimed_claim_succeeds(
    tx_repo, acc_repo, anomaly_repo
):
    """Single unclaimed candidate → claim succeeds, pair set."""
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
        counterparty_iban=accounts["uah_black"]["iban"],
        amount_cents=5000,
        operation_amount_cents=5000,
        time=T,
    )
    result = await make_service(tx_repo, acc_repo, anomaly_repo).detect_and_pair(
        None, tx
    )

    assert result.special_category == "transfer"
    assert result.related_transaction_id == partner["id"]
    assert tx_repo.get_by_id(partner["id"])["related_transaction_id"] == tx.id


async def test_107_candidate_already_claimed_skip_locked_returns_empty(
    tx_repo, acc_repo, anomaly_repo
):
    """Simulates SKIP LOCKED: repo returns empty list (row locked by another worker).
    Transaction inserts normally without a pair."""
    accounts = seed_accounts(acc_repo)

    # Mock tx_repo: exists returns False, but find_unclaimed_partner_tier_a returns []
    # (simulating SKIP LOCKED skipping the only candidate)
    mock_tx_repo = MagicMock()
    mock_tx_repo.exists = AsyncMock(return_value=False)
    mock_tx_repo.find_unclaimed_partner_tier_a = AsyncMock(return_value=[])
    mock_tx_repo.find_unclaimed_partner_tier_b = AsyncMock(return_value=[])
    mock_tx_repo.claim_pair = AsyncMock()

    tx = make_event(
        user_id=USER_ID,
        account_id=accounts["uah_fop"]["id"],
        direction="expense",
        mcc=MCC_TRANSFER,
        counterparty_iban=accounts["uah_black"]["iban"],
        amount_cents=5000,
        operation_amount_cents=5000,
        time=T,
    )

    service = MonobankTransferDetection(
        transaction_repo=mock_tx_repo,
        account_repo=acc_repo,
        anomaly_repo=anomaly_repo,
    )
    result = await service.detect_and_pair(None, tx)

    # No partner found → inserts as expense, no claim
    assert result.special_category is None
    assert result.related_transaction_id is None
    mock_tx_repo.claim_pair.assert_not_called()


async def test_108_concurrent_handlers_second_sees_already_claimed(
    tx_repo, acc_repo, anomaly_repo
):
    """Handler 1 claims the pair. Handler 2 sees leg B already in DB
    → idempotency guard fires."""
    accounts = seed_accounts(acc_repo)
    svc = make_service(tx_repo, acc_repo, anomaly_repo)

    # Leg A stored unclaimed
    leg_a = tx_repo.add_transaction(
        user_id=USER_ID,
        account_id=accounts["uah_black"]["id"],
        direction="income",
        mcc=MCC_TRANSFER,
        time=T,
        amount_cents=5000,
        operation_amount_cents=5000,
    )

    # Leg B — handler 1 processes it and pairs with leg A
    leg_b_event = make_event(
        user_id=USER_ID,
        account_id=accounts["uah_fop"]["id"],
        direction="expense",
        mcc=MCC_TRANSFER,
        counterparty_iban=accounts["uah_black"]["iban"],
        amount_cents=5000,
        operation_amount_cents=5000,
        time=T_PLUS_1S,
    )
    r_handler1 = await svc.detect_and_pair(None, leg_b_event)
    assert r_handler1.special_category == "transfer"
    assert r_handler1.related_transaction_id == leg_a["id"]

    # Simulate handler 1 having inserted leg B into the DB
    tx_repo.add_transaction(
        id=leg_b_event.id,
        user_id=USER_ID,
        account_id=accounts["uah_fop"]["id"],
        direction="expense",
        special_category="transfer",
        mcc=MCC_TRANSFER,
        counterparty_iban=accounts["uah_black"]["iban"],
        time=T_PLUS_1S,
        amount_cents=5000,
        operation_amount_cents=5000,
        related_transaction_id=leg_a["id"],
    )

    # Handler 2 re-delivers the same event for leg B
    r_handler2 = await svc.detect_and_pair(None, leg_b_event)

    # Idempotency guard: leg B already in DB → immediate return, no duplicate claim
    assert r_handler2.special_category is None
    assert r_handler2.related_transaction_id is None
    assert r_handler2.anomalies == []

    # Leg A still has its original claim; no double-claim occurred
    assert tx_repo.get_by_id(leg_a["id"])["related_transaction_id"] == leg_b_event.id
