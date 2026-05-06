"""Unit tests — idempotency guard — §8 (tests 98-100)."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from grosh_consumer.sources.monobank.transfer import MonobankTransferDetection
from tests.helpers import make_event
from tests.unit.transfer_fixtures import (
    MCC_TRANSFER,
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


async def test_98_already_exists_returns_immediately_no_side_effects(
    tx_repo, acc_repo, anomaly_repo
):
    """Transaction ID already in DB → detect_and_pair is a no-op."""
    accounts = seed_accounts(acc_repo)

    # Pre-insert tx_a into the store so exists() returns True
    tx_a_stored = tx_repo.add_transaction(
        user_id=USER_ID,
        account_id=accounts["uah_fop"]["id"],
        direction="expense",
        mcc=MCC_TRANSFER,
        time=T,
        amount_cents=5000,
        operation_amount_cents=5000,
    )

    # Create a NormalizedTransaction whose id matches the stored one
    tx = make_event(
        id=tx_a_stored["id"],
        user_id=USER_ID,
        account_id=accounts["uah_fop"]["id"],
        direction="expense",
        mcc=MCC_TRANSFER,
        time=T,
        amount_cents=5000,
        operation_amount_cents=5000,
    )

    result = await make_service(tx_repo, acc_repo, anomaly_repo).detect_and_pair(
        None, tx
    )

    # Returns no special_category, no pair, no anomalies
    assert result.special_category is None
    assert result.related_transaction_id is None
    assert result.anomalies == []
    # No anomalies were recorded
    assert anomaly_repo.all_anomalies() == []


async def test_99_new_id_proceeds_with_detection(tx_repo, acc_repo, anomaly_repo):
    """Transaction ID not in DB → normal tier resolution proceeds."""
    accounts = seed_accounts(acc_repo)

    # Partner already in store
    partner = tx_repo.add_transaction(
        user_id=USER_ID,
        account_id=accounts["uah_black"]["id"],
        direction="income",
        mcc=MCC_TRANSFER,
        time=T,
        amount_cents=5000,
        operation_amount_cents=5000,
    )

    # New tx with fresh ID
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


async def test_100_idempotency_guard_runs_before_any_tier_logic(acc_repo, anomaly_repo):
    """When exists() returns True, none of the find_unclaimed_partner_*
    methods are called."""
    # Build a mock tx_repo where exists() returns True
    mock_tx_repo = MagicMock()
    mock_tx_repo.exists = AsyncMock(return_value=True)
    mock_tx_repo.find_unclaimed_partner_tier_a = AsyncMock()
    mock_tx_repo.find_unclaimed_partner_tier_b = AsyncMock()
    mock_tx_repo.find_unclaimed_partner_tier_c = AsyncMock()

    accounts = seed_accounts(acc_repo)
    tx = make_event(
        user_id=USER_ID,
        account_id=accounts["uah_fop"]["id"],
        direction="expense",
        mcc=MCC_TRANSFER,
        counterparty_iban=accounts["uah_black"]["iban"],
        time=T,
    )

    service = MonobankTransferDetection(
        transaction_repo=mock_tx_repo,
        account_repo=acc_repo,
        anomaly_repo=anomaly_repo,
    )
    result = await service.detect_and_pair(None, tx)

    assert result.special_category is None
    mock_tx_repo.find_unclaimed_partner_tier_a.assert_not_called()
    mock_tx_repo.find_unclaimed_partner_tier_b.assert_not_called()
    mock_tx_repo.find_unclaimed_partner_tier_c.assert_not_called()
