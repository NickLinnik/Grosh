"""Unit tests for TransferResult model — §3 (tests 49-52)."""

from uuid import uuid4

import pytest

from grosh_consumer.services.transfer_detection import AnomalyRecord, TransferResult


def test_49_default_result_has_no_pair_and_empty_anomalies():
    result = TransferResult()
    assert result.special_category is None
    assert result.related_transaction_id is None
    assert result.anomalies == []


def test_50_paired_result_carries_transfer_special_category_and_related_id():
    partner_id = uuid4()
    result = TransferResult(
        special_category="transfer",
        related_transaction_id=partner_id,
    )
    assert result.special_category == "transfer"
    assert result.related_transaction_id == partner_id


def test_51_result_with_anomaly_has_no_special_category():
    tx_id = uuid4()
    anomaly = AnomalyRecord(
        transaction_id=tx_id,
        candidate_ids=[],
        reason_code="unpaired_from_description",
        reason_detail="no partner found",
    )
    result = TransferResult(anomalies=[anomaly])
    assert result.special_category is None
    assert result.related_transaction_id is None
    assert len(result.anomalies) == 1
    assert result.anomalies[0].reason_code == "unpaired_from_description"


def test_52_transfer_result_is_immutable():
    result = TransferResult()
    with pytest.raises((AttributeError, TypeError)):
        result.special_category = "transfer"  # type: ignore[misc]
