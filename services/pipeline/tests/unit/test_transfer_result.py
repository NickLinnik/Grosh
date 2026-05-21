"""Unit tests for TransferResult dataclass shape.

Test cases per `references/consumer-transfer-detection-test-suite.md` §8.
TransferResult already exists in services/transfer_detection.py (Slice 16
added the metadata_block field), so these tests run LIVE — no skip marker.
"""

import dataclasses
from uuid import uuid4

import pytest

from grosh_pipeline.services.transfer_detection import AnomalyRecord, TransferResult


def test_143_default_transfer_result_fields():
    result = TransferResult()
    assert result.special_category is None
    assert result.related_transaction_id is None
    assert result.anomalies == []
    assert result.metadata_block is None


def test_144_successful_claim_fields():
    partner_id = uuid4()
    row_block = {
        "description_matched": True,
        "multi_hop_description": False,
        "cp_iban_status": "honest",
    }
    pair_block = {"iban_evidence": "bilateral", "description_decisive": False}
    result = TransferResult(
        special_category="transfer",
        related_transaction_id=partner_id,
        anomalies=[],
        metadata_block={"row": row_block, "pair": pair_block},
    )
    assert result.special_category == "transfer"
    assert result.related_transaction_id == partner_id
    assert result.anomalies == []
    assert result.metadata_block == {"row": row_block, "pair": pair_block}


def test_145_successful_claim_with_canary_anomaly():
    partner_id = uuid4()
    tx_id = uuid4()
    canary = AnomalyRecord(
        transaction_id=tx_id,
        candidate_ids=[partner_id],
        reason_code="description_consistency_mismatch",
        reason_detail="descriptions disagree",
    )
    result = TransferResult(
        special_category="transfer",
        related_transaction_id=partner_id,
        anomalies=[canary],
        metadata_block={"row": {}, "pair": {}},
    )
    assert result.special_category == "transfer"
    assert len(result.anomalies) == 1
    assert result.anomalies[0].reason_code == "description_consistency_mismatch"


def test_146_anomaly_only_no_claim():
    tx_id = uuid4()
    anomaly = AnomalyRecord(
        transaction_id=tx_id,
        candidate_ids=[],
        reason_code="unpaired_from_description",
        reason_detail="З Чорної картки",
    )
    row_block = {
        "description_matched": True,
        "multi_hop_description": False,
        "cp_iban_status": "null",
    }
    result = TransferResult(
        special_category=None,
        related_transaction_id=None,
        anomalies=[anomaly],
        metadata_block={"row": row_block},
    )
    assert result.special_category is None
    assert result.related_transaction_id is None
    assert len(result.anomalies) == 1
    assert result.metadata_block == {"row": row_block}
    assert "pair" not in result.metadata_block


def test_147_skip_non_mcc_4829_metadata_block_is_none():
    # Non-MCC-4829 rows: metadata_block is None, not {}.
    # Orchestrator uses `is None` to skip writing metadata.layer.transfer.
    result = TransferResult(
        special_category=None,
        related_transaction_id=None,
        anomalies=[],
        metadata_block=None,
    )
    assert result.metadata_block is None


def test_148_transfer_result_is_frozen():
    result = TransferResult()
    with pytest.raises(dataclasses.FrozenInstanceError):
        result.special_category = "transfer"  # type: ignore[misc]


def test_149_transfer_result_equality_is_structural():
    partner_id = uuid4()
    a = TransferResult(
        special_category="transfer",
        related_transaction_id=partner_id,
        anomalies=[],
        metadata_block={"row": {}, "pair": {}},
    )
    b = TransferResult(
        special_category="transfer",
        related_transaction_id=partner_id,
        anomalies=[],
        metadata_block={"row": {}, "pair": {}},
    )
    assert a == b
