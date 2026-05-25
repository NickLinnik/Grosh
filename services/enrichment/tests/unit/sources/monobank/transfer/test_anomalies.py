"""Unit tests for AnomalyRecord builders."""

from uuid import uuid4

from grosh_enrichment.sources.monobank.transfer.anomalies import (
    ambiguous_pair_match,
    description_account_mismatch,
    description_consistency_mismatch,
    unpaired_from_description,
    unpaired_to_description,
)
from tests.unit.conftest import UAH_BLACK, UAH_FOP

_TX_ID = uuid4()
_PARTNER_ID = uuid4()
_CANDIDATE_A = uuid4()
_CANDIDATE_B = uuid4()
_CANDIDATE_C = uuid4()


# ---------------------------------------------------------------------------
# §6.1  unpaired_from_description (tests 113-114)
# ---------------------------------------------------------------------------


def test_113_unpaired_from_description_fields():
    record = unpaired_from_description(
        transaction_id=_TX_ID,
        description="З Чорної картки",
    )
    assert record.reason_code == "unpaired_from_description"
    assert record.candidate_ids == []
    assert "З Чорної картки" in record.reason_detail


def test_114_unpaired_from_description_idempotent():
    record_a = unpaired_from_description(
        transaction_id=_TX_ID,
        description="З Чорної картки",
    )
    record_b = unpaired_from_description(
        transaction_id=_TX_ID,
        description="З Чорної картки",
    )
    assert record_a == record_b


# ---------------------------------------------------------------------------
# §6.2  unpaired_to_description (test 115)
# ---------------------------------------------------------------------------


def test_115_unpaired_to_description_fields():
    record = unpaired_to_description(
        transaction_id=_TX_ID,
        description="На білу картку",
    )
    assert record.reason_code == "unpaired_to_description"
    assert record.candidate_ids == []
    assert "На білу картку" in record.reason_detail


# ---------------------------------------------------------------------------
# §6.3  description_consistency_mismatch (tests 116-117)
# ---------------------------------------------------------------------------


def test_116_description_consistency_mismatch_fields():
    record = description_consistency_mismatch(
        transaction_id=_TX_ID,
        partner_id=_PARTNER_ID,
        income_desc="З Чорної картки",
        expense_desc="На білу картку",
        income_acc_props=UAH_BLACK,
        expense_acc_props=UAH_FOP,
    )
    assert record.reason_code == "description_consistency_mismatch"
    assert _PARTNER_ID in record.candidate_ids
    # Detail must mention both descriptions so a debugger can diff them.
    assert "З Чорної картки" in record.reason_detail
    assert "На білу картку" in record.reason_detail


def test_117_description_consistency_mismatch_partner_id_in_candidate_ids():
    # The pair was claimed BEFORE this anomaly was constructed (canary semantics).
    # The partner_id being in candidate_ids confirms "claimed, then canary" order.
    record = description_consistency_mismatch(
        transaction_id=_TX_ID,
        partner_id=_PARTNER_ID,
        income_desc="З гривневого рахунку ФОП",
        expense_desc="На чорну картку",
        income_acc_props=UAH_BLACK,
        expense_acc_props=UAH_FOP,
    )
    assert _PARTNER_ID in record.candidate_ids


# ---------------------------------------------------------------------------
# §6.4  description_account_mismatch (test 118)
# ---------------------------------------------------------------------------


def test_118_description_account_mismatch_fields():
    rejected_ids = [_CANDIDATE_A, _CANDIDATE_B]
    record = description_account_mismatch(
        transaction_id=_TX_ID,
        rejected_candidate_ids=rejected_ids,
        incoming_description="На чорну картку",
        bucket_evidence="none",
    )
    assert record.reason_code == "description_account_mismatch"
    assert set(record.candidate_ids) == set(rejected_ids)
    assert "На чорну картку" in record.reason_detail
    assert "none" in record.reason_detail


# ---------------------------------------------------------------------------
# §6.5  ambiguous_pair_match (tests 119-120)
# ---------------------------------------------------------------------------


def test_119_ambiguous_pair_match_fields():
    bucket_ids = [_CANDIDATE_A, _CANDIDATE_B]
    record = ambiguous_pair_match(
        transaction_id=_TX_ID,
        bucket_candidate_ids=bucket_ids,
        bucket_evidence="bilateral",
        surviving_count=2,
    )
    assert record.reason_code == "ambiguous_pair_match"
    assert set(record.candidate_ids) == set(bucket_ids)
    assert "bilateral" in record.reason_detail


def test_120_ambiguous_pair_match_distinguishes_zero_vs_multi_survivors():
    # Zero survivors with evidence ≥unilateral: detail mentions description
    # eliminated all.  More than 1 survivor: detail reflects ambiguous count.
    # The detail strings must differ so an operator can tell them apart.
    record_zero = ambiguous_pair_match(
        transaction_id=_TX_ID,
        bucket_candidate_ids=[_CANDIDATE_A, _CANDIDATE_B],
        bucket_evidence="unilateral",
        surviving_count=0,
    )
    record_multi = ambiguous_pair_match(
        transaction_id=_TX_ID,
        bucket_candidate_ids=[_CANDIDATE_A, _CANDIDATE_B, _CANDIDATE_C],
        bucket_evidence="bilateral",
        surviving_count=3,
    )
    assert record_zero.reason_detail != record_multi.reason_detail
