"""Unit tests for decide().

Test cases per `references/consumer-transfer-detection-test-suite.md` §7.
Skipped at module level until decision.py lands (Slice 17 task T11).
The corresponding implementation task removes this marker.
"""

import dataclasses
from uuid import uuid4

import pytest

from grosh_pipeline.sources.monobank.transfer.decision import (
    Anomaly,
    Claim,
    Skip,
    decide,
)
from tests.unit.conftest import (
    EUR_FOP,
    UAH_BLACK,
    UAH_FOP,
    UAH_WHITE,
    USD_FOP,
    make_candidate,
)

# Fixed account IDs wired into AccountProps fixtures.
_INCOMING_ACCOUNT_ID = uuid4()
_CANDIDATE_A_ID = uuid4()
_CANDIDATE_B_ID = uuid4()
_CANDIDATE_C_ID = uuid4()


# ---------------------------------------------------------------------------
# §7.1  count == 0 (tests 121-124)
# ---------------------------------------------------------------------------


def test_121_count_zero_no_transfer_prefix_returns_skip():
    # Description has no transfer prefix → Skip, no anomaly.
    result = decide(
        incoming_id=uuid4(),
        candidates=[],
        incoming_description="Олена К.",
        incoming_account_id=_INCOMING_ACCOUNT_ID,
        incoming_acc_props=UAH_FOP,
        account_props_by_id={},
    )
    assert isinstance(result, Skip)


def test_122_count_zero_z_prefix_returns_anomaly_unpaired_from():
    result = decide(
        incoming_id=uuid4(),
        candidates=[],
        incoming_description="З Чорної картки",
        incoming_account_id=_INCOMING_ACCOUNT_ID,
        incoming_acc_props=UAH_FOP,
        account_props_by_id={},
    )
    assert isinstance(result, Anomaly)
    assert result.record.reason_code == "unpaired_from_description"


def test_123_count_zero_na_prefix_returns_anomaly_unpaired_to():
    result = decide(
        incoming_id=uuid4(),
        candidates=[],
        incoming_description="На білу картку",
        incoming_account_id=_INCOMING_ACCOUNT_ID,
        incoming_acc_props=UAH_BLACK,
        account_props_by_id={},
    )
    assert isinstance(result, Anomaly)
    assert result.record.reason_code == "unpaired_to_description"


def test_124_count_zero_perekazz_returns_skip():
    # "Переказ на картку" is the explicit allowed phrase; per ADR §6, count==0
    # emits unpaired only on prefix family, not on the explicit phrase alone.
    result = decide(
        incoming_id=uuid4(),
        candidates=[],
        incoming_description="Переказ на картку",
        incoming_account_id=_INCOMING_ACCOUNT_ID,
        incoming_acc_props=UAH_BLACK,
        account_props_by_id={},
    )
    assert isinstance(result, Skip)


# ---------------------------------------------------------------------------
# §7.2  count == 1 (tests 125-129b)
# ---------------------------------------------------------------------------


def test_125_count_one_descriptions_consistent_returns_claim():
    # Incoming EXPENSE on UAH_FOP; candidate INCOME on UAH_BLACK.
    # expense desc "На чорну картку" → (black) → checks income account UAH_BLACK → OK.
    # income desc "З гривневого рахунку ФОП" → (fop,UAH) → checks expense UAH_FOP → OK.
    cand_id = uuid4()
    cand_account_id = uuid4()
    candidate = make_candidate(
        id=cand_id,
        account_id=cand_account_id,
        direction="income",
        description="З гривневого рахунку ФОП",
        amount_cents=10_000,
        operation_amount_cents=10_000,
        counterparty_iban=None,
    )
    result = decide(
        incoming_id=uuid4(),
        candidates=[(candidate, "none")],
        incoming_description="На чорну картку",
        incoming_account_id=_INCOMING_ACCOUNT_ID,
        incoming_acc_props=UAH_FOP,
        account_props_by_id={cand_account_id: UAH_BLACK},
    )
    assert isinstance(result, Claim)
    assert result.partner_id == cand_id
    assert result.description_decisive is False
    assert result.canary_anomaly is None


def test_126_count_one_descriptions_inconsistent_claim_with_canary():
    # Descriptions disagree but claim proceeds (canary semantics).
    # income desc "З Білої картки" says expense is white — but expense account is FOP.
    cand_id = uuid4()
    cand_account_id = uuid4()
    candidate = make_candidate(
        id=cand_id,
        account_id=cand_account_id,
        direction="income",
        description="З Білої картки",
        counterparty_iban=None,
    )
    result = decide(
        incoming_id=uuid4(),
        candidates=[(candidate, "none")],
        incoming_description="На чорну картку",
        incoming_account_id=_INCOMING_ACCOUNT_ID,
        incoming_acc_props=UAH_FOP,  # NOT white — income desc fails against this
        account_props_by_id={cand_account_id: UAH_BLACK},
    )
    assert isinstance(result, Claim)
    assert result.partner_id == cand_id
    assert result.canary_anomaly is not None
    assert result.canary_anomaly.reason_code == "description_consistency_mismatch"


def test_127_count_one_bilateral_evidence_surfaces_in_claim():
    cand_id = uuid4()
    cand_account_id = uuid4()
    candidate = make_candidate(
        id=cand_id,
        account_id=cand_account_id,
        direction="income",
        description=None,
        counterparty_iban=None,
    )
    result = decide(
        incoming_id=uuid4(),
        candidates=[(candidate, "bilateral")],
        incoming_description=None,
        incoming_account_id=_INCOMING_ACCOUNT_ID,
        incoming_acc_props=UAH_FOP,
        account_props_by_id={cand_account_id: UAH_BLACK},
    )
    assert isinstance(result, Claim)
    assert result.iban_evidence == "bilateral"


def test_128_count_one_unilateral_evidence():
    cand_id = uuid4()
    cand_account_id = uuid4()
    candidate = make_candidate(id=cand_id, account_id=cand_account_id)
    result = decide(
        incoming_id=uuid4(),
        candidates=[(candidate, "unilateral")],
        incoming_description=None,
        incoming_account_id=_INCOMING_ACCOUNT_ID,
        incoming_acc_props=UAH_FOP,
        account_props_by_id={cand_account_id: UAH_BLACK},
    )
    assert isinstance(result, Claim)
    assert result.iban_evidence == "unilateral"


def test_129_count_one_none_evidence_still_claims():
    # Single consistency-filter survivor with none evidence → claim.
    # The count==1 path doesn't reject none-evidence claims.
    cand_id = uuid4()
    cand_account_id = uuid4()
    candidate = make_candidate(id=cand_id, account_id=cand_account_id)
    result = decide(
        incoming_id=uuid4(),
        candidates=[(candidate, "none")],
        incoming_description=None,
        incoming_account_id=_INCOMING_ACCOUNT_ID,
        incoming_acc_props=UAH_FOP,
        account_props_by_id={cand_account_id: UAH_BLACK},
    )
    assert isinstance(result, Claim)
    assert result.iban_evidence == "none"


def test_129b_count_one_none_evidence_inconsistent_desc_claim_with_canary():
    # Riskiest claim profile: no IBAN signal AND descriptions disagree.
    # count==1 still claims; canary is attached.
    # income desc "З Білої картки" says expense is white — but expense is FOP.
    cand_id = uuid4()
    cand_account_id = uuid4()
    candidate = make_candidate(
        id=cand_id,
        account_id=cand_account_id,
        direction="income",
        description="З Білої картки",
        counterparty_iban=None,
    )
    result = decide(
        incoming_id=uuid4(),
        candidates=[(candidate, "none")],
        incoming_description="На чорну картку",
        incoming_account_id=_INCOMING_ACCOUNT_ID,
        incoming_acc_props=UAH_FOP,
        account_props_by_id={cand_account_id: UAH_BLACK},
    )
    assert isinstance(result, Claim)
    assert result.iban_evidence == "none"
    assert result.description_decisive is False
    assert result.canary_anomaly is not None
    assert result.canary_anomaly.reason_code == "description_consistency_mismatch"


# ---------------------------------------------------------------------------
# §7.3  count > 1 — bucket-locked principle (tests 130-138)
# ---------------------------------------------------------------------------


def test_130_two_bilateral_both_valid_ambiguous():
    # Both bilateral, both descriptions valid → 2 survivors → ambiguous_pair_match.
    cand_a_account = uuid4()
    cand_b_account = uuid4()
    cand_a = make_candidate(
        id=_CANDIDATE_A_ID,
        account_id=cand_a_account,
        direction="income",
        description=None,
    )
    cand_b = make_candidate(
        id=_CANDIDATE_B_ID,
        account_id=cand_b_account,
        direction="income",
        description=None,
    )
    result = decide(
        incoming_id=uuid4(),
        candidates=[(cand_a, "bilateral"), (cand_b, "bilateral")],
        incoming_description=None,
        incoming_account_id=_INCOMING_ACCOUNT_ID,
        incoming_acc_props=UAH_FOP,
        account_props_by_id={cand_a_account: UAH_BLACK, cand_b_account: UAH_WHITE},
    )
    assert isinstance(result, Anomaly)
    assert result.record.reason_code == "ambiguous_pair_match"
    assert "bilateral" in result.record.reason_detail
    assert _CANDIDATE_A_ID in result.record.candidate_ids
    assert _CANDIDATE_B_ID in result.record.candidate_ids


def test_131_two_bilateral_description_narrows_to_one_claims():
    # Both bilateral; description filter eliminates one → claim survivor.
    # cand_a income desc "З Чорної картки" → constraint type=black;
    #   expense account is UAH_FOP (fop) → FAIL.
    # cand_b income desc "З гривневого рахунку ФОП" → constraint fop,UAH;
    #   expense account is UAH_FOP → OK.
    cand_a_account = uuid4()
    cand_b_account = uuid4()
    cand_a = make_candidate(
        id=_CANDIDATE_A_ID,
        account_id=cand_a_account,
        direction="income",
        description="З Чорної картки",
    )
    cand_b = make_candidate(
        id=_CANDIDATE_B_ID,
        account_id=cand_b_account,
        direction="income",
        description="З гривневого рахунку ФОП",
    )
    result = decide(
        incoming_id=uuid4(),
        candidates=[(cand_a, "bilateral"), (cand_b, "bilateral")],
        incoming_description=None,
        incoming_account_id=_INCOMING_ACCOUNT_ID,
        incoming_acc_props=UAH_FOP,
        account_props_by_id={cand_a_account: UAH_BLACK, cand_b_account: UAH_BLACK},
    )
    assert isinstance(result, Claim)
    assert result.partner_id == _CANDIDATE_B_ID
    assert result.iban_evidence == "bilateral"
    assert result.description_decisive is True


def test_132_two_bilateral_description_eliminates_all_ambiguous():
    # Both bilateral; description filter eliminates both → ambiguous_pair_match.
    # Detail notes evidence "bilateral" and description filter eliminated all.
    # Do NOT fall through to weaker bucket.
    # Both candidate income descs say "З Білої картки" (white);
    # expense account is UAH_FOP → constraint type=white fails → both FAIL.
    cand_a_account = uuid4()
    cand_b_account = uuid4()
    cand_a = make_candidate(
        id=_CANDIDATE_A_ID,
        account_id=cand_a_account,
        direction="income",
        description="З Білої картки",
    )
    cand_b = make_candidate(
        id=_CANDIDATE_B_ID,
        account_id=cand_b_account,
        direction="income",
        description="З Білої картки",
    )
    result = decide(
        incoming_id=uuid4(),
        candidates=[(cand_a, "bilateral"), (cand_b, "bilateral")],
        incoming_description=None,
        incoming_account_id=_INCOMING_ACCOUNT_ID,
        incoming_acc_props=UAH_FOP,
        account_props_by_id={cand_a_account: UAH_BLACK, cand_b_account: UAH_WHITE},
    )
    assert isinstance(result, Anomaly)
    assert result.record.reason_code == "ambiguous_pair_match"
    assert "bilateral" in result.record.reason_detail


def test_133_mixed_bilateral_plus_unilateral_picks_bilateral():
    # 1 bilateral + 2 unilateral. Bucket-locked picks bilateral.
    # Single bilateral candidate → count==1 path → Claim.
    # The two unilateral candidates are never considered.
    cand_bil_account = uuid4()
    cand_uni_a_account = uuid4()
    cand_uni_b_account = uuid4()
    cand_bil = make_candidate(
        id=_CANDIDATE_A_ID, account_id=cand_bil_account, direction="income"
    )
    cand_uni_a = make_candidate(
        id=_CANDIDATE_B_ID, account_id=cand_uni_a_account, direction="income"
    )
    cand_uni_b = make_candidate(
        id=_CANDIDATE_C_ID, account_id=cand_uni_b_account, direction="income"
    )
    result = decide(
        incoming_id=uuid4(),
        candidates=[
            (cand_bil, "bilateral"),
            (cand_uni_a, "unilateral"),
            (cand_uni_b, "unilateral"),
        ],
        incoming_description=None,
        incoming_account_id=_INCOMING_ACCOUNT_ID,
        incoming_acc_props=UAH_FOP,
        account_props_by_id={
            cand_bil_account: UAH_BLACK,
            cand_uni_a_account: UAH_WHITE,
            cand_uni_b_account: EUR_FOP,
        },
    )
    assert isinstance(result, Claim)
    assert result.partner_id == _CANDIDATE_A_ID
    assert result.iban_evidence == "bilateral"


def test_133b_all_three_buckets_nonempty_picks_bilateral_short_circuits():
    # 1 bilateral, 2 unilateral, 3 none candidates.
    # Bucket-locked picks bilateral (single member → count==1 → Claim).
    # The 5 candidates in lower buckets are NEVER evaluated.
    fourth_id = uuid4()
    fifth_id = uuid4()
    sixth_id = uuid4()
    bil_account = uuid4()
    uni_a_account = uuid4()
    uni_b_account = uuid4()
    none_a_account = uuid4()
    none_b_account = uuid4()
    none_c_account = uuid4()

    cand_bil = make_candidate(
        id=_CANDIDATE_A_ID, account_id=bil_account, direction="income"
    )
    cand_uni_a = make_candidate(
        id=_CANDIDATE_B_ID, account_id=uni_a_account, direction="income"
    )
    cand_uni_b = make_candidate(
        id=_CANDIDATE_C_ID, account_id=uni_b_account, direction="income"
    )
    cand_none_a = make_candidate(
        id=fourth_id, account_id=none_a_account, direction="income"
    )
    cand_none_b = make_candidate(
        id=fifth_id, account_id=none_b_account, direction="income"
    )
    cand_none_c = make_candidate(
        id=sixth_id, account_id=none_c_account, direction="income"
    )

    result = decide(
        incoming_id=uuid4(),
        candidates=[
            (cand_bil, "bilateral"),
            (cand_uni_a, "unilateral"),
            (cand_uni_b, "unilateral"),
            (cand_none_a, "none"),
            (cand_none_b, "none"),
            (cand_none_c, "none"),
        ],
        incoming_description=None,
        incoming_account_id=_INCOMING_ACCOUNT_ID,
        incoming_acc_props=UAH_FOP,
        account_props_by_id={
            bil_account: UAH_BLACK,
            uni_a_account: UAH_WHITE,
            uni_b_account: EUR_FOP,
            none_a_account: UAH_FOP,
            none_b_account: USD_FOP,
            none_c_account: EUR_FOP,
        },
    )
    assert isinstance(result, Claim)
    assert result.partner_id == _CANDIDATE_A_ID
    assert result.iban_evidence == "bilateral"


def test_134_zero_bilateral_two_unilateral_description_narrows_to_one():
    # cand_a income desc "З Білої картки" → expense should be white; FOP → FAIL.
    # cand_b income desc "З гривневого рахунку ФОП" → expense fop/UAH; UAH_FOP → OK.
    cand_a_account = uuid4()
    cand_b_account = uuid4()
    cand_a = make_candidate(
        id=_CANDIDATE_A_ID,
        account_id=cand_a_account,
        direction="income",
        description="З Білої картки",
    )
    cand_b = make_candidate(
        id=_CANDIDATE_B_ID,
        account_id=cand_b_account,
        direction="income",
        description="З гривневого рахунку ФОП",
    )
    result = decide(
        incoming_id=uuid4(),
        candidates=[(cand_a, "unilateral"), (cand_b, "unilateral")],
        incoming_description=None,
        incoming_account_id=_INCOMING_ACCOUNT_ID,
        incoming_acc_props=UAH_FOP,
        account_props_by_id={cand_a_account: UAH_BLACK, cand_b_account: UAH_BLACK},
    )
    assert isinstance(result, Claim)
    assert result.partner_id == _CANDIDATE_B_ID
    assert result.iban_evidence == "unilateral"
    assert result.description_decisive is True


def test_135_zero_bilateral_zero_unilateral_three_none_all_fail_desc():
    # none-evidence bucket; description hard filter rejects all 3 →
    # description_account_mismatch (special case for none-evidence + full rejection).
    # All candidate income descs say "З Білої картки" (white);
    # expense account is UAH_FOP → constraint type=white fails → all FAIL.
    cand_a_account = uuid4()
    cand_b_account = uuid4()
    cand_c_account = uuid4()
    cand_a = make_candidate(
        id=_CANDIDATE_A_ID,
        account_id=cand_a_account,
        direction="income",
        description="З Білої картки",
    )
    cand_b = make_candidate(
        id=_CANDIDATE_B_ID,
        account_id=cand_b_account,
        direction="income",
        description="З Білої картки",
    )
    cand_c = make_candidate(
        id=_CANDIDATE_C_ID,
        account_id=cand_c_account,
        direction="income",
        description="З Білої картки",
    )
    result = decide(
        incoming_id=uuid4(),
        candidates=[(cand_a, "none"), (cand_b, "none"), (cand_c, "none")],
        incoming_description=None,
        incoming_account_id=_INCOMING_ACCOUNT_ID,
        incoming_acc_props=UAH_FOP,
        account_props_by_id={
            cand_a_account: UAH_BLACK,
            cand_b_account: UAH_BLACK,
            cand_c_account: UAH_WHITE,
        },
    )
    assert isinstance(result, Anomaly)
    assert result.record.reason_code == "description_account_mismatch"


def test_136_zero_bilateral_zero_unilateral_two_none_both_survive_ambiguous():
    # none-evidence, description passes both → still ambiguous (2 survivors).
    cand_a_account = uuid4()
    cand_b_account = uuid4()
    cand_a = make_candidate(
        id=_CANDIDATE_A_ID,
        account_id=cand_a_account,
        direction="income",
        description=None,
    )
    cand_b = make_candidate(
        id=_CANDIDATE_B_ID,
        account_id=cand_b_account,
        direction="income",
        description=None,
    )
    result = decide(
        incoming_id=uuid4(),
        candidates=[(cand_a, "none"), (cand_b, "none")],
        incoming_description=None,
        incoming_account_id=_INCOMING_ACCOUNT_ID,
        incoming_acc_props=UAH_FOP,
        account_props_by_id={cand_a_account: UAH_BLACK, cand_b_account: UAH_WHITE},
    )
    assert isinstance(result, Anomaly)
    assert result.record.reason_code == "ambiguous_pair_match"
    assert "none" in result.record.reason_detail


def test_137_zero_bilateral_zero_unilateral_one_none_claims():
    cand_account = uuid4()
    cand = make_candidate(
        id=_CANDIDATE_A_ID, account_id=cand_account, direction="income"
    )
    result = decide(
        incoming_id=uuid4(),
        candidates=[(cand, "none")],
        incoming_description=None,
        incoming_account_id=_INCOMING_ACCOUNT_ID,
        incoming_acc_props=UAH_FOP,
        account_props_by_id={cand_account: UAH_BLACK},
    )
    assert isinstance(result, Claim)
    assert result.iban_evidence == "none"


def test_138_bucket_locked_invariant_bilateral_fails_desc_no_fallthrough():
    # 1 bilateral, 1 unilateral. Description filter eliminates the bilateral candidate.
    # Bucket-locked → ambiguous_pair_match. Do NOT look at unilateral.
    # Bilateral candidate income desc "З Білої картки" → expense should be white;
    # expense is FOP → FAIL.  Unilateral candidate has no description → would pass.
    bil_account = uuid4()
    uni_account = uuid4()
    cand_bil = make_candidate(
        id=_CANDIDATE_A_ID,
        account_id=bil_account,
        direction="income",
        description="З Білої картки",
    )
    cand_uni = make_candidate(
        id=_CANDIDATE_B_ID,
        account_id=uni_account,
        direction="income",
        description=None,
    )
    result = decide(
        incoming_id=uuid4(),
        candidates=[(cand_bil, "bilateral"), (cand_uni, "unilateral")],
        incoming_description=None,
        incoming_account_id=_INCOMING_ACCOUNT_ID,
        incoming_acc_props=UAH_FOP,
        account_props_by_id={bil_account: UAH_BLACK, uni_account: UAH_WHITE},
    )
    # Must NOT claim the unilateral candidate.
    assert isinstance(result, Anomaly)
    assert result.record.reason_code == "ambiguous_pair_match"


# ---------------------------------------------------------------------------
# §7.4  Decision discriminated union shape (tests 139-142)
# ---------------------------------------------------------------------------


def test_139_claim_carries_required_fields():
    cand_id = uuid4()
    cand_account = uuid4()
    cand = make_candidate(id=cand_id, account_id=cand_account, direction="income")
    result = decide(
        incoming_id=uuid4(),
        candidates=[(cand, "bilateral")],
        incoming_description=None,
        incoming_account_id=_INCOMING_ACCOUNT_ID,
        incoming_acc_props=UAH_FOP,
        account_props_by_id={cand_account: UAH_BLACK},
    )
    assert isinstance(result, Claim)
    assert isinstance(result.partner_id, type(cand_id))  # UUID
    assert result.iban_evidence in ("bilateral", "unilateral", "none")
    assert isinstance(result.description_decisive, bool)
    assert result.canary_anomaly is None or hasattr(
        result.canary_anomaly, "reason_code"
    )


def test_140_anomaly_carries_single_record():
    result = decide(
        incoming_id=uuid4(),
        candidates=[],
        incoming_description="З Чорної картки",
        incoming_account_id=_INCOMING_ACCOUNT_ID,
        incoming_acc_props=UAH_FOP,
        account_props_by_id={},
    )
    assert isinstance(result, Anomaly)
    # Anomaly carries a single record (not a list).
    assert hasattr(result, "record")
    assert hasattr(result.record, "reason_code")


def test_141_skip_is_sentinel_no_fields():
    result = decide(
        incoming_id=uuid4(),
        candidates=[],
        incoming_description="Олена К.",
        incoming_account_id=_INCOMING_ACCOUNT_ID,
        incoming_acc_props=UAH_FOP,
        account_props_by_id={},
    )
    assert isinstance(result, Skip)


def test_142_decision_types_are_frozen_dataclasses():
    cand_id = uuid4()
    cand_account = uuid4()
    cand = make_candidate(id=cand_id, account_id=cand_account, direction="income")
    claim = decide(
        incoming_id=uuid4(),
        candidates=[(cand, "bilateral")],
        incoming_description=None,
        incoming_account_id=_INCOMING_ACCOUNT_ID,
        incoming_acc_props=UAH_FOP,
        account_props_by_id={cand_account: UAH_BLACK},
    )
    assert isinstance(claim, Claim)
    # Frozen dataclass — mutating any field must raise FrozenInstanceError.
    with pytest.raises(dataclasses.FrozenInstanceError):
        claim.partner_id = uuid4()  # type: ignore[misc]
