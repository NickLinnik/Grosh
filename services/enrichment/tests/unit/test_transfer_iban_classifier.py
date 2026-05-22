"""Unit tests for is_consistent and classify_pair_evidence.

Test cases per `references/consumer-transfer-detection-test-suite.md` §4.
Skipped at module level until iban_classifier.py lands (Slice 17 task T8).
The corresponding implementation task removes this marker.
"""

from uuid import uuid4

import pytest

from grosh_enrichment.sources.monobank.transfer.iban_classifier import (
    classify_pair_evidence,
    is_consistent,
)
from tests.unit.conftest import (
    UAH_BLACK,
    UAH_FOP,
    make_row_flags,
)

# Fixed account IDs for the consistency tests — incoming on UAH_FOP, candidate
# on UAH_BLACK.
_INCOMING_ACCOUNT_ID = uuid4()
_CANDIDATE_ACCOUNT_ID = uuid4()
_THIRD_ACCOUNT_ID = uuid4()  # neither party — used for "wrong" pointer tests

_INCOMING_IBAN = UAH_FOP.iban
_CANDIDATE_IBAN = UAH_BLACK.iban
_THIRD_IBAN = "UA777000000000000000000777"


# ---------------------------------------------------------------------------
# §4.1  is_consistent — hard filter (tests 80-93)
# ---------------------------------------------------------------------------


def test_80_both_null_consistent():
    incoming = make_row_flags(cp_iban_status="null")
    candidate = make_row_flags(cp_iban_status="null")
    assert (
        is_consistent(
            incoming_flags=incoming,
            candidate_flags=candidate,
            incoming_cp_iban=None,
            candidate_cp_iban=None,
            incoming_account_id=_INCOMING_ACCOUNT_ID,
            candidate_account_id=_CANDIDATE_ACCOUNT_ID,
            iban_to_account={},
        )
        is True
    )


def test_81_both_honest_pointing_correctly_consistent():
    incoming = make_row_flags(cp_iban_status="honest")
    candidate = make_row_flags(cp_iban_status="honest")
    # incoming.cp_iban resolves to candidate's account;
    # candidate.cp_iban resolves to incoming's.
    lookup = {
        _CANDIDATE_IBAN: _CANDIDATE_ACCOUNT_ID,
        _INCOMING_IBAN: _INCOMING_ACCOUNT_ID,
    }
    assert (
        is_consistent(
            incoming_flags=incoming,
            candidate_flags=candidate,
            incoming_cp_iban=_CANDIDATE_IBAN,
            candidate_cp_iban=_INCOMING_IBAN,
            incoming_account_id=_INCOMING_ACCOUNT_ID,
            candidate_account_id=_CANDIDATE_ACCOUNT_ID,
            iban_to_account=lookup,
        )
        is True
    )


def test_82_incoming_honest_pointing_at_candidate_candidate_null_consistent():
    incoming = make_row_flags(cp_iban_status="honest")
    candidate = make_row_flags(cp_iban_status="null")
    lookup = {_CANDIDATE_IBAN: _CANDIDATE_ACCOUNT_ID}
    assert (
        is_consistent(
            incoming_flags=incoming,
            candidate_flags=candidate,
            incoming_cp_iban=_CANDIDATE_IBAN,
            candidate_cp_iban=None,
            incoming_account_id=_INCOMING_ACCOUNT_ID,
            candidate_account_id=_CANDIDATE_ACCOUNT_ID,
            iban_to_account=lookup,
        )
        is True
    )


def test_83_candidate_honest_pointing_at_incoming_incoming_null_consistent():
    incoming = make_row_flags(cp_iban_status="null")
    candidate = make_row_flags(cp_iban_status="honest")
    lookup = {_INCOMING_IBAN: _INCOMING_ACCOUNT_ID}
    assert (
        is_consistent(
            incoming_flags=incoming,
            candidate_flags=candidate,
            incoming_cp_iban=None,
            candidate_cp_iban=_INCOMING_IBAN,
            incoming_account_id=_INCOMING_ACCOUNT_ID,
            candidate_account_id=_CANDIDATE_ACCOUNT_ID,
            iban_to_account=lookup,
        )
        is True
    )


def test_84_incoming_honest_pointing_at_wrong_account_inconsistent():
    # Critical: incoming claims a specific account but it's NOT the candidate's.
    incoming = make_row_flags(cp_iban_status="honest")
    candidate = make_row_flags(cp_iban_status="null")
    lookup = {_THIRD_IBAN: _THIRD_ACCOUNT_ID}
    assert (
        is_consistent(
            incoming_flags=incoming,
            candidate_flags=candidate,
            incoming_cp_iban=_THIRD_IBAN,  # points at a third account, not candidate
            candidate_cp_iban=None,
            incoming_account_id=_INCOMING_ACCOUNT_ID,
            candidate_account_id=_CANDIDATE_ACCOUNT_ID,
            iban_to_account=lookup,
        )
        is False
    )


def test_85_candidate_honest_pointing_at_wrong_account_inconsistent():
    incoming = make_row_flags(cp_iban_status="null")
    candidate = make_row_flags(cp_iban_status="honest")
    lookup = {_THIRD_IBAN: _THIRD_ACCOUNT_ID}
    assert (
        is_consistent(
            incoming_flags=incoming,
            candidate_flags=candidate,
            incoming_cp_iban=None,
            candidate_cp_iban=_THIRD_IBAN,  # points at third account, not incoming
            incoming_account_id=_INCOMING_ACCOUNT_ID,
            candidate_account_id=_CANDIDATE_ACCOUNT_ID,
            iban_to_account=lookup,
        )
        is False
    )


def test_86_incoming_transitive_candidate_honest_pointing_at_incoming_consistent():
    # Incoming's IBAN is suppressed (transitive) — can't contradict.
    # Candidate's honest IBAN points at incoming → satisfied.
    incoming = make_row_flags(cp_iban_status="transitive")
    candidate = make_row_flags(cp_iban_status="honest")
    lookup = {_INCOMING_IBAN: _INCOMING_ACCOUNT_ID}
    assert (
        is_consistent(
            incoming_flags=incoming,
            candidate_flags=candidate,
            incoming_cp_iban=_CANDIDATE_IBAN,  # transitive — suppressed, not evaluated
            candidate_cp_iban=_INCOMING_IBAN,
            incoming_account_id=_INCOMING_ACCOUNT_ID,
            candidate_account_id=_CANDIDATE_ACCOUNT_ID,
            iban_to_account=lookup,
        )
        is True
    )


def test_87_incoming_transitive_candidate_honest_pointing_at_wrong_inconsistent():
    # Candidate's honest claim is binding regardless of incoming's status.
    incoming = make_row_flags(cp_iban_status="transitive")
    candidate = make_row_flags(cp_iban_status="honest")
    lookup = {_THIRD_IBAN: _THIRD_ACCOUNT_ID}
    assert (
        is_consistent(
            incoming_flags=incoming,
            candidate_flags=candidate,
            incoming_cp_iban=_CANDIDATE_IBAN,
            candidate_cp_iban=_THIRD_IBAN,  # points at wrong account
            incoming_account_id=_INCOMING_ACCOUNT_ID,
            candidate_account_id=_CANDIDATE_ACCOUNT_ID,
            iban_to_account=lookup,
        )
        is False
    )


def test_88_candidate_transitive_incoming_honest_pointing_at_candidate_consistent():
    # Candidate's transitive IBAN suppressed; incoming's honest claim satisfied.
    incoming = make_row_flags(cp_iban_status="honest")
    candidate = make_row_flags(cp_iban_status="transitive")
    lookup = {_CANDIDATE_IBAN: _CANDIDATE_ACCOUNT_ID}
    assert (
        is_consistent(
            incoming_flags=incoming,
            candidate_flags=candidate,
            incoming_cp_iban=_CANDIDATE_IBAN,
            candidate_cp_iban=_INCOMING_IBAN,  # transitive — suppressed
            incoming_account_id=_INCOMING_ACCOUNT_ID,
            candidate_account_id=_CANDIDATE_ACCOUNT_ID,
            iban_to_account=lookup,
        )
        is True
    )


def test_89_both_transitive_consistent():
    incoming = make_row_flags(cp_iban_status="transitive")
    candidate = make_row_flags(cp_iban_status="transitive")
    assert (
        is_consistent(
            incoming_flags=incoming,
            candidate_flags=candidate,
            incoming_cp_iban=_CANDIDATE_IBAN,
            candidate_cp_iban=_INCOMING_IBAN,
            incoming_account_id=_INCOMING_ACCOUNT_ID,
            candidate_account_id=_CANDIDATE_ACCOUNT_ID,
            iban_to_account={},
        )
        is True
    )


def test_90_incoming_null_candidate_unlinked_inconsistent():
    # Candidate has explicitly claimed its partner is outside the platform.
    incoming = make_row_flags(cp_iban_status="null")
    candidate = make_row_flags(cp_iban_status="unlinked")
    assert (
        is_consistent(
            incoming_flags=incoming,
            candidate_flags=candidate,
            incoming_cp_iban=None,
            candidate_cp_iban="UA999000000000000000000999",  # unlinked IBAN
            incoming_account_id=_INCOMING_ACCOUNT_ID,
            candidate_account_id=_CANDIDATE_ACCOUNT_ID,
            iban_to_account={},
        )
        is False
    )


def test_91_incoming_transitive_candidate_unlinked_inconsistent():
    incoming = make_row_flags(cp_iban_status="transitive")
    candidate = make_row_flags(cp_iban_status="unlinked")
    assert (
        is_consistent(
            incoming_flags=incoming,
            candidate_flags=candidate,
            incoming_cp_iban=_CANDIDATE_IBAN,
            candidate_cp_iban="UA999000000000000000000999",
            incoming_account_id=_INCOMING_ACCOUNT_ID,
            candidate_account_id=_CANDIDATE_ACCOUNT_ID,
            iban_to_account={},
        )
        is False
    )


def test_92_incoming_honest_pointing_at_candidate_candidate_unlinked_inconsistent():
    # Candidate's unlinked claim wins: cp_iban definitionally doesn't resolve to
    # any own account, so it can't resolve to incoming's account either.
    incoming = make_row_flags(cp_iban_status="honest")
    candidate = make_row_flags(cp_iban_status="unlinked")
    lookup = {_CANDIDATE_IBAN: _CANDIDATE_ACCOUNT_ID}
    assert (
        is_consistent(
            incoming_flags=incoming,
            candidate_flags=candidate,
            incoming_cp_iban=_CANDIDATE_IBAN,
            candidate_cp_iban="UA999000000000000000000999",
            incoming_account_id=_INCOMING_ACCOUNT_ID,
            candidate_account_id=_CANDIDATE_ACCOUNT_ID,
            iban_to_account=lookup,
        )
        is False
    )


@pytest.mark.parametrize(
    "incoming_status",
    ["null", "transitive", "honest"],
)
def test_93_candidate_null_incoming_various_consistent(incoming_status):
    # Candidate makes no claim → always consistent, regardless of incoming's status.
    incoming = make_row_flags(cp_iban_status=incoming_status)
    candidate = make_row_flags(cp_iban_status="null")
    lookup = {_CANDIDATE_IBAN: _CANDIDATE_ACCOUNT_ID}
    assert (
        is_consistent(
            incoming_flags=incoming,
            candidate_flags=candidate,
            incoming_cp_iban=_CANDIDATE_IBAN if incoming_status == "honest" else None,
            candidate_cp_iban=None,
            incoming_account_id=_INCOMING_ACCOUNT_ID,
            candidate_account_id=_CANDIDATE_ACCOUNT_ID,
            iban_to_account=lookup,
        )
        is True
    )


# ---------------------------------------------------------------------------
# §4.2  classify_pair_evidence — level assignment (tests 94-101)
# ---------------------------------------------------------------------------


def test_94_both_honest_pointing_at_each_other_bilateral():
    incoming = make_row_flags(cp_iban_status="honest")
    candidate = make_row_flags(cp_iban_status="honest")
    lookup = {
        _CANDIDATE_IBAN: _CANDIDATE_ACCOUNT_ID,
        _INCOMING_IBAN: _INCOMING_ACCOUNT_ID,
    }
    result = classify_pair_evidence(
        incoming_flags=incoming,
        candidate_flags=candidate,
        incoming_cp_iban=_CANDIDATE_IBAN,
        candidate_cp_iban=_INCOMING_IBAN,
        incoming_account_id=_INCOMING_ACCOUNT_ID,
        candidate_account_id=_CANDIDATE_ACCOUNT_ID,
        iban_to_account=lookup,
    )
    assert result == "bilateral"


def test_95_incoming_honest_pointing_at_candidate_candidate_null_unilateral():
    incoming = make_row_flags(cp_iban_status="honest")
    candidate = make_row_flags(cp_iban_status="null")
    lookup = {_CANDIDATE_IBAN: _CANDIDATE_ACCOUNT_ID}
    result = classify_pair_evidence(
        incoming_flags=incoming,
        candidate_flags=candidate,
        incoming_cp_iban=_CANDIDATE_IBAN,
        candidate_cp_iban=None,
        incoming_account_id=_INCOMING_ACCOUNT_ID,
        candidate_account_id=_CANDIDATE_ACCOUNT_ID,
        iban_to_account=lookup,
    )
    assert result == "unilateral"


def test_96_incoming_honest_pointing_at_candidate_candidate_transitive_unilateral():
    # Transitive doesn't contribute; honest one side is enough for unilateral.
    incoming = make_row_flags(cp_iban_status="honest")
    candidate = make_row_flags(cp_iban_status="transitive")
    lookup = {_CANDIDATE_IBAN: _CANDIDATE_ACCOUNT_ID}
    result = classify_pair_evidence(
        incoming_flags=incoming,
        candidate_flags=candidate,
        incoming_cp_iban=_CANDIDATE_IBAN,
        candidate_cp_iban=_INCOMING_IBAN,  # transitive — suppressed
        incoming_account_id=_INCOMING_ACCOUNT_ID,
        candidate_account_id=_CANDIDATE_ACCOUNT_ID,
        iban_to_account=lookup,
    )
    assert result == "unilateral"


def test_97_incoming_null_candidate_honest_pointing_at_incoming_unilateral():
    incoming = make_row_flags(cp_iban_status="null")
    candidate = make_row_flags(cp_iban_status="honest")
    lookup = {_INCOMING_IBAN: _INCOMING_ACCOUNT_ID}
    result = classify_pair_evidence(
        incoming_flags=incoming,
        candidate_flags=candidate,
        incoming_cp_iban=None,
        candidate_cp_iban=_INCOMING_IBAN,
        incoming_account_id=_INCOMING_ACCOUNT_ID,
        candidate_account_id=_CANDIDATE_ACCOUNT_ID,
        iban_to_account=lookup,
    )
    assert result == "unilateral"


def test_98_incoming_transitive_candidate_honest_pointing_at_incoming_unilateral():
    incoming = make_row_flags(cp_iban_status="transitive")
    candidate = make_row_flags(cp_iban_status="honest")
    lookup = {_INCOMING_IBAN: _INCOMING_ACCOUNT_ID}
    result = classify_pair_evidence(
        incoming_flags=incoming,
        candidate_flags=candidate,
        incoming_cp_iban=_CANDIDATE_IBAN,  # transitive — suppressed
        candidate_cp_iban=_INCOMING_IBAN,
        incoming_account_id=_INCOMING_ACCOUNT_ID,
        candidate_account_id=_CANDIDATE_ACCOUNT_ID,
        iban_to_account=lookup,
    )
    assert result == "unilateral"


def test_99_both_null_none():
    incoming = make_row_flags(cp_iban_status="null")
    candidate = make_row_flags(cp_iban_status="null")
    result = classify_pair_evidence(
        incoming_flags=incoming,
        candidate_flags=candidate,
        incoming_cp_iban=None,
        candidate_cp_iban=None,
        incoming_account_id=_INCOMING_ACCOUNT_ID,
        candidate_account_id=_CANDIDATE_ACCOUNT_ID,
        iban_to_account={},
    )
    assert result == "none"


def test_100_both_transitive_none():
    incoming = make_row_flags(cp_iban_status="transitive")
    candidate = make_row_flags(cp_iban_status="transitive")
    result = classify_pair_evidence(
        incoming_flags=incoming,
        candidate_flags=candidate,
        incoming_cp_iban=_CANDIDATE_IBAN,
        candidate_cp_iban=_INCOMING_IBAN,
        incoming_account_id=_INCOMING_ACCOUNT_ID,
        candidate_account_id=_CANDIDATE_ACCOUNT_ID,
        iban_to_account={},
    )
    assert result == "none"


def test_101_one_null_one_transitive_none():
    incoming = make_row_flags(cp_iban_status="null")
    candidate = make_row_flags(cp_iban_status="transitive")
    result = classify_pair_evidence(
        incoming_flags=incoming,
        candidate_flags=candidate,
        incoming_cp_iban=None,
        candidate_cp_iban=_INCOMING_IBAN,
        incoming_account_id=_INCOMING_ACCOUNT_ID,
        candidate_account_id=_CANDIDATE_ACCOUNT_ID,
        iban_to_account={},
    )
    assert result == "none"
