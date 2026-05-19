"""Unit tests for build_row_block and build_pair_block.

Test cases per `references/consumer-transfer-detection-test-suite.md` §5.
Skipped at module level until metadata.py lands (Slice 17 task T9).
The corresponding implementation task removes this marker.
"""

import json

import pytest

from grosh_consumer.sources.monobank.transfer.metadata import (
    build_pair_block,
    build_row_block,
)
from tests.unit.conftest import make_row_flags

# ---------------------------------------------------------------------------
# §5.1  build_row_block (tests 102-106)
# ---------------------------------------------------------------------------


def test_102_default_flags_produces_expected_dict():
    flags = make_row_flags()
    block = build_row_block(flags)
    assert block == {
        "description_matched": False,
        "multi_hop_description": False,
        "cp_iban_status": "null",
    }


def test_103_all_true_flags_honest_status():
    flags = make_row_flags(
        description_matched=True,
        multi_hop_description=True,
        cp_iban_status="honest",
    )
    block = build_row_block(flags)
    assert block == {
        "description_matched": True,
        "multi_hop_description": True,
        "cp_iban_status": "honest",
    }


@pytest.mark.parametrize(
    "cp_iban_status",
    ["null", "transitive", "unlinked", "honest"],
)
def test_104_cp_iban_status_round_trips(cp_iban_status):
    flags = make_row_flags(cp_iban_status=cp_iban_status)
    block = build_row_block(flags)
    assert block["cp_iban_status"] == cp_iban_status


def test_105_row_block_has_exactly_three_keys():
    flags = make_row_flags()
    block = build_row_block(flags)
    assert set(block.keys()) == {
        "description_matched",
        "multi_hop_description",
        "cp_iban_status",
    }


def test_106_row_block_is_json_serializable():
    flags = make_row_flags(
        description_matched=True,
        multi_hop_description=False,
        cp_iban_status="transitive",
    )
    block = build_row_block(flags)
    serialized = json.dumps(block)
    assert json.loads(serialized) == block


# ---------------------------------------------------------------------------
# §5.2  build_pair_block (tests 107-112)
# ---------------------------------------------------------------------------


def test_107_bilateral_not_decisive():
    block = build_pair_block("bilateral", False)
    assert block == {"iban_evidence": "bilateral", "description_decisive": False}


def test_108_unilateral_decisive():
    block = build_pair_block("unilateral", True)
    assert block == {"iban_evidence": "unilateral", "description_decisive": True}


def test_109_none_not_decisive():
    block = build_pair_block("none", False)
    assert block == {"iban_evidence": "none", "description_decisive": False}


def test_110_none_decisive_valid_combination():
    # Rare but not rejected by the builder — the decision module is responsible
    # for not constructing this combination in normal operation.
    block = build_pair_block("none", True)
    assert block == {"iban_evidence": "none", "description_decisive": True}


def test_111_pair_block_has_exactly_two_keys():
    block = build_pair_block("bilateral", False)
    assert set(block.keys()) == {"iban_evidence", "description_decisive"}


def test_112_pair_block_is_json_serializable():
    block = build_pair_block("unilateral", True)
    serialized = json.dumps(block)
    assert json.loads(serialized) == block
