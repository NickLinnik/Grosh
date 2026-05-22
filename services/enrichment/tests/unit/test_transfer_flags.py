"""Unit tests for compute_row_flags.

Test cases per `references/consumer-transfer-detection-test-suite.md` §3.
Skipped at module level until flags.py lands (Slice 17 task T7).
The corresponding implementation task removes this marker.
"""

from uuid import uuid4

from grosh_enrichment.sources.monobank.transfer.flags import compute_row_flags
from tests.unit.conftest import (
    EXTERNAL,
    UAH_FOP,
    make_normalized_tx,
)

# ---------------------------------------------------------------------------
# §3.1  description_matched and multi_hop_description (tests 61-67)
# ---------------------------------------------------------------------------


def test_61_description_in_income_map_matched():
    tx = make_normalized_tx(description="З Чорної картки", direction="income")
    flags = compute_row_flags(tx, {})
    assert flags.description_matched is True
    assert flags.multi_hop_description is False


def test_62_description_in_expense_map_matched():
    tx = make_normalized_tx(description="На чорну картку", direction="expense")
    flags = compute_row_flags(tx, {})
    assert flags.description_matched is True
    assert flags.multi_hop_description is False


def test_63_multi_hop_description_matched_and_flagged():
    # Expense multi-hop description — matched AND multi_hop.
    tx = make_normalized_tx(
        description="На доларовий рахунок ФОП для переказу на картку",
        direction="expense",
    )
    flags = compute_row_flags(tx, {})
    assert flags.description_matched is True
    assert flags.multi_hop_description is True


def test_64_unknown_description_both_false():
    tx = make_normalized_tx(description="Олена К.", direction="expense")
    flags = compute_row_flags(tx, {})
    assert flags.description_matched is False
    assert flags.multi_hop_description is False


def test_65_prefix_but_unknown_both_false():
    # "З " prefix but NOT in the known set — description_matched is False.
    tx = make_normalized_tx(description="З невідомого рахунку", direction="income")
    flags = compute_row_flags(tx, {})
    assert flags.description_matched is False
    assert flags.multi_hop_description is False


def test_66_none_description_both_false():
    tx = make_normalized_tx(description=None, direction="expense")
    flags = compute_row_flags(tx, {})
    assert flags.description_matched is False
    assert flags.multi_hop_description is False


def test_67_empty_description_both_false():
    tx = make_normalized_tx(description="", direction="expense")
    flags = compute_row_flags(tx, {})
    assert flags.description_matched is False
    assert flags.multi_hop_description is False


# ---------------------------------------------------------------------------
# §3.2  cp_iban_status — directional transitive rule (tests 68-75)
#
# 4 cells of (direction, multi_hop_description) × IBAN-presence axis.
# ---------------------------------------------------------------------------

_OWN_ACCOUNT_ID = uuid4()
_OWN_IBAN = UAH_FOP.iban


def test_68_expense_multi_hop_iban_resolves_to_own_is_transitive():
    # Even though the IBAN resolves to an own account, it's suppressed because
    # direction=expense AND multi_hop_description.  Evidence-suppression rule.
    tx = make_normalized_tx(
        description="На гривневий рахунок ФОП для переказу на картку",
        direction="expense",
        counterparty_iban=_OWN_IBAN,
    )
    lookup = {_OWN_IBAN: _OWN_ACCOUNT_ID}
    flags = compute_row_flags(tx, lookup)
    assert flags.cp_iban_status == "transitive"


def test_69_expense_multi_hop_iban_does_not_resolve_still_transitive():
    # IBAN doesn't resolve to any own account — rule fires BEFORE the lookup.
    tx = make_normalized_tx(
        description="На доларовий рахунок ФОП для переказу на картку",
        direction="expense",
        counterparty_iban=EXTERNAL,
    )
    lookup = {}
    flags = compute_row_flags(tx, lookup)
    assert flags.cp_iban_status == "transitive"


def test_70_expense_non_multi_hop_iban_resolves_is_honest():
    tx = make_normalized_tx(
        description="На чорну картку",
        direction="expense",
        counterparty_iban=_OWN_IBAN,
    )
    lookup = {_OWN_IBAN: _OWN_ACCOUNT_ID}
    flags = compute_row_flags(tx, lookup)
    assert flags.cp_iban_status == "honest"


def test_71_expense_non_multi_hop_iban_does_not_resolve_is_unlinked():
    tx = make_normalized_tx(
        description="На чорну картку",
        direction="expense",
        counterparty_iban=EXTERNAL,
    )
    lookup = {}
    flags = compute_row_flags(tx, lookup)
    assert flags.cp_iban_status == "unlinked"


def test_72_income_multi_hop_iban_resolves_is_honest():
    # Income side is honest even with multi-hop description —
    # the directional rule applies only to expense.
    tx = make_normalized_tx(
        description="З доларового рахунку ФОП для переказу на картку",
        direction="income",
        counterparty_iban=_OWN_IBAN,
    )
    lookup = {_OWN_IBAN: _OWN_ACCOUNT_ID}
    flags = compute_row_flags(tx, lookup)
    assert flags.cp_iban_status == "honest"


def test_73_income_multi_hop_iban_does_not_resolve_is_unlinked():
    tx = make_normalized_tx(
        description="З доларового рахунку ФОП для переказу на картку",
        direction="income",
        counterparty_iban=EXTERNAL,
    )
    lookup = {}
    flags = compute_row_flags(tx, lookup)
    assert flags.cp_iban_status == "unlinked"


def test_74_income_non_multi_hop_iban_resolves_is_honest():
    tx = make_normalized_tx(
        description="З Чорної картки",
        direction="income",
        counterparty_iban=_OWN_IBAN,
    )
    lookup = {_OWN_IBAN: _OWN_ACCOUNT_ID}
    flags = compute_row_flags(tx, lookup)
    assert flags.cp_iban_status == "honest"


def test_75_income_non_multi_hop_iban_does_not_resolve_is_unlinked():
    tx = make_normalized_tx(
        description="З Чорної картки",
        direction="income",
        counterparty_iban=EXTERNAL,
    )
    lookup = {}
    flags = compute_row_flags(tx, lookup)
    assert flags.cp_iban_status == "unlinked"


# ---------------------------------------------------------------------------
# §3.3  cp_iban_status — null IBAN (test 76)
# ---------------------------------------------------------------------------


def test_76_null_iban_any_direction_any_desc_is_null():
    for direction in ("income", "expense"):
        tx = make_normalized_tx(
            description="З Чорної картки",
            direction=direction,
            counterparty_iban=None,
        )
        flags = compute_row_flags(tx, {_OWN_IBAN: _OWN_ACCOUNT_ID})
        assert (
            flags.cp_iban_status == "null"
        ), f"direction={direction!r}: expected 'null', got {flags.cp_iban_status!r}"


# ---------------------------------------------------------------------------
# §3.4  Composition (tests 77-79)
# ---------------------------------------------------------------------------


def test_77_realistic_multi_hop_expense_full_flags():
    # Multi-hop expense with transitive IBAN — all three flags in one assertion.
    tx = make_normalized_tx(
        description="На гривневий рахунок ФОП для переказу на картку",
        direction="expense",
        counterparty_iban=_OWN_IBAN,
    )
    lookup = {_OWN_IBAN: _OWN_ACCOUNT_ID}
    flags = compute_row_flags(tx, lookup)
    assert flags.description_matched is True
    assert flags.multi_hop_description is True
    assert flags.cp_iban_status == "transitive"


def test_78_realistic_card_to_card_no_iban():
    # Card expense with no IBAN — matched description, no IBAN.
    tx = make_normalized_tx(
        description="Переказ на картку",
        direction="expense",
        counterparty_iban=None,
    )
    flags = compute_row_flags(tx, {})
    assert flags.description_matched is True
    assert flags.multi_hop_description is False
    assert flags.cp_iban_status == "null"


def test_79_realistic_unlinked_expense_unknown_description():
    # Expense to a friend's bank — unknown description, cp_iban unlinked.
    tx = make_normalized_tx(
        description="Олена К.",
        direction="expense",
        counterparty_iban=EXTERNAL,
    )
    lookup = {}
    flags = compute_row_flags(tx, lookup)
    assert flags.description_matched is False
    assert flags.multi_hop_description is False
    assert flags.cp_iban_status == "unlinked"
