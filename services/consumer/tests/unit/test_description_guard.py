"""Unit tests for Monobank description guard — §2 (tests 1-60).

Sections 1-48 cover parse_description, is_transfer_description, and
validate_pair_descriptions (v1-survived cases that remain valid in v2).

Sections v2_39-v2_50 cover the new is_multi_hop_description helper (§2.5).
These tests skip individually until T5 lands (is_multi_hop_description added
to descriptions.py).

Sections v2_51-v2_60 cover extended validate_pair_descriptions cases (§2.6)
exercising the full AccountProps-level cross-validation.  These run live
using the existing 6-arg signature.
"""

import pytest

from grosh_consumer.sources.monobank.descriptions import (
    DescriptionConstraint,
    _constraint_matches,
    is_transfer_description,
    parse_description,
    validate_pair_descriptions,
)

try:
    from grosh_consumer.sources.monobank.descriptions import is_multi_hop_description
except ImportError:
    is_multi_hop_description = None  # populated when T5 lands; individual tests skip


def _validates_against(desc: str, account_type: str, currency: str) -> bool:
    constraint = parse_description(desc)
    if constraint is None:
        return False
    return _constraint_matches(constraint, account_type, currency)


# ---------------------------------------------------------------------------
# §2.1  Income description parsing (tests 1-12)
# ---------------------------------------------------------------------------


def test_1_uah_fop_income():
    assert parse_description("З гривневого рахунку ФОП") == DescriptionConstraint(
        type="fop", currency="UAH"
    )


def test_2_usd_fop_income():
    assert parse_description("З доларового рахунку ФОП") == DescriptionConstraint(
        type="fop", currency="USD"
    )


def test_3_eur_fop_income():
    assert parse_description("З єврового рахунку ФОП") == DescriptionConstraint(
        type="fop", currency="EUR"
    )


def test_4_usd_fop_relay_income():
    assert parse_description(
        "З доларового рахунку ФОП для переказу на картку"
    ) == DescriptionConstraint(type="fop", currency="USD")


def test_5_eur_fop_relay_income():
    assert parse_description(
        "З єврового рахунку ФОП для переказу на картку"
    ) == DescriptionConstraint(type="fop", currency="EUR")


def test_6_black_card_income():
    assert parse_description("З Чорної картки") == DescriptionConstraint(
        type="black", currency=None
    )


def test_7_white_card_income():
    assert parse_description("З Білої картки") == DescriptionConstraint(
        type="white", currency=None
    )


def test_8_platinum_card_income():
    assert parse_description("З Платинової картки") == DescriptionConstraint(
        type="platinum", currency=None
    )


def test_9_iron_card_income():
    assert parse_description("З Залізної картки") == DescriptionConstraint(
        type="iron", currency=None
    )


def test_10_yellow_card_income():
    assert parse_description("З Жовтої картки") == DescriptionConstraint(
        type="yellow", currency=None
    )


def test_11_usd_card_income():
    assert parse_description("З доларової картки") == DescriptionConstraint(
        type=None, currency="USD"
    )


def test_12_eur_card_income():
    assert parse_description("З єврової картки") == DescriptionConstraint(
        type=None, currency="EUR"
    )


# ---------------------------------------------------------------------------
# §2.2  Expense description parsing (tests 13-24)
# ---------------------------------------------------------------------------


def test_13_generic_transfer():
    assert parse_description("Переказ на картку") == DescriptionConstraint(
        type=None, currency=None
    )


def test_14_black_card_expense():
    assert parse_description("На чорну картку") == DescriptionConstraint(
        type="black", currency=None
    )


def test_15_white_card_expense():
    assert parse_description("На білу картку") == DescriptionConstraint(
        type="white", currency=None
    )


def test_16_platinum_card_expense():
    assert parse_description("На платинову картку") == DescriptionConstraint(
        type="platinum", currency=None
    )


def test_17_iron_card_expense():
    assert parse_description("На залізну картку") == DescriptionConstraint(
        type="iron", currency=None
    )


def test_18_yellow_card_expense():
    assert parse_description("На жовту картку") == DescriptionConstraint(
        type="yellow", currency=None
    )


def test_19_uah_fop_expense():
    assert parse_description("На гривневий рахунок ФОП") == DescriptionConstraint(
        type="fop", currency="UAH"
    )


def test_20_usd_fop_expense():
    assert parse_description("На доларовий рахунок ФОП") == DescriptionConstraint(
        type="fop", currency="USD"
    )


def test_21_eur_fop_expense():
    assert parse_description("На євровий рахунок ФОП") == DescriptionConstraint(
        type="fop", currency="EUR"
    )


def test_22_uah_fop_relay_expense():
    assert parse_description(
        "На гривневий рахунок ФОП для переказу на картку"
    ) == DescriptionConstraint(type="fop", currency="UAH")


def test_23_usd_fop_relay_expense():
    assert parse_description(
        "На доларовий рахунок ФОП для переказу на картку"
    ) == DescriptionConstraint(type="fop", currency="USD")


def test_24_eur_fop_relay_expense():
    assert parse_description(
        "На євровий рахунок ФОП для переказу на картку"
    ) == DescriptionConstraint(type="fop", currency="EUR")


# ---------------------------------------------------------------------------
# §2.3  Unrecognized descriptions (tests 25-29)
# ---------------------------------------------------------------------------


def test_25_person_name_returns_none():
    assert parse_description("Олена К.") is None


def test_26_masked_card_returns_none():
    assert parse_description("516874****1234") is None


def test_27_bank_fee_returns_none():
    assert parse_description("Випуск іменної картки") is None


def test_28_empty_string_returns_none():
    assert parse_description("") is None


def test_29_none_returns_none():
    assert parse_description(None) is None


# ---------------------------------------------------------------------------
# §2.4  Transfer-like prefix detection (tests 30-31)
# ---------------------------------------------------------------------------


def test_30_unknown_z_prefix_has_transfer_prefix():
    desc = "З невідомого рахунку"
    # parse_description returns None (not in known set)
    assert parse_description(desc) is None
    # but is_transfer_description sees the "З " prefix
    assert is_transfer_description(desc) is True


def test_31_unknown_na_prefix_has_transfer_prefix():
    desc = "На невідому картку"
    assert parse_description(desc) is None
    assert is_transfer_description(desc) is True


# ---------------------------------------------------------------------------
# §2.5  Account validation (tests 32-44)
# ---------------------------------------------------------------------------


def test_32_black_income_validates_against_black():
    assert _validates_against("З Чорної картки", "black", "UAH") is True


def test_33_black_income_fails_against_white():
    assert _validates_against("З Чорної картки", "white", "UAH") is False


def test_34_uah_fop_income_validates_against_fop_uah():
    assert _validates_against("З гривневого рахунку ФОП", "fop", "UAH") is True


def test_35_uah_fop_income_fails_against_fop_usd():
    assert _validates_against("З гривневого рахунку ФОП", "fop", "USD") is False


def test_36_uah_fop_income_fails_against_black_uah():
    assert _validates_against("З гривневого рахунку ФОП", "black", "UAH") is False


def test_37_usd_card_income_validates_against_black_usd():
    assert _validates_against("З доларової картки", "black", "USD") is True


def test_38_usd_card_income_validates_against_white_usd():
    assert _validates_against("З доларової картки", "white", "USD") is True


def test_39_usd_card_income_fails_against_black_eur():
    assert _validates_against("З доларової картки", "black", "EUR") is False


def test_40_generic_expense_validates_against_any_account():
    assert _validates_against("Переказ на картку", "fop", "UAH") is True
    assert _validates_against("Переказ на картку", "black", "EUR") is True


def test_41_black_expense_validates_against_black():
    assert _validates_against("На чорну картку", "black", "UAH") is True


def test_42_black_expense_fails_against_fop():
    assert _validates_against("На чорну картку", "fop", "UAH") is False


def test_43_uah_fop_expense_validates_against_fop_uah():
    assert _validates_against("На гривневий рахунок ФОП", "fop", "UAH") is True


def test_44_uah_fop_expense_fails_against_fop_eur():
    assert _validates_against("На гривневий рахунок ФОП", "fop", "EUR") is False


# ---------------------------------------------------------------------------
# §2.6  Cross-pair validation (tests 45-48)
# ---------------------------------------------------------------------------


def test_45_card_to_card_both_pass():
    # Income "З Чорної картки" on black card, expense "Переказ на картку" on black card.
    # Income constraint (type=black) validates against expense account (black).
    # Expense constraint (None) validates against income account (any).
    result = validate_pair_descriptions(
        income_desc="З Чорної картки",
        expense_desc="Переказ на картку",
        income_account_type="black",
        income_account_currency="UAH",
        expense_account_type="black",
        expense_account_currency="UAH",
    )
    assert result is True


def test_46_fop_to_card_both_pass():
    # FOP UAH expense → black card income.
    # Income "З гривневого рахунку ФОП" → (fop, UAH)
    # validates against expense account (FOP UAH).
    # Expense "На чорну картку" → (black) → validates against income (black).
    result = validate_pair_descriptions(
        income_desc="З гривневого рахунку ФОП",
        expense_desc="На чорну картку",
        income_account_type="black",
        income_account_currency="UAH",
        expense_account_type="fop",
        expense_account_currency="UAH",
    )
    assert result is True


def test_47_income_desc_wrong_type_fails():
    # Income "З Білої картки" but expense is on FOP (not white).
    result = validate_pair_descriptions(
        income_desc="З Білої картки",
        expense_desc="На чорну картку",
        income_account_type="black",
        income_account_currency="UAH",
        expense_account_type="fop",
        expense_account_currency="UAH",
    )
    assert result is False


def test_48_generic_expense_with_constrained_income_passes():
    # Expense "Переказ на картку" (no constraint) + income "З Чорної картки".
    # Income constraint (type=black) validated against expense account → True.
    result = validate_pair_descriptions(
        income_desc="З Чорної картки",
        expense_desc="Переказ на картку",
        income_account_type="black",
        income_account_currency="UAH",
        expense_account_type="black",
        expense_account_currency="UAH",
    )
    assert result is True


# ---------------------------------------------------------------------------
# §2.5  is_multi_hop_description — "для переказу на" family detection (v2_39-v2_50)
#
# Each test skips individually until T5 lands (is_multi_hop_description added
# to descriptions.py).  The file always collects without error because the
# import is guarded by try/except above.
# ---------------------------------------------------------------------------


def test_v2_39_usd_fop_relay_income_is_multi_hop():
    if is_multi_hop_description is None:
        pytest.skip("is_multi_hop_description not yet implemented (T5)")
    assert (
        is_multi_hop_description("З доларового рахунку ФОП для переказу на картку")
        is True
    )


def test_v2_40_eur_fop_relay_income_is_multi_hop():
    if is_multi_hop_description is None:
        pytest.skip("is_multi_hop_description not yet implemented (T5)")
    assert (
        is_multi_hop_description("З єврового рахунку ФОП для переказу на картку")
        is True
    )


def test_v2_41_uah_fop_relay_income_is_multi_hop():
    if is_multi_hop_description is None:
        pytest.skip("is_multi_hop_description not yet implemented (T5)")
    assert (
        is_multi_hop_description("З гривневого рахунку ФОП для переказу на картку")
        is True
    )


def test_v2_42_uah_fop_relay_expense_is_multi_hop():
    if is_multi_hop_description is None:
        pytest.skip("is_multi_hop_description not yet implemented (T5)")
    assert (
        is_multi_hop_description("На гривневий рахунок ФОП для переказу на картку")
        is True
    )


def test_v2_43_usd_fop_relay_expense_is_multi_hop():
    if is_multi_hop_description is None:
        pytest.skip("is_multi_hop_description not yet implemented (T5)")
    assert (
        is_multi_hop_description("На доларовий рахунок ФОП для переказу на картку")
        is True
    )


def test_v2_44_eur_fop_relay_expense_is_multi_hop():
    if is_multi_hop_description is None:
        pytest.skip("is_multi_hop_description not yet implemented (T5)")
    assert (
        is_multi_hop_description("На євровий рахунок ФОП для переказу на картку")
        is True
    )


def test_v2_45_uah_fop_base_income_not_multi_hop():
    if is_multi_hop_description is None:
        pytest.skip("is_multi_hop_description not yet implemented (T5)")
    # FOP transfer without the multi-hop suffix — not multi-hop.
    assert is_multi_hop_description("З гривневого рахунку ФОП") is False


def test_v2_46_black_card_income_not_multi_hop():
    if is_multi_hop_description is None:
        pytest.skip("is_multi_hop_description not yet implemented (T5)")
    assert is_multi_hop_description("З Чорної картки") is False


def test_v2_47_generic_expense_not_multi_hop():
    if is_multi_hop_description is None:
        pytest.skip("is_multi_hop_description not yet implemented (T5)")
    assert is_multi_hop_description("Переказ на картку") is False


def test_v2_48_person_name_not_multi_hop():
    if is_multi_hop_description is None:
        pytest.skip("is_multi_hop_description not yet implemented (T5)")
    assert is_multi_hop_description("Олена К.") is False


def test_v2_49_empty_string_not_multi_hop():
    if is_multi_hop_description is None:
        pytest.skip("is_multi_hop_description not yet implemented (T5)")
    assert is_multi_hop_description("") is False


def test_v2_50_none_not_multi_hop():
    if is_multi_hop_description is None:
        pytest.skip("is_multi_hop_description not yet implemented (T5)")
    assert is_multi_hop_description(None) is False


# ---------------------------------------------------------------------------
# §2.6  validate_pair_descriptions — extended v2 cross-validation (v2_51-v2_60)
#
# These cases run LIVE (no skip guard).  They use the existing 6-arg signature:
#   validate_pair_descriptions(
#       income_desc, expense_desc,
#       income_account_type, income_account_currency,
#       expense_account_type, expense_account_currency,
#   )
# The income "З ..." names the SOURCE (expense leg's account), so the income
# constraint is validated against the expense account's props.
# The expense "На ..." names the TARGET (income leg's account), so the expense
# constraint is validated against the income account's props.
# ---------------------------------------------------------------------------


def test_v2_51_card_to_card_consistent_pair():
    # income="З Чорної картки" (constraint type=black) checks expense account.
    # expense="Переказ на картку" (no constraint) → passes.
    assert (
        validate_pair_descriptions(
            income_desc="З Чорної картки",
            expense_desc="Переказ на картку",
            income_account_type="white",  # income leg is white card
            income_account_currency="UAH",
            expense_account_type="black",  # expense leg — income desc checks this
            expense_account_currency="UAH",
        )
        is True
    )


def test_v2_52_fop_to_card_consistent_pair():
    # income="З гривневого рахунку ФОП" (fop, UAH) vs expense account UAH_FOP → True.
    # expense="На чорну картку" (black) vs income account UAH_BLACK → True.
    assert (
        validate_pair_descriptions(
            income_desc="З гривневого рахунку ФОП",
            expense_desc="На чорну картку",
            income_account_type="black",  # income leg is black card
            income_account_currency="UAH",
            expense_account_type="fop",  # expense leg is UAH FOP
            expense_account_currency="UAH",
        )
        is True
    )


def test_v2_53_mismatch_wrong_card_color():
    # income="З Білої картки" (white) but expense leg is UAH_BLACK (black) → False.
    assert (
        validate_pair_descriptions(
            income_desc="З Білої картки",
            expense_desc="Переказ на картку",
            income_account_type="white",
            income_account_currency="UAH",
            expense_account_type="black",  # should be white — mismatch
            expense_account_currency="UAH",
        )
        is False
    )


def test_v2_54_mismatch_wrong_currency():
    # income="З гривневого рахунку ФОП" (UAH) but expense leg is USD_FOP → False.
    assert (
        validate_pair_descriptions(
            income_desc="З гривневого рахунку ФОП",
            expense_desc="Переказ на картку",
            income_account_type="black",
            income_account_currency="UAH",
            expense_account_type="fop",  # type OK but currency is USD — mismatch
            expense_account_currency="USD",
        )
        is False
    )


def test_v2_55_mismatch_wrong_type():
    # income="З Чорної картки" (black) but expense leg is UAH_FOP (fop) → False.
    assert (
        validate_pair_descriptions(
            income_desc="З Чорної картки",
            expense_desc="Переказ на картку",
            income_account_type="black",
            income_account_currency="UAH",
            expense_account_type="fop",  # should be black — mismatch
            expense_account_currency="UAH",
        )
        is False
    )


def test_v2_56_generic_expense_one_side_validates():
    # expense="Переказ на картку" (no constraint from expense side).
    # income="З Чорної картки" (type=black) vs expense account UAH_BLACK → True.
    # Generic side imposes no constraint — specific side passes.
    assert (
        validate_pair_descriptions(
            income_desc="З Чорної картки",
            expense_desc="Переказ на картку",
            income_account_type="black",
            income_account_currency="UAH",
            expense_account_type="black",
            expense_account_currency="UAH",
        )
        is True
    )


def test_v2_57_both_unknown_vacuously_valid():
    # Both descriptions outside the known set → both parse_description calls return None
    # → no constraints → True.  Validation can't punish an unknown description.
    assert (
        validate_pair_descriptions(
            income_desc="Олена К.",
            expense_desc="516874****1234",
            income_account_type="black",
            income_account_currency="UAH",
            expense_account_type="fop",
            expense_account_currency="UAH",
        )
        is True
    )


def test_v2_58_multi_hop_variant_validates_same_as_base():
    # Multi-hop suffix doesn't change the constraint.
    # income="З доларового рахунку ФОП для переказу на картку" → (fop, USD).
    # expense leg on USD_FOP (fop, USD) → True.
    assert (
        validate_pair_descriptions(
            income_desc="З доларового рахунку ФОП для переказу на картку",
            expense_desc="Переказ на картку",
            income_account_type="fop",
            income_account_currency="UAH",
            expense_account_type="fop",  # USD FOP — income constraint checks currency
            expense_account_currency="USD",
        )
        is True
    )


def test_v2_59_both_wrong_returns_false():
    # Both constraints fail: expense side wrong for income constraint,
    # income side wrong for expense constraint.
    assert (
        validate_pair_descriptions(
            income_desc="З Білої картки",  # expects expense to be white
            expense_desc="На гривневий рахунок ФОП",  # expects income to be fop/UAH
            income_account_type="black",  # not fop/UAH — expense constraint fails
            income_account_currency="UAH",
            expense_account_type="black",  # not white — income constraint fails
            expense_account_currency="UAH",
        )
        is False
    )


def test_v2_60_none_income_desc_no_income_constraint():
    # income_desc=None → parse_description returns None → no income-side constraint.
    # expense_desc="На чорну картку" (black) vs income account UAH_BLACK → True.
    assert (
        validate_pair_descriptions(
            income_desc=None,
            expense_desc="На чорну картку",
            income_account_type="black",
            income_account_currency="UAH",
            expense_account_type="fop",
            expense_account_currency="UAH",
        )
        is True
    )
