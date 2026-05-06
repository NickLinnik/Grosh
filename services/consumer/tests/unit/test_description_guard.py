"""Unit tests for Monobank description guard — §2 (tests 1-48)."""

from grosh_consumer.sources.monobank.descriptions import (
    DescriptionConstraint,
    _constraint_matches,
    is_transfer_description,
    parse_description,
    validate_pair_descriptions,
)


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
