"""Monobank transfer description guard.

Monobank encodes the source/target account type in the transaction
description for internal transfers.  These descriptions are the only
signal available for Tier C (both IBANs NULL) matching, and serve as
a canary consistency check in Tier A/B (IBAN-based) matching.

Descriptions follow two patterns:
  Income:  "З <account phrase>"  — names the EXPENSE account (source)
  Expense: "На <account phrase>" — names the INCOME account (target)
           "Переказ на картку"   — generic, no constraint
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class DescriptionConstraint:
    """Account-level constraint implied by a Monobank transfer description.

    type:     required account type ('fop', 'black', 'white', ...) or None = any.
    currency: required account currency ('UAH', 'USD', 'EUR') or None = any.

    Both None means "no constraint" (e.g. "Переказ на картку").
    """

    type: str | None
    currency: str | None


# ---------------------------------------------------------------------------
# Known income descriptions ("З ...")
# ---------------------------------------------------------------------------

_INCOME_MAP: dict[str, DescriptionConstraint] = {
    "З гривневого рахунку ФОП": DescriptionConstraint(type="fop", currency="UAH"),
    "З доларового рахунку ФОП": DescriptionConstraint(type="fop", currency="USD"),
    "З єврового рахунку ФОП": DescriptionConstraint(type="fop", currency="EUR"),
    "З доларового рахунку ФОП для переказу на картку": DescriptionConstraint(
        type="fop", currency="USD"
    ),
    "З єврового рахунку ФОП для переказу на картку": DescriptionConstraint(
        type="fop", currency="EUR"
    ),
    "З гривневого рахунку ФОП для переказу на картку": DescriptionConstraint(
        type="fop", currency="UAH"
    ),
    "З Чорної картки": DescriptionConstraint(type="black", currency=None),
    "З Білої картки": DescriptionConstraint(type="white", currency=None),
    "З Платинової картки": DescriptionConstraint(type="platinum", currency=None),
    "З Залізної картки": DescriptionConstraint(type="iron", currency=None),
    "З Жовтої картки": DescriptionConstraint(type="yellow", currency=None),
    "З доларової картки": DescriptionConstraint(type=None, currency="USD"),
    "З єврової картки": DescriptionConstraint(type=None, currency="EUR"),
}

# ---------------------------------------------------------------------------
# Known expense descriptions ("На ..." and generic)
# ---------------------------------------------------------------------------

_EXPENSE_MAP: dict[str, DescriptionConstraint] = {
    "Переказ на картку": DescriptionConstraint(type=None, currency=None),
    "На чорну картку": DescriptionConstraint(type="black", currency=None),
    "На білу картку": DescriptionConstraint(type="white", currency=None),
    "На платинову картку": DescriptionConstraint(type="platinum", currency=None),
    "На залізну картку": DescriptionConstraint(type="iron", currency=None),
    "На жовту картку": DescriptionConstraint(type="yellow", currency=None),
    "На гривневий рахунок ФОП": DescriptionConstraint(type="fop", currency="UAH"),
    "На доларовий рахунок ФОП": DescriptionConstraint(type="fop", currency="USD"),
    "На євровий рахунок ФОП": DescriptionConstraint(type="fop", currency="EUR"),
    "На гривневий рахунок ФОП для переказу на картку": DescriptionConstraint(
        type="fop", currency="UAH"
    ),
    "На доларовий рахунок ФОП для переказу на картку": DescriptionConstraint(
        type="fop", currency="USD"
    ),
    "На євровий рахунок ФОП для переказу на картку": DescriptionConstraint(
        type="fop", currency="EUR"
    ),
}

_ALL_KNOWN: dict[str, DescriptionConstraint] = {**_INCOME_MAP, **_EXPENSE_MAP}


def parse_description(description: str | None) -> DescriptionConstraint | None:
    """Return the DescriptionConstraint for a known transfer description, or None.

    None is returned for:
    - None input
    - Empty string
    - Any description not in the known set (e.g. person names, bank fees)
    """
    if not description:
        return None
    return _ALL_KNOWN.get(description)


def is_transfer_description(description: str | None) -> bool:
    """True if the description has a transfer-like prefix or is the generic phrase.

    Does NOT require the description to be in the known set.  Used to decide
    whether to record an `unpaired_*` anomaly for unmatched transactions.
    """
    if not description:
        return False
    return (
        description.startswith("З ")
        or description.startswith("На ")
        or description == "Переказ на картку"
    )


def _constraint_matches(
    constraint: DescriptionConstraint | None,
    account_type: str,
    account_currency: str,
) -> bool:
    """True if the constraint is satisfied by the given account properties.

    None constraint (unknown description) is vacuously valid.
    """
    if constraint is None:
        return True
    if constraint.type is not None and constraint.type != account_type:
        return False
    if constraint.currency is not None and constraint.currency != account_currency:
        return False
    return True


def validate_pair_descriptions(
    income_desc: str | None,
    expense_desc: str | None,
    income_account_type: str,
    income_account_currency: str,
    expense_account_type: str,
    expense_account_currency: str,
) -> bool:
    """Validate both sides of a transfer pair.

    Income "З ..." names the SOURCE (expense) account — validate against expense leg.
    Expense "На ..." names the TARGET (income) account — validate against income leg.

    Both sides must pass.
    """
    return _constraint_matches(
        parse_description(income_desc), expense_account_type, expense_account_currency
    ) and _constraint_matches(
        parse_description(expense_desc), income_account_type, income_account_currency
    )
