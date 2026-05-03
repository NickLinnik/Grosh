"""Unit tests for the Monobank adapter.

Covers to_raw_transaction_event(): type classification, amount sign stripping,
currency conversion, deterministic ID generation, and optional metadata handling.
"""

from uuid import UUID, uuid4

import pytest
from grosh_shared.id_utils import generate_transaction_id
from grosh_shared.models import TransactionType

from grosh_ingestion.sources.monobank.models import MonobankStatementItem
from grosh_ingestion.sources.monobank.transaction_adapter import (
    to_raw_transaction_event,
)

_USER_ID: UUID = uuid4()
_ACCOUNT_ID: UUID = uuid4()


def _make_item(**overrides: object) -> MonobankStatementItem:
    """Build a MonobankStatementItem with sensible defaults."""
    defaults: dict[str, object] = {
        "id": "tx-001",
        "time": 1700000000,
        "description": "ATB Market",
        "mcc": 5411,
        "originalMcc": 5411,
        "hold": False,
        "amount": -5000,
        "operationAmount": -5000,
        "currencyCode": 980,
        "cashbackAmount": 25,
        "balance": 95000,
    }
    defaults.update(overrides)
    return MonobankStatementItem.model_validate(defaults)


# ---------------------------------------------------------------------------
# Transaction type classification
# ---------------------------------------------------------------------------


def test_negative_amount_is_expense() -> None:
    item = _make_item(amount=-5000, operationAmount=-5000)
    event = to_raw_transaction_event(item, _USER_ID, _ACCOUNT_ID)

    assert event.transaction_type is TransactionType.expense
    assert event.amount_cents == 5000


def test_positive_amount_is_income() -> None:
    item = _make_item(amount=5000, operationAmount=5000)
    event = to_raw_transaction_event(item, _USER_ID, _ACCOUNT_ID)

    assert event.transaction_type is TransactionType.income
    assert event.amount_cents == 5000


def test_zero_amount_is_check() -> None:
    item = _make_item(amount=0, operationAmount=0)
    event = to_raw_transaction_event(item, _USER_ID, _ACCOUNT_ID)

    assert event.transaction_type is TransactionType.check
    assert event.amount_cents == 0


# ---------------------------------------------------------------------------
# Currency conversion
# ---------------------------------------------------------------------------


def test_currency_code_converted() -> None:
    item = _make_item(currencyCode=980)
    event = to_raw_transaction_event(item, _USER_ID, _ACCOUNT_ID)

    assert event.operation_currency_code == "UAH"


def test_unknown_currency_raises() -> None:
    item = _make_item(currencyCode=1)

    with pytest.raises(ValueError, match="Unknown ISO 4217 numeric code: 1"):
        to_raw_transaction_event(item, _USER_ID, _ACCOUNT_ID)


# ---------------------------------------------------------------------------
# Deterministic ID
# ---------------------------------------------------------------------------


def test_deterministic_id() -> None:
    item = _make_item(id="tx-001")
    event_a = to_raw_transaction_event(item, _USER_ID, _ACCOUNT_ID)
    event_b = to_raw_transaction_event(item, _USER_ID, _ACCOUNT_ID)

    assert event_a.id == event_b.id
    assert event_a.id == generate_transaction_id("monobank", "tx-001")


def test_hold_and_settled_same_id() -> None:
    """Adapter uses same base ID regardless of hold flag. Consumer handles linking."""
    hold = _make_item(id="tx-001", hold=True)
    settled = _make_item(id="tx-001", hold=False)

    assert to_raw_transaction_event(hold, _USER_ID, _ACCOUNT_ID).id == (
        to_raw_transaction_event(settled, _USER_ID, _ACCOUNT_ID).id
    )


def test_hold_flag_passed_through() -> None:
    hold = _make_item(hold=True)
    settled = _make_item(hold=False)

    assert to_raw_transaction_event(hold, _USER_ID, _ACCOUNT_ID).hold is True
    assert to_raw_transaction_event(settled, _USER_ID, _ACCOUNT_ID).hold is False


# ---------------------------------------------------------------------------
# Metadata handling
# ---------------------------------------------------------------------------


def test_metadata_populated() -> None:
    item = _make_item(comment="test", counterName="Corner Shop")
    event = to_raw_transaction_event(item, _USER_ID, _ACCOUNT_ID)

    assert event.metadata is not None
    assert event.metadata["comment"] == "test"
    assert event.metadata["counter_name"] == "Corner Shop"


def test_metadata_none_when_empty() -> None:
    # No optional fields set — all default to None.
    item = _make_item()
    event = to_raw_transaction_event(item, _USER_ID, _ACCOUNT_ID)

    assert event.metadata is None


# ---------------------------------------------------------------------------
# Optional passthrough fields
# ---------------------------------------------------------------------------


def test_counterparty_iban_passed_through() -> None:
    item = _make_item(counterIban="UA123456789012345678901234567")
    event = to_raw_transaction_event(item, _USER_ID, _ACCOUNT_ID)

    assert event.counterparty_iban == "UA123456789012345678901234567"
