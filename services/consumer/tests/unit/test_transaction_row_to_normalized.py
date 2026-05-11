"""Unit tests for TransactionRow.to_normalized().

Replaces the deleted test_reconstruct.py — same coverage, targeting the
instance method on the dataclass instead of the standalone reconstruct() function.
"""

from datetime import UTC, datetime
from uuid import UUID

from grosh_consumer.repositories.transaction_repo import TransactionRow

_USER_ID = UUID("00000000-0000-0000-0000-000000000001")
_ACCOUNT_ID = UUID("00000000-0000-0000-0000-000000000002")
_TX_ID = UUID("00000000-0000-0000-0000-000000000003")
_TIME = datetime(2024, 6, 15, 10, 30, 0, tzinfo=UTC)


def _make_row(**overrides) -> TransactionRow:
    defaults = dict(
        id=_TX_ID,
        source="monobank",
        source_id="mono_abc123",
        user_id=_USER_ID,
        account_id=_ACCOUNT_ID,
        time=_TIME,
        amount_cents=50000,
        operation_amount_cents=None,
        operation_currency_code="UAH",
        description="Coffee shop",
        mcc="5814",
        cashback_amount_cents=0,
        balance_cents=1000000,
        hold=False,
        direction="expense",
        counterparty_iban=None,
        rate_source="monobank",
        metadata={"source": {"counter_name": "Starbucks", "receipt_id": "r99"}},
    )
    defaults.update(overrides)
    return TransactionRow(**defaults)


def test_happy_path_preserved_fields():
    row = _make_row()
    result = row.to_normalized()

    assert result.id == _TX_ID
    assert result.source == "monobank"
    assert result.source_id == "mono_abc123"
    assert result.user_id == _USER_ID
    assert result.account_id == _ACCOUNT_ID
    assert result.time == _TIME
    assert result.amount_cents == 50000
    assert result.operation_amount_cents is None
    assert result.operation_currency_code == "UAH"
    assert result.description == "Coffee shop"
    assert result.mcc == "5814"
    assert result.cashback_amount_cents == 0
    assert result.balance_cents == 1000000
    assert result.hold is False
    assert result.direction == "expense"
    assert result.counterparty_iban is None
    assert result.rate_source == "monobank"


def test_happy_path_metadata_source_preserved():
    row = _make_row(
        metadata={"source": {"counter_name": "Starbucks", "receipt_id": "r99"}}
    )
    result = row.to_normalized()

    assert result.metadata == {
        "source": {"counter_name": "Starbucks", "receipt_id": "r99"}
    }


def test_metadata_layer_is_dropped():
    row = _make_row(
        metadata={
            "source": {"counter_name": "ATM", "receipt_id": "r42"},
            "layer": {
                "rate": {"uah": 4123, "usd": 102, "eur": 110},
                "transfer": {"paired": True},
            },
        }
    )
    result = row.to_normalized()

    assert result.metadata == {"source": {"counter_name": "ATM", "receipt_id": "r42"}}
    assert "layer" not in result.metadata


def test_none_metadata_yields_empty_source():
    row = _make_row(metadata=None)
    result = row.to_normalized()

    assert result.metadata == {"source": {}}


def test_missing_source_key_yields_empty_source():
    row = _make_row(metadata={})
    result = row.to_normalized()

    assert result.metadata == {"source": {}}


def test_direction_preserved_as_is():
    """direction is immutable post-normalization; must survive replay unchanged."""
    for direction in ("income", "expense", "zero"):
        row = _make_row(direction=direction)
        result = row.to_normalized()
        assert result.direction == direction


def test_optional_fields_pass_through_as_none():
    row = _make_row(
        operation_amount_cents=None,
        description=None,
        mcc=None,
        balance_cents=None,
        hold=None,
        counterparty_iban=None,
        rate_source=None,
    )
    result = row.to_normalized()

    assert result.operation_amount_cents is None
    assert result.description is None
    assert result.mcc is None
    assert result.balance_cents is None
    assert result.hold is None
    assert result.counterparty_iban is None
    assert result.rate_source is None


def test_none_operation_currency_code_falls_back_to_empty_string():
    """operation_currency_code=None is stored as '' (NormalizedTransaction non-null)."""
    row = _make_row(operation_currency_code=None)
    result = row.to_normalized()

    assert result.operation_currency_code == ""
