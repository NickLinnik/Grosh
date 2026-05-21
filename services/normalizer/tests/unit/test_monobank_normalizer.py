"""Unit tests for MonobankNormalizer.

Covers: type classification, amount sign stripping, currency conversion,
deterministic ID generation, and optional metadata handling.
"""

from uuid import UUID, uuid4

import pytest
from grosh_shared.envelope import TransactionEnvelope
from grosh_shared.id_utils import generate_transaction_id
from grosh_shared.models import TransactionDirection

from grosh_normalizer.sources.monobank.normalizer import MonobankNormalizer

_USER_ID: UUID = uuid4()
_ACCOUNT_ID: UUID = uuid4()

_normalizer = MonobankNormalizer()


def _make_envelope(**overrides: object) -> TransactionEnvelope:
    """Build a TransactionEnvelope with a Monobank payload using sensible defaults."""
    payload: dict = {
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
    payload.update(overrides)
    return TransactionEnvelope(
        user_id=_USER_ID,
        account_id=_ACCOUNT_ID,
        source="monobank",
        payload=payload,
    )


# ---------------------------------------------------------------------------
# Transaction type classification
# ---------------------------------------------------------------------------


def test_negative_amount_is_expense() -> None:
    tx = _normalizer.normalize(_make_envelope(amount=-5000, operationAmount=-5000))

    assert tx.direction == TransactionDirection.expense
    assert tx.amount_cents == 5000


def test_positive_amount_is_income() -> None:
    tx = _normalizer.normalize(_make_envelope(amount=5000, operationAmount=5000))

    assert tx.direction == TransactionDirection.income
    assert tx.amount_cents == 5000


def test_zero_amount_is_zero() -> None:
    tx = _normalizer.normalize(_make_envelope(amount=0, operationAmount=0))

    assert tx.direction == TransactionDirection.zero
    assert tx.amount_cents == 0


# ---------------------------------------------------------------------------
# Currency conversion
# ---------------------------------------------------------------------------


def test_currency_code_converted() -> None:
    tx = _normalizer.normalize(_make_envelope(currencyCode=980))

    assert tx.operation_currency_code == "UAH"


def test_unknown_currency_raises() -> None:
    with pytest.raises(ValueError, match="Unknown ISO 4217 numeric code: 1"):
        _normalizer.normalize(_make_envelope(currencyCode=1))


# ---------------------------------------------------------------------------
# Deterministic ID
# ---------------------------------------------------------------------------


def test_deterministic_id() -> None:
    tx_a = _normalizer.normalize(_make_envelope(id="tx-001"))
    tx_b = _normalizer.normalize(_make_envelope(id="tx-001"))

    assert tx_a.id == tx_b.id
    assert tx_a.id == generate_transaction_id("monobank", "tx-001")


def test_hold_and_settled_same_id() -> None:
    """Same base ID regardless of hold flag — consumer handles linking."""
    tx_hold = _normalizer.normalize(_make_envelope(id="tx-001", hold=True))
    tx_settled = _normalizer.normalize(_make_envelope(id="tx-001", hold=False))

    assert tx_hold.id == tx_settled.id


def test_hold_flag_passed_through() -> None:
    tx_hold = _normalizer.normalize(_make_envelope(hold=True))
    tx_settled = _normalizer.normalize(_make_envelope(hold=False))

    assert tx_hold.hold is True
    assert tx_settled.hold is False


# ---------------------------------------------------------------------------
# Metadata handling
# ---------------------------------------------------------------------------


def test_metadata_populated() -> None:
    tx = _normalizer.normalize(
        _make_envelope(comment="test", counterName="Corner Shop")
    )

    assert tx.metadata is not None
    assert tx.metadata["source"]["comment"] == "test"
    assert tx.metadata["source"]["counter_name"] == "Corner Shop"


def test_metadata_none_when_empty() -> None:
    # No optional fields set — all default to None.
    tx = _normalizer.normalize(_make_envelope())

    assert tx.metadata is None


# ---------------------------------------------------------------------------
# Optional passthrough fields
# ---------------------------------------------------------------------------


def test_counterparty_iban_passed_through() -> None:
    tx = _normalizer.normalize(
        _make_envelope(counterIban="UA123456789012345678901234567")
    )

    assert tx.counterparty_iban == "UA123456789012345678901234567"
