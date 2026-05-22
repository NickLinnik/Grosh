"""Unit tests for ManualNormalizer."""

from datetime import UTC, datetime
from uuid import uuid4

from grosh_shared.messaging.envelope import TransactionEnvelope

from grosh_normalization.sources.manual.normalizer import ManualNormalizer

_USER_ID = uuid4()
_ACCOUNT_ID = uuid4()
_TX_ID = uuid4()

_normalizer = ManualNormalizer()


def _make_envelope(**overrides: object) -> TransactionEnvelope:
    payload: dict = {
        "id": str(_TX_ID),
        "source_id": "manual-001",
        "amount_cents": 5000,
        "operation_currency_code": "UAH",
        "description": "Coffee",
        "time": "2025-06-01T12:00:00+00:00",
        "direction": "expense",
        "mcc": "5814",
        "rate_source": None,
    }
    payload.update(overrides)
    return TransactionEnvelope(
        user_id=_USER_ID,
        account_id=_ACCOUNT_ID,
        source="manual",
        payload=payload,
    )


def test_basic_normalization() -> None:
    tx = _normalizer.normalize(_make_envelope())

    assert tx.id == _TX_ID
    assert tx.source == "manual"
    assert tx.source_id == "manual-001"
    assert tx.user_id == _USER_ID
    assert tx.account_id == _ACCOUNT_ID
    assert tx.amount_cents == 5000
    assert tx.operation_amount_cents == 5000
    assert tx.operation_currency_code == "UAH"
    assert tx.description == "Coffee"
    assert tx.direction == "expense"
    assert tx.mcc == "5814"
    assert tx.hold is False
    assert tx.cashback_amount_cents == 0
    assert tx.balance_cents is None
    assert tx.counterparty_iban is None
    assert tx.metadata is None


def test_direction_passthrough() -> None:
    tx = _normalizer.normalize(_make_envelope(direction="income"))
    assert tx.direction == "income"


def test_rate_source_passthrough() -> None:
    tx = _normalizer.normalize(_make_envelope(rate_source="nbu"))
    assert tx.rate_source == "nbu"


def test_time_parsed() -> None:
    tx = _normalizer.normalize(_make_envelope(time="2025-03-15T10:30:00+00:00"))
    assert tx.time == datetime(2025, 3, 15, 10, 30, tzinfo=UTC)


def test_description_none() -> None:
    tx = _normalizer.normalize(_make_envelope(description=None))
    assert tx.description is None


def test_mcc_none() -> None:
    tx = _normalizer.normalize(_make_envelope(mcc=None))
    assert tx.mcc is None
