from datetime import UTC, datetime
from uuid import uuid4

import pytest

from grosh_shared.events import BackfillRequestEvent, RawTransactionEvent
from grosh_shared.models import TransactionSource, TransactionType


def _make_raw_transaction_event() -> RawTransactionEvent:
    return RawTransactionEvent(
        id=uuid4(),
        source=TransactionSource.monobank,
        source_id="mono-tx-001",
        user_id=uuid4(),
        account_id=uuid4(),
        time=datetime(2024, 3, 15, 12, 0, 0, tzinfo=UTC),
        amount_cents=15000,
        operation_amount_cents=15000,
        currency_code="UAH",
        description="ATB Market",
        mcc=5411,
        cashback_amount_cents=75,
        balance_cents=200000,
        hold=False,
        transaction_type=TransactionType.expense,
        counterparty_iban=None,
        metadata={"invoice_id": "INV-999"},
    )


def test_raw_transaction_event_serialization_round_trip() -> None:
    original = _make_raw_transaction_event()

    json_str = original.model_dump_json()
    restored = RawTransactionEvent.model_validate_json(json_str)

    assert restored == original


def test_raw_transaction_event_frozen() -> None:
    event = _make_raw_transaction_event()

    with pytest.raises(Exception):
        event.description = "tampered"  # type: ignore[misc]


def test_backfill_request_event_serialization_round_trip() -> None:
    original = BackfillRequestEvent(
        integration_id=uuid4(),
        user_id=uuid4(),
        account_external_id="acc-ext-42",
        from_timestamp=1_700_000_000,
        to_timestamp=1_700_086_400,
    )

    json_str = original.model_dump_json()
    restored = BackfillRequestEvent.model_validate_json(json_str)

    assert restored == original


def test_backfill_request_event_frozen() -> None:
    event = BackfillRequestEvent(
        integration_id=uuid4(),
        user_id=uuid4(),
        account_external_id="acc-ext-42",
        from_timestamp=1_700_000_000,
        to_timestamp=1_700_086_400,
    )

    with pytest.raises(Exception):
        event.account_external_id = "tampered"  # type: ignore[misc]
