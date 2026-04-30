"""Shared test helpers for grosh-consumer tests."""

from datetime import UTC, datetime
from uuid import uuid4

from grosh_shared.events import RawTransactionEvent
from grosh_shared.models import TransactionSource, TransactionType

T = datetime(2025, 6, 1, 12, 0, tzinfo=UTC)


def make_event(
    source="monobank",
    operation_currency_code="PLN",
    amount_cents=10000,
    time=None,
    **kwargs,
) -> RawTransactionEvent:
    return RawTransactionEvent(
        id=kwargs.pop("id", uuid4()),
        source=TransactionSource(source) if isinstance(source, str) else source,
        source_id=kwargs.pop("source_id", f"test-{uuid4().hex[:8]}"),
        user_id=kwargs.pop("user_id", uuid4()),
        account_id=kwargs.pop("account_id", uuid4()),
        time=time or T,
        amount_cents=amount_cents,
        operation_currency_code=operation_currency_code,
        transaction_type=kwargs.pop("transaction_type", TransactionType.expense),
        **kwargs,
    )
