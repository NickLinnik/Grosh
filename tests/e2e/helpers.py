"""Shared test helpers for the cross-service e2e suite."""

from datetime import UTC, datetime
from uuid import uuid4

from grosh_shared.normalized import NormalizedTransaction

T = datetime(2025, 6, 1, 12, 0, tzinfo=UTC)


def make_event(
    source="monobank",
    operation_currency_code="PLN",
    amount_cents=10000,
    time=None,
    **kwargs,
) -> NormalizedTransaction:
    return NormalizedTransaction(
        id=kwargs.pop("id", uuid4()),
        source=source,
        source_id=kwargs.pop("source_id", f"test-{uuid4().hex[:8]}"),
        user_id=kwargs.pop("user_id", uuid4()),
        account_id=kwargs.pop("account_id", uuid4()),
        time=time or T,
        amount_cents=amount_cents,
        operation_amount_cents=kwargs.pop("operation_amount_cents", amount_cents),
        operation_currency_code=operation_currency_code,
        description=kwargs.pop("description", None),
        mcc=kwargs.pop("mcc", None),
        cashback_amount_cents=kwargs.pop("cashback_amount_cents", 0),
        balance_cents=kwargs.pop("balance_cents", None),
        hold=kwargs.pop("hold", False),
        direction=kwargs.pop("direction", "expense"),
        counterparty_iban=kwargs.pop("counterparty_iban", None),
        rate_source=kwargs.pop("rate_source", None),
        metadata=kwargs.pop("metadata", None),
    )
