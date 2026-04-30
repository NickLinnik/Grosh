from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict

from grosh_shared.models import TransactionSource, TransactionType


class RawTransactionEvent(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: UUID
    source: TransactionSource
    source_id: str
    user_id: UUID
    account_id: UUID
    time: datetime
    amount_cents: int  # Always positive. Direction indicated by transaction_type.
    operation_amount_cents: int | None = (
        None  # Always positive. Merchant-currency amount.
    )
    # Merchant/operation currency (equals card currency when no FX conversion).
    operation_currency_code: str
    description: str | None = None
    mcc: int | None = None
    cashback_amount_cents: int = 0
    balance_cents: int | None = None
    hold: bool = False
    transaction_type: TransactionType
    counterparty_iban: str | None = None
    metadata: dict | None = (
        None  # Bank-specific extras (e.g. counter_edrpou, invoice_id)
    )
    rate_source: str | None = (
        None  # Rate source chain entry point for currency conversion
    )
