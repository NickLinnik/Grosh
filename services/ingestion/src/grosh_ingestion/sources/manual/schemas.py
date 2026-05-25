from datetime import datetime
from uuid import UUID

from grosh_shared.domain.models import (
    TransactionDirection,
    TransactionOrigin,
    TransactionSource,
)
from pydantic import BaseModel, ConfigDict


class ManualAccountResponse(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: UUID
    type: str
    currency_code: str
    name: str
    created_at: datetime


class ManualTransactionResponse(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: UUID
    source: TransactionSource
    source_id: str
    account_id: UUID
    time: datetime
    amount_cents: int
    operation_currency_code: str
    direction: TransactionDirection
    description: str | None
    origin: TransactionOrigin
    rate_source: str | None
