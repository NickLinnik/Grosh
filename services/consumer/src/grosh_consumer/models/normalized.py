from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict


class NormalizedTransaction(BaseModel):
    """Internal consumer contract produced by the normalization stage.

    All fields use canonical types and conventions:
    - amount_cents is always positive; direction carries the money flow direction
      (income/expense/zero).
    - operation_currency_code is the merchant-side currency (alpha-3).
    - metadata carries bank-specific extras (comment, receipt_id, etc.).

    This model is NOT in the shared package — only the consumer uses it.
    It is JSON-serializable for the normalized_transactions Kafka topic.
    """

    model_config = ConfigDict(frozen=True)

    id: UUID
    source: str
    source_id: str
    user_id: UUID
    account_id: UUID
    time: datetime
    amount_cents: int
    operation_amount_cents: int | None
    operation_currency_code: str
    description: str | None
    mcc: str | None
    cashback_amount_cents: int
    balance_cents: int | None
    hold: bool
    direction: str
    counterparty_iban: str | None
    rate_source: str | None
    metadata: dict | None
