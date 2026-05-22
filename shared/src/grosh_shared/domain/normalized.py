from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict

from grosh_shared.domain.models import TransactionDirection


class NormalizedTransaction(BaseModel):
    """Internal contract produced by the normalization stage.

    Lives in the shared package because both the normalizer and pipeline services
    consume it; the API service does NOT use it (see tech spec §2.9 carve-out).

    All fields use canonical types and conventions:
    - amount_cents is always positive; direction carries the money flow direction
      (income/expense/zero).
    - operation_currency_code is the merchant-side currency (alpha-3).
    - metadata carries bank-specific extras (comment, receipt_id, etc.).

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
    hold: bool | None
    direction: TransactionDirection
    counterparty_iban: str | None
    rate_source: str | None
    metadata: dict[str, Any] | None


@dataclass(frozen=True)
class TransactionRow:
    """Typed representation of a row read back from the transactions table.

    Used by the reprocess job to reconstruct NormalizedTransaction objects.
    Fields match the column names exactly; nullable DB columns use Optional types.
    """

    id: UUID
    source: str
    source_id: str
    user_id: UUID
    account_id: UUID
    time: datetime
    amount_cents: int
    operation_amount_cents: int | None
    operation_currency_code: str | None
    description: str | None
    mcc: str | None
    cashback_amount_cents: int
    balance_cents: int | None
    hold: bool | None
    direction: TransactionDirection
    counterparty_iban: str | None
    rate_source: str | None
    metadata: dict[str, Any] | None

    def to_normalized(self) -> NormalizedTransaction:
        """Reconstruct a NormalizedTransaction from this stored row for reprocessing.

        metadata.layer is dropped entirely so each pipeline layer writes a fresh
        sub-namespace on replay; only metadata.source (bank-original fields) is
        preserved byte-identical. Per the ADR's "Preserved vs re-derived fields"
        boundary.
        """
        metadata_for_replay = {"source": (self.metadata or {}).get("source", {})}
        return NormalizedTransaction(
            id=self.id,
            source=self.source,
            source_id=self.source_id,
            user_id=self.user_id,
            account_id=self.account_id,
            time=self.time,
            amount_cents=self.amount_cents,
            operation_amount_cents=self.operation_amount_cents,
            operation_currency_code=self.operation_currency_code or "",
            description=self.description,
            mcc=self.mcc,
            cashback_amount_cents=self.cashback_amount_cents,
            balance_cents=self.balance_cents,
            hold=self.hold,
            direction=self.direction,
            counterparty_iban=self.counterparty_iban,
            rate_source=self.rate_source,
            metadata=metadata_for_replay,
        )


__all__ = ["NormalizedTransaction", "TransactionRow"]
