from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict


class ManualTransactionPayload(BaseModel):
    """Canonical payload for manual transactions published to raw_transactions.manual.

    The ingestion service generates the deterministic id and source_id and
    includes them here so the ManualNormalizer can use them without recomputing.
    """

    model_config = ConfigDict(frozen=True)

    id: UUID
    source_id: str
    amount_cents: int
    operation_currency_code: str
    description: str | None
    time: datetime
    transaction_type: str
    mcc: int | None
    rate_source: str | None
