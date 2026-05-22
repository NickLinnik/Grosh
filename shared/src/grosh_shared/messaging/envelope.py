from uuid import UUID

from pydantic import BaseModel, ConfigDict


class TransactionEnvelope(BaseModel):
    """Wire format published to per-source raw_transactions.{source} topics.

    The ingestion service wraps raw bank payloads in this envelope before
    publishing. The normalization consumer reads it and dispatches to the
    per-source NormalizationStrategy.

    payload carries the raw bank-specific dict — untyped at this layer.
    Each source's normalizer knows how to validate and parse it.
    """

    model_config = ConfigDict(frozen=True)

    user_id: UUID
    account_id: UUID
    source: str
    payload: dict
