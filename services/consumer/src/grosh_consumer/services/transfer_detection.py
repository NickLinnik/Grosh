"""Transfer detection protocol and result types."""

from dataclasses import dataclass, field
from typing import Protocol
from uuid import UUID

import asyncpg

from grosh_consumer.models.normalized import NormalizedTransaction


@dataclass(frozen=True)
class AnomalyRecord:
    """A single anomaly to be persisted for a transfer detection decision."""

    transaction_id: UUID
    candidate_ids: list[UUID]
    reason_code: str
    reason_detail: str | None


@dataclass(frozen=True)
class TransferResult:
    """Result of running transfer detection on a single normalized transaction.

    special_category: set to 'transfer' when a pair is found, None otherwise.
    related_transaction_id: the paired partner's ID, or None if not paired.
    anomalies: zero or more anomaly records to be persisted.
    metadata_block: optional dict written under metadata.layer.transfer by the
        orchestrator.  None means the layer writes nothing into metadata.layer.
        Slice 17 (v2 transfer detection) populates this field.
    """

    special_category: str | None = None
    related_transaction_id: UUID | None = None
    anomalies: list[AnomalyRecord] = field(default_factory=list)
    metadata_block: dict[str, object] | None = None


class TransferDetectionStrategy(Protocol):
    """Protocol for source-specific transfer detection strategies.

    Each source (monobank, pumb, revolut, ...) implements this protocol.
    The pipeline orchestrator dispatches via a registry dict, never by
    inspecting the source name directly.
    """

    async def detect_and_pair(
        self,
        conn: asyncpg.Connection,
        tx: NormalizedTransaction,
    ) -> TransferResult: ...
