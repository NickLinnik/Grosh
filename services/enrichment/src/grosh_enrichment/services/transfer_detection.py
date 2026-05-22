"""Transfer detection protocol and result types."""

from dataclasses import dataclass, field
from typing import Protocol
from uuid import UUID

import asyncpg
from grosh_shared.domain.normalized import NormalizedTransaction


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
    metadata_block: opaque payload written under metadata.layer.transfer by
        the orchestrator's structured per-layer merge. Contract:
          {"row": {...}}                — unpaired MCC 4829 row
          {"row": {...}, "pair": {...}} — successful claim (both legs carry
                                          identical "pair" sub-block)
          None                          — non-MCC-4829 row, or a source
                                          whose strategy doesn't write a
                                          block. The orchestrator writes
                                          nothing under metadata.layer.transfer
                                          when None, preserving the invariant
                                          `metadata.layer.transfer exists ⇔
                                          mcc == '4829'` (Monobank only).
        The orchestrator never inspects this dict — it's owned by the strategy.
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
