from typing import Protocol, runtime_checkable

from grosh_shared.domain.normalized import NormalizedTransaction
from grosh_shared.messaging.envelope import TransactionEnvelope


@runtime_checkable
class NormalizationStrategy(Protocol):
    def normalize(self, envelope: TransactionEnvelope) -> NormalizedTransaction: ...
