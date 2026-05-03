from typing import Protocol, runtime_checkable

from grosh_shared.envelope import TransactionEnvelope

from grosh_consumer.models.normalized import NormalizedTransaction


@runtime_checkable
class NormalizationStrategy(Protocol):
    def normalize(self, envelope: TransactionEnvelope) -> NormalizedTransaction: ...
