"""Producer-side mechanic for the reprocessing job.

Owns only the Kafka publishing concern: holds the producer for its lifetime
and exposes a single method to publish reconstructed NormalizedTransaction
events to the normalized_transactions topic.

No DB access — all DB operations live in ReprocessOrchestrator.
"""

import logging
from uuid import UUID

from confluent_kafka import KafkaException, Producer
from grosh_shared.models import Topic
from grosh_shared.normalized import NormalizedTransaction

from grosh_normalizer.kafka import on_delivery

logger = logging.getLogger(__name__)


class ReplayService:
    """Publishes reconstructed NormalizedTransaction events to normalized_transactions.

    Bypasses staging — the reprocess job is the lock holder; staging is for
    events arriving via the normalization consumer while the lock is held.
    """

    def __init__(self, producer: Producer) -> None:
        self._producer = producer

    def publish_normalized_events(
        self, user_id: UUID, events: list[NormalizedTransaction]
    ) -> int:
        """Publish all events for a user to normalized_transactions.

        Returns the count of published messages. Flushes the producer before
        returning so callers can rely on all events being in-flight.
        Raises on Kafka errors after one retry attempt.
        """
        for event in events:
            payload = event.model_dump_json().encode()
            key = str(event.user_id).encode()
            try:
                self._producer.produce(
                    topic=Topic.normalized_transactions,
                    key=key,
                    value=payload,
                    on_delivery=on_delivery,
                )
            except KafkaException as exc:
                self._producer.flush(timeout=10)
                self._producer.produce(
                    topic=Topic.normalized_transactions,
                    key=key,
                    value=payload,
                    on_delivery=on_delivery,
                )
                logger.warning(
                    "Kafka BufferError for event %s, retried: %s", event.id, exc
                )
            self._producer.poll(0)

        self._producer.flush(timeout=30)
        return len(events)
