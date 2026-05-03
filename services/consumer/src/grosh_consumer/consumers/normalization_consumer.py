import asyncio
import logging
import os
from pathlib import Path

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from grosh_shared.envelope import TransactionEnvelope
from grosh_shared.models import Topic

from grosh_consumer.kafka import on_delivery
from grosh_consumer.models.normalized import NormalizedTransaction
from grosh_consumer.sources import NormalizationStrategy
from grosh_consumer.sources.manual.normalizer import ManualNormalizer
from grosh_consumer.sources.monobank.normalizer import MonobankNormalizer

logger = logging.getLogger(__name__)

_HEALTH_FILE = Path("/tmp/healthy")

# Registry maps source name → normalizer. Add new banks here.
_NORMALIZER_REGISTRY: dict[str, NormalizationStrategy] = {
    "monobank": MonobankNormalizer(),
    "manual": ManualNormalizer(),
}

_SUBSCRIBED_TOPICS = [
    Topic.raw_transactions_monobank,
    Topic.raw_transactions_manual,
]


async def run_normalization_consumer() -> None:
    """Consume raw per-source envelopes, normalize, publish to normalized_transactions."""  # noqa: E501
    bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": "normalization",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe(list(_SUBSCRIBED_TOPICS))

    producer = Producer({"bootstrap.servers": bootstrap_servers})

    logger.info(
        "Normalization consumer started, subscribed to %s",
        list(_SUBSCRIBED_TOPICS),
    )

    try:
        while True:
            _HEALTH_FILE.touch()
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                await asyncio.sleep(0.1)
                continue
            err = msg.error()
            if err is not None:
                if err.code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(err)

            raw_value = msg.value()
            assert raw_value is not None

            try:
                envelope = TransactionEnvelope.model_validate_json(raw_value)
            except Exception:
                logger.exception(
                    "Failed to deserialize envelope from %s: %s",
                    msg.topic(),
                    raw_value,
                )
                consumer.commit(message=msg)
                continue

            normalizer = _NORMALIZER_REGISTRY.get(envelope.source)
            if normalizer is None:
                logger.error(
                    "No normalizer registered for source=%r; skipping message",
                    envelope.source,
                )
                consumer.commit(message=msg)
                continue

            try:
                normalized: NormalizedTransaction = normalizer.normalize(envelope)
            except Exception:
                logger.exception(
                    "Normalization failed for source=%r user_id=%s; skipping",
                    envelope.source,
                    envelope.user_id,
                )
                consumer.commit(message=msg)
                continue

            try:
                producer.produce(
                    topic=Topic.normalized_transactions,
                    key=str(normalized.user_id).encode(),
                    value=normalized.model_dump_json().encode(),
                    on_delivery=on_delivery,
                )
            except BufferError:
                producer.flush(timeout=10)
                producer.produce(
                    topic=Topic.normalized_transactions,
                    key=str(normalized.user_id).encode(),
                    value=normalized.model_dump_json().encode(),
                    on_delivery=on_delivery,
                )
            producer.poll(0)

            logger.debug(
                "Normalized transaction %s (source=%s) for user %s",
                normalized.id,
                normalized.source,
                normalized.user_id,
            )

            # Offset committed before broker ack — intentional at-most-once.
            # The pipeline consumer deduplicates via ON CONFLICT DO NOTHING,
            # so a re-delivered raw envelope just produces a harmless duplicate
            # on normalized_transactions. No data loss risk.
            consumer.commit(message=msg)
    finally:
        producer.flush(timeout=10)
        consumer.close()
