import asyncio
import logging

from confluent_kafka import Consumer, KafkaError, KafkaException, Message
from grosh_shared.messaging.kafka_resilience import TransientErrorBudget

logger = logging.getLogger(__name__)

# How long UNKNOWN_TOPIC_OR_PART is tolerated before being escalated to a
# fatal KafkaException. Covers normal broker reshuffles, controlled topic
# recreations, and the e2e test harness's between-session topic resets.
# Beyond this, a missing topic almost certainly indicates a misconfigured
# deployment (wrong topic name, wrong cluster) and should crash visibly so
# k8s/CI surfaces it instead of silently consuming nothing forever.
UNKNOWN_TOPIC_TOLERANCE_SECONDS = 30.0


def on_delivery(err: KafkaError | None, msg: Message) -> None:
    if err is not None:
        logger.error("Kafka delivery failed for %s: %s", msg.topic(), err)


_unknown_topic_budget = TransientErrorBudget(UNKNOWN_TOPIC_TOLERANCE_SECONDS)


async def poll_message(consumer: Consumer) -> tuple[bytes, Message] | None:
    """Poll for the next message, handling EOF and bounded transient errors.

    Returns (raw_value, msg) on success, None if no message available.
    Raises KafkaException on fatal errors.

    Transient errors handled here:
    - _PARTITION_EOF: end of partition, expected.
    - UNKNOWN_TOPIC_OR_PART: topic momentarily missing. Tolerated for
      UNKNOWN_TOPIC_TOLERANCE_SECONDS of contiguous failures; if a single
      successful poll occurs, the budget resets. If the budget runs out,
      we raise — a permanently-missing topic indicates a misconfig and
      should crash loudly.
    """
    msg = consumer.poll(timeout=1.0)
    if msg is None:
        _unknown_topic_budget.reset()
        await asyncio.sleep(0.1)
        return None
    err = msg.error()
    if err is not None:
        if err.code() == KafkaError._PARTITION_EOF:
            _unknown_topic_budget.reset()
            return None
        if err.code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
            within_budget = _unknown_topic_budget.record()
            if within_budget:
                logger.warning(
                    "Kafka topic transiently missing (within tolerance): %s",
                    err,
                )
                await asyncio.sleep(1.0)
                return None
            logger.error(
                "Kafka topic missing for more than %.0fs — escalating",
                UNKNOWN_TOPIC_TOLERANCE_SECONDS,
            )
            raise KafkaException(err)
        raise KafkaException(err)
    _unknown_topic_budget.reset()
    raw_value = msg.value()
    assert raw_value is not None
    return raw_value, msg
