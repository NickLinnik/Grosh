import asyncio
import logging

from confluent_kafka import Consumer, KafkaError, KafkaException, Message

logger = logging.getLogger(__name__)


def on_delivery(err: KafkaError | None, msg: Message) -> None:
    if err is not None:
        logger.error("Kafka delivery failed for %s: %s", msg.topic(), err)


async def poll_message(consumer: Consumer) -> tuple[bytes, Message] | None:
    """Poll for the next message, handling EOF and transient errors.

    Returns (raw_value, msg) on success, None if no message available.
    Raises KafkaException on fatal errors.
    """
    msg = consumer.poll(timeout=1.0)
    if msg is None:
        await asyncio.sleep(0.1)
        return None
    err = msg.error()
    if err is not None:
        if err.code() == KafkaError._PARTITION_EOF:
            return None
        raise KafkaException(err)
    raw_value = msg.value()
    assert raw_value is not None
    return raw_value, msg
