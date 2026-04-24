import logging

from confluent_kafka import KafkaError, Message

logger = logging.getLogger(__name__)


def on_delivery(err: KafkaError | None, msg: Message) -> None:
    if err is not None:
        logger.error("Kafka delivery failed for %s: %s", msg.topic(), err)
