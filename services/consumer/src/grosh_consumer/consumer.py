import asyncio
import logging
import os
from pathlib import Path

from confluent_kafka import Consumer, KafkaError, KafkaException
from grosh_shared.events import RawTransactionEvent
from grosh_shared.models import Topic

from grosh_consumer.db import create_pool
from grosh_consumer.handlers.transaction_handler import TransactionHandler
from grosh_consumer.repositories.account_repo import AccountNotFoundError, AccountRepo
from grosh_consumer.repositories.currency_rate_repo import CurrencyRateRepo
from grosh_consumer.repositories.transaction_repo import TransactionRepo
from grosh_consumer.services.currency_conversion_service import (
    CurrencyConversionService,
)

logger = logging.getLogger(__name__)

_HEALTH_FILE = Path("/tmp/healthy")


async def run() -> None:
    pool = await create_pool()

    transaction_repo = TransactionRepo()
    account_repo = AccountRepo()
    rate_repo = CurrencyRateRepo()
    conversion_service = CurrencyConversionService(rate_repo)
    transaction_handler = TransactionHandler(
        transaction_repo, account_repo, conversion_service
    )

    conf = {
        "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092"),
        "group.id": "transaction-pipeline",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
    consumer = Consumer(conf)
    consumer.subscribe([Topic.raw_transactions])

    logger.info("Consumer started, subscribed to %s", Topic.raw_transactions)

    try:
        while True:
            _HEALTH_FILE.touch()
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                await asyncio.sleep(0.1)
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())

            # Deserialization errors: skip and commit. The message is
            # malformed — retrying won't fix it. Log raw bytes for
            # post-mortem debugging.
            try:
                event = RawTransactionEvent.model_validate_json(msg.value())
            except Exception:
                logger.exception(
                    "Failed to deserialize message: %s",
                    msg.value(),
                )
                consumer.commit(message=msg)
                continue

            # Processing errors: distinguish permanent from transient failures.
            try:
                async with pool.acquire() as conn:
                    await transaction_handler.handle(conn, event)
            except AccountNotFoundError:
                # Permanent: account row doesn't exist and never will until
                # re-linked. Retrying won't help — skip and commit.
                logger.error(
                    "Account not found for event %s (source=%s, source_id=%s)"
                    "; skipping",
                    event.id,
                    event.source,
                    event.source_id,
                )
                consumer.commit(message=msg)
                continue
            except Exception:
                # Transient: DB down, chain misconfigured, etc. k8s restarts
                # the container and the uncommitted message is redelivered.
                logger.exception(
                    "Failed to handle event %s (source=%s, source_id=%s)",
                    event.id,
                    event.source,
                    event.source_id,
                )
                raise

            consumer.commit(message=msg)
    finally:
        consumer.close()
        await pool.close()
