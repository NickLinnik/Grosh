import asyncio
import logging
import os
from pathlib import Path

import asyncpg
from confluent_kafka import Consumer, KafkaError, KafkaException
from grosh_shared.models import Topic

from grosh_consumer.models.normalized import NormalizedTransaction
from grosh_consumer.repositories.account_repo import AccountNotFoundError
from grosh_consumer.services.pipeline import PipelineOrchestrator

logger = logging.getLogger(__name__)

_HEALTH_FILE = Path("/tmp/healthy")


async def run_pipeline_consumer(
    pool: asyncpg.Pool, orchestrator: PipelineOrchestrator
) -> None:
    """Consume NormalizedTransaction events and run the enrichment pipeline."""
    conf = {
        "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092"),
        "group.id": "transaction-pipeline",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
    consumer = Consumer(conf)
    consumer.subscribe([Topic.normalized_transactions])

    logger.info(
        "Pipeline consumer started, subscribed to %s",
        Topic.normalized_transactions,
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
                tx = NormalizedTransaction.model_validate_json(raw_value)
            except Exception:
                logger.exception(
                    "Failed to deserialize NormalizedTransaction: %s",
                    raw_value,
                )
                consumer.commit(message=msg)
                continue

            try:
                async with pool.acquire() as conn:
                    await orchestrator.run(conn, tx)
            except AccountNotFoundError:
                # Permanent: account row doesn't exist and never will until
                # re-linked. Retrying won't help — skip and commit.
                logger.error(
                    "Account not found for transaction %s (source=%s, source_id=%s)"
                    "; skipping",
                    tx.id,
                    tx.source,
                    tx.source_id,
                )
                consumer.commit(message=msg)
                continue
            except Exception:
                # Transient: DB down, chain misconfigured, etc. k8s restarts
                # the container and the uncommitted message is redelivered.
                logger.exception(
                    "Failed to process transaction %s (source=%s, source_id=%s)",
                    tx.id,
                    tx.source,
                    tx.source_id,
                )
                raise

            consumer.commit(message=msg)
    finally:
        consumer.close()
