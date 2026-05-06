import logging
import os
from pathlib import Path

import asyncpg
from confluent_kafka import Consumer
from grosh_shared.models import Topic
from grosh_shared.user_db import acquire_user_lock

from grosh_consumer.kafka import poll_message
from grosh_consumer.models.normalized import NormalizedTransaction
from grosh_consumer.repositories.account_repo import AccountNotFoundError
from grosh_consumer.services.pipeline import PipelineOrchestrator

logger = logging.getLogger(__name__)

_HEALTH_FILE = Path("/tmp/healthy-pipeline")


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
            result = await poll_message(consumer)
            if result is None:
                continue
            raw_value, msg = result

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
                async with pool.acquire() as conn, conn.transaction():
                    await acquire_user_lock(conn, tx.user_id)
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
