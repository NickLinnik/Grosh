"""Reprocessing job entrypoint: backup → delete → replay → verify, per user.

See reprocess_service.py for the full state machine and design notes.
"""

import asyncio
import json
import logging
import os
import sys
from uuid import UUID

import asyncpg
from confluent_kafka import Producer
from grosh_shared.db_url import for_asyncpg

from grosh_consumer.repositories.reprocess_repo import (
    ReprocessError,
    ReprocessRepo,
)
from grosh_consumer.repositories.transaction_repo import TransactionRepo
from grosh_consumer.services.reprocess_service import ReprocessService

logger = logging.getLogger(__name__)


async def run_reprocess(user_ids: list[UUID] | None = None) -> None:
    dsn = for_asyncpg(os.environ["DATABASE_URL"])
    session_conn = await asyncpg.connect(dsn)

    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
    producer = Producer({"bootstrap.servers": bootstrap, "enable.idempotence": "true"})

    repo = ReprocessRepo()
    transaction_repo = TransactionRepo()
    service = ReprocessService(repo, transaction_repo, producer)

    try:
        if user_ids is None:
            user_ids = await repo.list_all_user_ids(session_conn)

        for user_id in user_ids:
            logger.info("Starting reprocess for user %s", user_id)
            try:
                proceeded = await service.reprocess_user(session_conn, user_id)
                if proceeded:
                    logger.info("Completed reprocess for user %s", user_id)
            except ReprocessError:
                logger.exception(
                    "Reprocess failed for user %s but data was restored from backup; "
                    "safe to retry. Continuing to next user.",
                    user_id,
                )
            except Exception:
                logger.exception(
                    "Reprocess crashed unexpectedly for user %s; DB state may be "
                    "inconsistent — inspect reprocessing_locks and "
                    "reprocessing_backups before retrying. Continuing to next user.",
                    user_id,
                )
    finally:
        await session_conn.close()
        try:
            producer.flush(timeout=10)
        except Exception as flush_exc:
            logger.warning("Producer flush failed during shutdown: %s", flush_exc)


if __name__ == "__main__":
    raw = os.environ.get("USER_IDS_JSON")
    _user_ids: list[UUID] | None = None
    if raw:
        _parsed = json.loads(raw)  # raises ValueError on invalid JSON — loud and clear
        if not isinstance(_parsed, list):
            raise ValueError(
                f"USER_IDS_JSON must be a JSON array, got {type(_parsed).__name__}"
            )
        _user_ids = [UUID(item) for item in _parsed]  # raises ValueError on non-UUID

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stdout,
    )
    asyncio.run(run_reprocess(_user_ids))
