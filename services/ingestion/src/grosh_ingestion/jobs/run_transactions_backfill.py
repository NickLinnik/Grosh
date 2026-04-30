"""Standalone backfill script for historical transactions.

Intended to run as a K8s Job:
    python -m grosh_ingestion.jobs.run_transactions_backfill

Required environment variables:
    BACKFILL_INTEGRATION_ID        UUID of the bank integration
    BACKFILL_USER_ID               UUID of the user who owns the integration
    BACKFILL_ACCOUNT_EXTERNAL_ID   External account ID at the bank
    BACKFILL_FROM_TIMESTAMP        Unix timestamp (int string) — range start
    BACKFILL_TO_TIMESTAMP          Unix timestamp (int string) — range end
    DATABASE_URL                   PostgreSQL connection string
    KAFKA_BOOTSTRAP_SERVERS        Optional, default "redpanda:9092"
"""

import asyncio
import logging
import os
from datetime import UTC, datetime
from uuid import UUID

import asyncpg
from confluent_kafka import Producer
from grosh_shared.db_url import for_asyncpg

from grosh_ingestion.registry import TRANSACTION_BACKFILL_PROVIDERS
from grosh_ingestion.repositories.integration_repo import IntegrationRepo

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

_integration_repo = IntegrationRepo()


async def main() -> None:
    integration_id = UUID(os.environ["BACKFILL_INTEGRATION_ID"])
    user_id = UUID(os.environ["BACKFILL_USER_ID"])
    account_external_id = os.environ["BACKFILL_ACCOUNT_EXTERNAL_ID"]
    from_timestamp = int(os.environ["BACKFILL_FROM_TIMESTAMP"])
    to_timestamp = int(os.environ["BACKFILL_TO_TIMESTAMP"])
    dsn = for_asyncpg(os.environ["DATABASE_URL"])
    bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")

    pool = await asyncpg.create_pool(dsn)
    try:
        producer = Producer({"bootstrap.servers": bootstrap_servers})

        async with pool.acquire() as conn:
            bank_source = await _integration_repo.get_bank_source(conn, integration_id)

        if bank_source is None:
            raise ValueError(f"Integration {integration_id} not found")

        provider = TRANSACTION_BACKFILL_PROVIDERS.get(bank_source)
        if provider is None:
            raise ValueError(
                f"No backfill provider registered for source '{bank_source}'"
            )

        logger.info(
            "Starting transaction backfill: integration=%s source=%s from=%s to=%s",
            integration_id,
            bank_source,
            datetime.fromtimestamp(from_timestamp, tz=UTC).isoformat(),
            datetime.fromtimestamp(to_timestamp, tz=UTC).isoformat(),
        )

        await provider.run_backfill(
            pool=pool,
            producer=producer,
            integration_id=integration_id,
            user_id=user_id,
            account_external_id=account_external_id,
            from_timestamp=from_timestamp,
            to_timestamp=to_timestamp,
        )

        logger.info("Transaction backfill complete for integration %s", integration_id)
    finally:
        producer.flush(timeout=30)
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
