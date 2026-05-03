import asyncio
import logging
import os
from datetime import UTC, datetime
from uuid import UUID

import asyncpg
from confluent_kafka import Producer
from grosh_shared.envelope import TransactionEnvelope
from grosh_shared.models import Topic

from grosh_ingestion.kafka import on_delivery
from grosh_ingestion.sources.monobank.client import MonobankAPIError, MonobankClient
from grosh_ingestion.sources.monobank.repo import MonobankRepo

logger = logging.getLogger(__name__)

_CHUNK_SECONDS = 2_678_400  # 31 days
_RATE_LIMIT_PAUSE = 61  # seconds between Monobank API calls


class MonobankBackfillProvider:
    def __init__(self) -> None:
        self._repo = MonobankRepo()

    async def run_backfill(
        self,
        pool: asyncpg.Pool,
        producer: Producer,
        integration_id: UUID,
        user_id: UUID,
        account_external_id: str,
        from_timestamp: int,
        to_timestamp: int,
    ) -> None:
        encryption_key = os.environ["ENCRYPTION_KEY"]

        async with pool.acquire() as conn:
            token = await self._repo.decrypt_token(conn, integration_id, encryption_key)
            if token is None:
                raise ValueError(f"Integration {integration_id} not found or inactive")

            account_ref = await self._repo.get_account_by_external_id(
                conn, account_external_id, integration_id
            )
            if account_ref is None:
                raise ValueError(
                    f"Account external_id={account_external_id} "
                    f"for integration {integration_id} not found"
                )
            account_id = account_ref.id

        async with MonobankClient(token) as client:
            current_to = to_timestamp

            try:
                while current_to > from_timestamp:
                    current_from = max(from_timestamp, current_to - _CHUNK_SECONDS)

                    try:
                        statements = await client.get_statements(
                            account_external_id, current_from, current_to
                        )
                    except MonobankAPIError as e:
                        if e.status_code == 400 and "out of bounds" in e.message:
                            logger.info(
                                "Monobank returned 'out of bounds' for period"
                                " %s -> %s — reached account creation date,"
                                " stopping",
                                datetime.fromtimestamp(current_from, tz=UTC).date(),
                                datetime.fromtimestamp(current_to, tz=UTC).date(),
                            )
                            break
                        raise

                    logger.info(
                        "Fetched %d statements for period %s -> %s",
                        len(statements),
                        datetime.fromtimestamp(current_from, tz=UTC).date(),
                        datetime.fromtimestamp(current_to, tz=UTC).date(),
                    )

                    for item in statements:
                        envelope = TransactionEnvelope(
                            user_id=user_id,
                            account_id=account_id,
                            source="monobank",
                            payload=item.model_dump(by_alias=True),
                        )
                        producer.produce(
                            topic=Topic.raw_transactions_monobank,
                            key=str(user_id).encode(),
                            value=envelope.model_dump_json().encode(),
                            on_delivery=on_delivery,
                        )
                    producer.poll(0)

                    current_to = current_from - 1
                    if current_to > from_timestamp:
                        logger.info("Rate limit pause before next chunk")
                        await asyncio.sleep(_RATE_LIMIT_PAUSE)
            finally:
                producer.flush(timeout=30)

        logger.info(
            "Transactions backfill completed for integration %s",
            integration_id,
        )
