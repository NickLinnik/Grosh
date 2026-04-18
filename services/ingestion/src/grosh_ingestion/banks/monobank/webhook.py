import logging
from typing import Annotated

import asyncpg
from confluent_kafka import KafkaError, Message, Producer
from fastapi import APIRouter, Depends
from grosh_shared.models import Topic
from pydantic import ValidationError

from grosh_ingestion.banks.monobank.adapter import to_raw_transaction_event
from grosh_ingestion.banks.monobank.models import (
    MonobankStatementItem,
    MonobankWebhookPayload,
)
from grosh_ingestion.deps import get_account_repo, get_db_conn, get_producer
from grosh_ingestion.repositories.account_repo import AccountRepo

logger = logging.getLogger(__name__)


def _on_delivery(err: KafkaError | None, msg: Message) -> None:
    if err is not None:
        logger.error("Kafka delivery failed for %s: %s", msg.topic(), err)


router = APIRouter(prefix="/webhook/monobank", tags=["webhook"])


# noinspection PyUnusedLocal
@router.get("/{webhook_secret}", status_code=200)
async def verify_webhook(webhook_secret: str) -> None:  # noqa: ARG001
    return


@router.post("/{webhook_secret}", status_code=200)
async def receive_webhook(
    webhook_secret: str,
    payload: MonobankWebhookPayload,
    conn: Annotated[asyncpg.Connection, Depends(get_db_conn)],
    producer: Annotated[Producer, Depends(get_producer)],
    account_repo: Annotated[AccountRepo, Depends(get_account_repo)],
) -> None:
    """Receive a Monobank transaction push, validate, publish to Redpanda."""
    integration = await account_repo.get_active_integration_by_webhook_secret(
        conn, webhook_secret
    )
    if integration is None:
        logger.warning("Unknown webhook_secret received — ignoring payload")
        return

    account = await account_repo.get_account_by_external_id(
        conn, payload.data.account, integration.id
    )
    if account is None:
        logger.warning(
            "Account %s not found for integration %s — ignoring payload",
            payload.data.account,
            integration.id,
        )
        return

    try:
        statement_item = MonobankStatementItem.model_validate(
            payload.data.statement_item
        )
        event = to_raw_transaction_event(
            statement_item, integration.user_id, account.id
        )
    except (ValidationError, ValueError):
        logger.exception(
            "Failed to parse webhook payload for integration %s", integration.id
        )
        return

    producer.produce(
        topic=Topic.raw_transactions,
        key=str(integration.user_id).encode(),
        value=event.model_dump_json().encode(),
        on_delivery=_on_delivery,
    )
    producer.poll(0)

    logger.info(
        "Published raw transaction %s for user %s to %s",
        event.id,
        integration.user_id,
        Topic.raw_transactions,
    )
