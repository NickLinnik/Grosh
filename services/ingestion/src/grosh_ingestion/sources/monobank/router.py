import logging
import os
from typing import Annotated
from uuid import UUID

import asyncpg
from confluent_kafka import Producer
from fastapi import APIRouter, Depends
from grosh_shared.models import Topic
from pydantic import BaseModel, ValidationError

from grosh_ingestion.deps import get_current_user_id, get_db_conn, get_producer
from grosh_ingestion.kafka import on_delivery
from grosh_ingestion.sources.monobank.linking_service import (
    MonobankLinkingService,
    get_monobank_linking_service,
)
from grosh_ingestion.sources.monobank.models import (
    MonobankStatementItem,
    MonobankWebhookPayload,
)
from grosh_ingestion.sources.monobank.repo import MonobankRepo
from grosh_ingestion.sources.monobank.transaction_adapter import (
    to_raw_transaction_event,
)

logger = logging.getLogger(__name__)

_monobank_repo = MonobankRepo()


def get_monobank_repo() -> MonobankRepo:
    return _monobank_repo


router = APIRouter(prefix="/monobank", tags=["monobank"])


# -- Account linking --


class LinkMonobankRequest(BaseModel):
    token: str


@router.post("/link", status_code=201)
async def link_monobank(
    body: LinkMonobankRequest,
    user_id: Annotated[UUID, Depends(get_current_user_id)],
    conn: Annotated[asyncpg.Connection, Depends(get_db_conn)],
    service: Annotated[MonobankLinkingService, Depends(get_monobank_linking_service)],
) -> dict:
    """Link a Monobank account via personal token."""
    webhook_base_url = os.environ["WEBHOOK_BASE_URL"]
    encryption_key = os.environ["TOKEN_ENCRYPTION_KEY"]
    key_version = int(os.environ.get("TOKEN_KEY_VERSION", "1"))

    return await service.link(
        conn=conn,
        user_id=user_id,
        token=body.token,
        webhook_base_url=webhook_base_url,
        encryption_key=encryption_key,
        key_version=key_version,
    )


# -- Webhook --


# noinspection PyUnusedLocal
@router.get("/webhook/{webhook_secret}", status_code=200)
async def verify_webhook(webhook_secret: str) -> None:  # noqa: ARG001
    return


@router.post("/webhook/{webhook_secret}", status_code=200)
async def receive_webhook(
    webhook_secret: str,
    payload: MonobankWebhookPayload,
    conn: Annotated[asyncpg.Connection, Depends(get_db_conn)],
    producer: Annotated[Producer, Depends(get_producer)],
    repo: Annotated[MonobankRepo, Depends(get_monobank_repo)],
) -> None:
    """Receive a Monobank transaction push, validate, publish to Redpanda."""
    integration = await repo.get_active_integration_by_webhook_secret(
        conn, webhook_secret
    )
    if integration is None:
        logger.warning("Unknown webhook_secret received — ignoring payload")
        return

    account = await repo.get_account_by_external_id(
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

    # Fire-and-forget: Monobank retries webhook delivery on failure.
    # poll(0) triggers pending delivery callbacks for error logging.
    producer.produce(
        topic=Topic.raw_transactions,
        key=str(integration.user_id).encode(),
        value=event.model_dump_json().encode(),
        on_delivery=on_delivery,
    )
    producer.poll(0)

    logger.info(
        "Published raw transaction %s for user %s to %s",
        event.id,
        integration.user_id,
        Topic.raw_transactions,
    )
