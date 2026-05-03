import logging
import os
from datetime import UTC, date, datetime, timedelta
from typing import Annotated
from uuid import UUID

import asyncpg
from confluent_kafka import Producer
from fastapi import APIRouter, Depends, HTTPException, Query
from grosh_shared.envelope import TransactionEnvelope
from grosh_shared.models import Topic
from pydantic import BaseModel, ValidationError

from grosh_ingestion.deps import (
    get_backfill_service,
    get_current_user_id,
    get_db_conn,
    get_producer,
)
from grosh_ingestion.kafka import on_delivery
from grosh_ingestion.repositories.account_repo import AccountRepo
from grosh_ingestion.services.backfill_service import BackfillService
from grosh_ingestion.sources.monobank.linking_service import (
    MonobankLinkingService,
    get_monobank_linking_service,
)
from grosh_ingestion.sources.monobank.models import (
    MonobankStatementItem,
    MonobankWebhookPayload,
)
from grosh_ingestion.sources.monobank.repo import MonobankRepo

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
    encryption_key = os.environ["ENCRYPTION_KEY"]
    key_version = int(os.environ.get("TOKEN_KEY_VERSION", "1"))

    return await service.link(
        conn=conn,
        user_id=user_id,
        token=body.token,
        webhook_base_url=webhook_base_url,
        encryption_key=encryption_key,
        key_version=key_version,
    )


@router.post("/relink", status_code=200)
async def relink_monobank(
    body: LinkMonobankRequest,
    user_id: Annotated[UUID, Depends(get_current_user_id)],
    conn: Annotated[asyncpg.Connection, Depends(get_db_conn)],
    service: Annotated[MonobankLinkingService, Depends(get_monobank_linking_service)],
) -> dict:
    """Re-register webhook for an existing Monobank integration."""
    webhook_base_url = os.environ["WEBHOOK_BASE_URL"]
    encryption_key = os.environ["ENCRYPTION_KEY"]
    key_version = int(os.environ.get("TOKEN_KEY_VERSION", "1"))

    return await service.relink(
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
        logger.warning("Unknown webhook_secret received")
        raise HTTPException(status_code=404, detail="Unknown webhook.")

    account = await repo.get_account_by_external_id(
        conn, payload.data.account, integration.id
    )
    if account is None:
        logger.warning(
            "Account %s not found for integration %s",
            payload.data.account,
            integration.id,
        )
        raise HTTPException(status_code=404, detail="Unknown account.")

    try:
        # Validate early to catch malformed payloads before publishing.
        MonobankStatementItem.model_validate(payload.data.statement_item)
    except (ValidationError, ValueError):
        logger.exception(
            "Failed to parse webhook payload for integration %s", integration.id
        )
        raise HTTPException(status_code=422, detail="Invalid statement payload.")

    envelope = TransactionEnvelope(
        user_id=integration.user_id,
        account_id=account.id,
        source="monobank",
        payload=payload.data.statement_item,
    )

    # Fire-and-forget: Monobank retries webhook delivery on failure.
    # poll(0) triggers pending delivery callbacks for error logging.
    producer.produce(
        topic=Topic.raw_transactions_monobank,
        key=str(integration.user_id).encode(),
        value=envelope.model_dump_json().encode(),
        on_delivery=on_delivery,
    )
    producer.poll(0)

    logger.info(
        "Published transaction envelope for user %s to %s",
        integration.user_id,
        Topic.raw_transactions_monobank,
    )


# -- Backfill --


_account_repo = AccountRepo()


def get_account_repo() -> AccountRepo:
    return _account_repo


@router.post("/accounts/{account_id}/backfill", status_code=202)
async def trigger_backfill(
    account_id: UUID,
    user_id: Annotated[UUID, Depends(get_current_user_id)],
    conn: Annotated[asyncpg.Connection, Depends(get_db_conn)],
    account_repo: Annotated[AccountRepo, Depends(get_account_repo)],
    backfill_service: Annotated[BackfillService, Depends(get_backfill_service)],
    from_date: date | None = Query(None, alias="from"),
    to_date: date | None = Query(None, alias="to"),
) -> dict:
    """Trigger a historical transaction backfill for a Monobank account."""
    result = await account_repo.get_external_ref(conn, account_id, user_id)
    if result is None:
        raise HTTPException(status_code=404, detail="Account not found.")
    external_id, integration_id = result

    today = date.today()
    if to_date is None:
        to_date = today
    if from_date is None:
        from_date = to_date - timedelta(days=90)

    if from_date >= to_date:
        raise HTTPException(
            status_code=422,
            detail="'from' must be before 'to'.",
        )

    from_ts = int(
        datetime.combine(from_date, datetime.min.time(), tzinfo=UTC).timestamp()
    )
    to_ts = int(datetime.combine(to_date, datetime.max.time(), tzinfo=UTC).timestamp())

    job_name = backfill_service.trigger_transactions_backfill(
        integration_id=integration_id,
        user_id=user_id,
        account_external_id=external_id,
        from_timestamp=from_ts,
        to_timestamp=to_ts,
    )
    return {"job_name": job_name, "status": "accepted"}
