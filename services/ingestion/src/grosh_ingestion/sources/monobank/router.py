"""Monobank routers: lifecycle (link / list / delete) and webhook (receive)."""

import logging
import os
from datetime import UTC, date, datetime, timedelta
from typing import Annotated
from uuid import UUID

import asyncpg
from confluent_kafka import Producer
from fastapi import APIRouter, Depends, Query, Response
from grosh_shared.envelope import TransactionEnvelope
from grosh_shared.errors import ErrorCode, raise_problem
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
from grosh_ingestion.sources.monobank.schemas import (
    MonobankIntegrationResponse,
    MonobankLinkResponse,
)

logger = logging.getLogger(__name__)

_monobank_repo = MonobankRepo()


def get_monobank_repo() -> MonobankRepo:
    return _monobank_repo


lifecycle_router = APIRouter(prefix="/monobank", tags=["monobank"])
webhook_router = APIRouter(prefix="/monobank", tags=["monobank"])


# ---------------------------------------------------------------------------
# Request models
# ---------------------------------------------------------------------------


class LinkMonobankRequest(BaseModel):
    token: str


# ---------------------------------------------------------------------------
# Lifecycle endpoints
# ---------------------------------------------------------------------------


@lifecycle_router.post("/link", response_model=MonobankLinkResponse)
async def link_monobank(
    body: LinkMonobankRequest,
    response: Response,
    user_id: Annotated[UUID, Depends(get_current_user_id)],
    conn: Annotated[asyncpg.Connection, Depends(get_db_conn)],
    service: Annotated[MonobankLinkingService, Depends(get_monobank_linking_service)],
) -> MonobankLinkResponse:
    """Link a Monobank account via personal token (idempotent on monobank_client_id).

    Returns 201 when a fresh integration row is created, 200 when only the
    token is rotated on an existing integration.
    """
    webhook_base_url = os.environ["WEBHOOK_BASE_URL"]
    encryption_key = os.environ["ENCRYPTION_KEY"]
    key_version = int(os.environ.get("TOKEN_KEY_VERSION", "1"))

    result = await service.link(
        conn=conn,
        user_id=user_id,
        token=body.token,
        webhook_base_url=webhook_base_url,
        encryption_key=encryption_key,
        key_version=key_version,
    )
    response.status_code = 201 if result.is_new else 200
    return result


@lifecycle_router.get(
    "/integrations",
    response_model=list[MonobankIntegrationResponse],
)
async def list_integrations(
    user_id: Annotated[UUID, Depends(get_current_user_id)],
    conn: Annotated[asyncpg.Connection, Depends(get_db_conn)],
    service: Annotated[MonobankLinkingService, Depends(get_monobank_linking_service)],
) -> list[MonobankIntegrationResponse]:
    """List the current user's Monobank integrations."""
    return await service.list_integrations(conn=conn, user_id=user_id)


@lifecycle_router.delete(
    "/integrations/{integration_id}",
    status_code=204,
)
async def delete_integration(
    integration_id: UUID,
    user_id: Annotated[UUID, Depends(get_current_user_id)],
    conn: Annotated[asyncpg.Connection, Depends(get_db_conn)],
    service: Annotated[MonobankLinkingService, Depends(get_monobank_linking_service)],
) -> Response:
    """Hard-delete a Monobank integration.

    Accounts and transactions remain in the DB untouched. Webhook
    de-registration is best-effort.
    """
    encryption_key = os.environ["ENCRYPTION_KEY"]
    deleted = await service.unlink(
        conn=conn,
        integration_id=integration_id,
        user_id=user_id,
        encryption_key=encryption_key,
    )
    if not deleted:
        raise_problem(
            404,
            ErrorCode.INTEGRATION_NOT_FOUND,
            "Integration not found.",
        )
    return Response(status_code=204)


# ---------------------------------------------------------------------------
# Webhook endpoints
# ---------------------------------------------------------------------------


# noinspection PyUnusedLocal
@webhook_router.get("/webhook/{webhook_secret}", status_code=200)
async def verify_webhook(webhook_secret: str) -> None:  # noqa: ARG001
    return


@webhook_router.post("/webhook/{webhook_secret}", status_code=200)
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
        raise_problem(404, ErrorCode.INTEGRATION_NOT_FOUND, "Unknown webhook.")

    account = await repo.get_account_by_external_id(
        conn, payload.data.account, integration.id
    )
    if account is None:
        logger.warning(
            "Account %s not found for integration %s",
            payload.data.account,
            integration.id,
        )
        raise_problem(404, ErrorCode.ACCOUNT_NOT_FOUND, "Unknown account.")

    try:
        # Validate early to catch malformed payloads before publishing.
        MonobankStatementItem.model_validate(payload.data.statement_item)
    except (ValidationError, ValueError):
        logger.exception(
            "Failed to parse webhook payload for integration %s", integration.id
        )
        raise_problem(422, ErrorCode.VALIDATION_ERROR, "Invalid statement payload.")

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


# ---------------------------------------------------------------------------
# Backfill endpoint
# ---------------------------------------------------------------------------


_account_repo = AccountRepo()


def get_account_repo() -> AccountRepo:
    return _account_repo


@lifecycle_router.post("/accounts/{account_id}/backfill", status_code=202)
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
        raise_problem(404, ErrorCode.ACCOUNT_NOT_FOUND, "Account not found.")
    external_id, integration_id = result

    today = date.today()
    if to_date is None:
        to_date = today
    if from_date is None:
        from_date = to_date - timedelta(days=90)

    if from_date >= to_date:
        raise_problem(422, ErrorCode.INVALID_DATE_RANGE, "'from' must be before 'to'.")

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
