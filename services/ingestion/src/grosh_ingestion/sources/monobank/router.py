"""Monobank routers: lifecycle (link / list / delete) and webhook (receive)."""

import asyncio
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
from grosh_shared.jobs import JobStatusResponse, JobTriggerResponse
from grosh_shared.models import Topic, UserRole
from kubernetes.client.exceptions import ApiException
from pydantic import BaseModel, ValidationError

from grosh_ingestion.deps import (
    get_backfill_service,
    get_current_user_id,
    get_db_conn,
    get_job_status_service,
    get_producer,
    get_user_repo,
)
from grosh_ingestion.kafka import on_delivery
from grosh_ingestion.repositories.account_repo import AccountRepo
from grosh_ingestion.repositories.user_repo import UserRepo
from grosh_ingestion.services.backfill_service import BackfillService
from grosh_ingestion.services.job_status_service import JobStatusService
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
@webhook_router.get(
    "/webhook/{webhook_secret}", status_code=200, response_class=Response
)
async def verify_webhook(webhook_secret: str) -> None:  # noqa: ARG001
    return


@webhook_router.post(
    "/webhook/{webhook_secret}", status_code=200, response_class=Response
)
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


@lifecycle_router.post(
    "/accounts/{account_id}/backfill",
    response_model=JobTriggerResponse,
    status_code=202,
)
async def trigger_backfill(
    account_id: UUID,
    user_id: Annotated[UUID, Depends(get_current_user_id)],
    conn: Annotated[asyncpg.Connection, Depends(get_db_conn)],
    account_repo: Annotated[AccountRepo, Depends(get_account_repo)],
    user_repo: Annotated[UserRepo, Depends(get_user_repo)],
    backfill_service: Annotated[BackfillService, Depends(get_backfill_service)],
    from_date: date | None = Query(None, alias="from"),
    to_date: date | None = Query(None, alias="to"),
) -> JobTriggerResponse:
    """Trigger a historical transaction backfill for a Monobank account."""
    today = date.today()
    if to_date is None:
        to_date = today
    if from_date is None:
        from_date = to_date - timedelta(days=90)

    if from_date >= to_date:
        raise_problem(
            422,
            ErrorCode.INVALID_DATE_RANGE,
            "from must be earlier than to",
        )

    window_days = (to_date - from_date).days
    if window_days > 31:
        raise_problem(
            422,
            ErrorCode.BACKFILL_WINDOW_TOO_LARGE,
            f"Requested window is {window_days} days; Monobank API caps a single"
            f" request at 31 days. Split larger windows into multiple calls.",
        )

    role = await user_repo.get_role(conn, user_id)
    is_admin = role == UserRole.admin

    if is_admin:
        account_user_id = await account_repo.get_user_id(conn, account_id)
        if account_user_id is None:
            raise_problem(404, ErrorCode.ACCOUNT_NOT_FOUND, "Account not found.")
        result = await account_repo.get_external_ref(conn, account_id, account_user_id)
    else:
        result = await account_repo.get_external_ref(conn, account_id, user_id)
        account_user_id = user_id

    if result is None:
        raise_problem(404, ErrorCode.ACCOUNT_NOT_FOUND, "Account not found.")
    external_id, integration_id = result

    from_ts = int(
        datetime.combine(from_date, datetime.min.time(), tzinfo=UTC).timestamp()
    )
    to_ts = int(datetime.combine(to_date, datetime.min.time(), tzinfo=UTC).timestamp())

    job_name = backfill_service.trigger_transactions_backfill(
        account_id=account_id,
        integration_id=integration_id,
        user_id=account_user_id,
        account_external_id=external_id,
        from_timestamp=from_ts,
        to_timestamp=to_ts,
    )
    status_url = f"/v1/monobank/accounts/{account_id}/backfill/{job_name}"
    return JobTriggerResponse(job_id=job_name, status_url=status_url)


@lifecycle_router.get(
    "/accounts/{account_id}/backfill/{job_id}",
    response_model=JobStatusResponse,
)
async def get_backfill_status(
    account_id: UUID,
    job_id: str,
    user_id: Annotated[UUID, Depends(get_current_user_id)],
    conn: Annotated[asyncpg.Connection, Depends(get_db_conn)],
    account_repo: Annotated[AccountRepo, Depends(get_account_repo)],
    user_repo: Annotated[UserRepo, Depends(get_user_repo)],
    status_service: Annotated[JobStatusService, Depends(get_job_status_service)],
) -> JobStatusResponse:
    """Poll the status of a Monobank transaction backfill K8s Job.

    Returns 404 if the job does not exist or its labels do not match the
    URL's account_id / owner user_id (IDOR defense).
    Returns 503 if the K8s API is unreachable.
    """
    role = await user_repo.get_role(conn, user_id)
    is_admin = role == UserRole.admin

    if is_admin:
        account_user_id = await account_repo.get_user_id(conn, account_id)
    else:
        account_user_id = await account_repo.get_user_id(conn, account_id)
        if account_user_id != user_id:
            raise_problem(404, ErrorCode.ACCOUNT_NOT_FOUND, "Account not found.")

    if account_user_id is None:
        raise_problem(404, ErrorCode.ACCOUNT_NOT_FOUND, "Account not found.")

    expected_labels = {
        "grosh.app/job-kind": "monobank_backfill",
        "grosh.app/account-id": str(account_id),
        "grosh.app/user-id": str(account_user_id),
    }
    try:
        result = await asyncio.to_thread(
            status_service.fetch_status,
            job_id,
            expected_labels,
        )
    except ApiException:
        raise_problem(
            503,
            ErrorCode.JOB_STATUS_UNAVAILABLE,
            "K8s API is unavailable. Try again later.",
        )

    if result is None:
        raise_problem(
            404,
            ErrorCode.JOB_NOT_FOUND,
            f"Job {job_id!r} not found for account {account_id}.",
        )

    return result
