"""Admin bulk-reprocess endpoints.

POST /v1/admin/reprocess          — trigger reprocess for all or selected users.
GET  /v1/admin/reprocess/{job_id} — poll status of a bulk reprocess job.
"""

import asyncio
import logging
from typing import Annotated
from uuid import UUID

import asyncpg
from fastapi import APIRouter, Depends
from grosh_shared.http.errors import ErrorCode, raise_problem
from grosh_shared.messaging.jobs import (
    BulkReprocessResponse,
    JobStatusResponse,
    SkippedUser,
)
from kubernetes.client.exceptions import ApiException
from pydantic import BaseModel, Field

from grosh_ingestion.deps import (
    get_db_conn,
    get_job_status_service,
    get_reprocess_dispatcher,
    get_reprocess_repo,
    get_user_repo,
    require_admin,
)
from grosh_ingestion.repositories.reprocess_repo import ReprocessRepo
from grosh_ingestion.repositories.user_repo import UserRepo
from grosh_ingestion.services.job_status_service import JobStatusService
from grosh_ingestion.services.reprocess_dispatcher import ReprocessDispatcher

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin", tags=["admin", "reprocess"])

# Labels that must be absent on admin bulk jobs (per-user jobs carry this).
_ADMIN_ABSENT_LABELS: frozenset[str] = frozenset({"grosh.app/user-id"})


class BulkReprocessRequest(BaseModel):
    user_ids: list[UUID] | None = None
    force: bool = Field(
        default=False,
        description=(
            "When True (admin only), bypass the per-user rate-limit check"
            " but still update the timestamp."
        ),
    )


@router.post(
    "/reprocess",
    response_model=BulkReprocessResponse,
    status_code=202,
)
async def trigger_bulk_reprocess(
    body: BulkReprocessRequest,
    caller_id: Annotated[UUID, Depends(require_admin)],
    conn: Annotated[asyncpg.Connection, Depends(get_db_conn)],
    repo: Annotated[ReprocessRepo, Depends(get_reprocess_repo)],
    user_repo: Annotated[UserRepo, Depends(get_user_repo)],
    dispatcher: Annotated[ReprocessDispatcher, Depends(get_reprocess_dispatcher)],
) -> BulkReprocessResponse:
    """Trigger a full reprocess K8s Job for all or selected users.

    If user_ids is null, a snapshot of all current user IDs is taken.
    Users created after this snapshot are NOT included — deliberate snapshot
    semantic to avoid moving-target issues.

    Users that already have a reprocessing_locks row are added to the skipped
    list and excluded from the job. If every target user is locked, no job is
    submitted and the response has job_id=null, status_url=null.
    """
    if body.user_ids is None:
        target_ids = await user_repo.list_all_ids(conn)
    else:
        target_ids = list(body.user_ids)

    successful_targets: list[UUID] = []
    skipped: list[SkippedUser] = []

    for uid in target_ids:
        if body.force:
            try:
                await user_repo.force_claim_reprocess_slot(conn, uid)
            except ValueError:
                skipped.append(SkippedUser(user_id=uid, reason="REPROCESS_LOCKED"))
                continue
        else:
            claimed = await user_repo.claim_reprocess_slot(conn, uid)
            if not claimed:
                skipped.append(SkippedUser(user_id=uid, reason="RATE_LIMITED"))
                continue

        inserted = await repo.insert_lock_atomic(conn, uid)
        if inserted:
            successful_targets.append(uid)
        else:
            skipped.append(SkippedUser(user_id=uid, reason="REPROCESS_LOCKED"))

    if not successful_targets:
        return BulkReprocessResponse(
            job_id=None,
            status_url=None,
            skipped=skipped,
        )

    job_name = await asyncio.to_thread(
        dispatcher.submit,
        successful_targets,
        None,
    )
    logger.info(
        "Triggered bulk reprocess job %s for %d user(s), %d skipped",
        job_name,
        len(successful_targets),
        len(skipped),
    )

    status_url = f"/v1/admin/reprocess/{job_name}"
    return BulkReprocessResponse(
        job_id=job_name,
        status_url=status_url,
        skipped=skipped,
    )


@router.get(
    "/reprocess/{job_id}",
    response_model=JobStatusResponse,
)
async def get_bulk_reprocess_status(
    job_id: str,
    caller_id: Annotated[UUID, Depends(require_admin)],
    status_service: Annotated[JobStatusService, Depends(get_job_status_service)],
) -> JobStatusResponse:
    """Poll the status of an admin bulk reprocess K8s Job.

    Verifies grosh.app/job-kind=reprocess and that grosh.app/user-id is absent.
    The absence check prevents per-user jobs from being visible via the admin
    endpoint (IDOR defense). If either label check fails, returns 404.
    """
    expected_labels = {"grosh.app/job-kind": "reprocess"}
    try:
        result = await asyncio.to_thread(
            status_service.fetch_status,
            job_id,
            expected_labels,
            _ADMIN_ABSENT_LABELS,
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
            f"Bulk reprocess job {job_id!r} not found.",
        )

    return result
