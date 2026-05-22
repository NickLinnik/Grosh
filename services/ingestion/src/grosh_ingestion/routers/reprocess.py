"""Per-user reprocess endpoints.

POST /v1/users/{user_id}/reprocess  — trigger reprocess for one user.
GET  /v1/users/{user_id}/reprocess/{job_id} — poll status.
"""

import asyncio
import logging
from typing import Annotated
from uuid import UUID

import asyncpg
from fastapi import APIRouter, Depends
from grosh_shared.errors import ErrorCode, raise_problem
from grosh_shared.jobs import JobStatusResponse, JobTriggerResponse
from grosh_shared.models import UserRole
from kubernetes.client.exceptions import ApiException

from grosh_ingestion.deps import (
    get_current_user_id,
    get_db_conn,
    get_job_status_service,
    get_reprocess_dispatcher,
    get_reprocess_repo,
    get_user_repo,
)
from grosh_ingestion.repositories.reprocess_repo import ReprocessRepo
from grosh_ingestion.repositories.user_repo import UserRepo
from grosh_ingestion.services.job_status_service import JobStatusService
from grosh_ingestion.services.reprocess_dispatcher import ReprocessDispatcher

_REPROCESS_LABEL_TEMPLATE = "grosh.app/job-kind=reprocess,grosh.app/user-id={user_id}"

logger = logging.getLogger(__name__)

router = APIRouter(tags=["reprocess"])


@router.post(
    "/users/{user_id}/reprocess",
    response_model=JobTriggerResponse,
    status_code=202,
)
async def trigger_user_reprocess(
    user_id: UUID,
    caller_id: Annotated[UUID, Depends(get_current_user_id)],
    conn: Annotated[asyncpg.Connection, Depends(get_db_conn)],
    repo: Annotated[ReprocessRepo, Depends(get_reprocess_repo)],
    user_repo: Annotated[UserRepo, Depends(get_user_repo)],
    dispatcher: Annotated[ReprocessDispatcher, Depends(get_reprocess_dispatcher)],
    status_service: Annotated[JobStatusService, Depends(get_job_status_service)],
) -> JobTriggerResponse:
    """Trigger a full transaction reprocess K8s Job for the given user.

    The caller must be the user themselves or an admin. The lock row is
    inserted atomically inside the DB transaction; if K8s job submission
    fails the transaction rolls back and no lock row is left.
    """
    role = await user_repo.get_role(conn, caller_id)
    if caller_id != user_id and role != UserRole.admin:
        raise_problem(
            403,
            ErrorCode.INSUFFICIENT_PERMISSIONS,
            f"User {caller_id} cannot trigger reprocess for user {user_id}.",
        )

    claimed = await user_repo.claim_reprocess_slot(conn, user_id)
    if not claimed:
        next_at = await user_repo.get_next_eligible_at(conn, user_id)
        next_at_str = next_at.isoformat() if next_at else "unknown"
        raise_problem(
            429,
            ErrorCode.RATE_LIMITED,
            f"Reprocess rate limit exceeded for user {user_id}."
            f" Next reprocess allowed at {next_at_str}.",
        )

    inserted = await repo.insert_lock_atomic(conn, user_id)
    if not inserted:
        label_selector = _REPROCESS_LABEL_TEMPLATE.format(user_id=user_id)
        try:
            job_id = await asyncio.to_thread(
                status_service.find_active_job,
                label_selector,
            )
        except ApiException:
            job_id = None

        if job_id is not None:
            status_url = f"/v1/users/{user_id}/reprocess/{job_id}"
            raise_problem(
                409,
                ErrorCode.REPROCESS_LOCKED,
                f"Reprocessing already in progress for user {user_id}."
                f" Existing job: {job_id}. Poll {status_url} for progress.",
            )
        else:
            raise_problem(
                409,
                ErrorCode.REPROCESS_LOCKED,
                f"Reprocessing already in progress for user {user_id}."
                " The previous job may have crashed;"
                " contact an administrator to clear the lock.",
            )

    job_name = await asyncio.to_thread(
        dispatcher.submit,
        [user_id],
        user_id,
    )
    logger.info("Triggered reprocess job %s for user %s", job_name, user_id)

    status_url = f"/v1/users/{user_id}/reprocess/{job_name}"
    return JobTriggerResponse(job_id=job_name, status_url=status_url)


@router.get(
    "/users/{user_id}/reprocess/{job_id}",
    response_model=JobStatusResponse,
)
async def get_user_reprocess_status(
    user_id: UUID,
    job_id: str,
    caller_id: Annotated[UUID, Depends(get_current_user_id)],
    conn: Annotated[asyncpg.Connection, Depends(get_db_conn)],
    user_repo: Annotated[UserRepo, Depends(get_user_repo)],
    status_service: Annotated[JobStatusService, Depends(get_job_status_service)],
) -> JobStatusResponse:
    """Poll the status of a per-user reprocess K8s Job.

    Returns 404 if the job does not exist or its labels do not match the
    URL's user_id (IDOR defense — does not reveal jobs belonging to other users).
    Returns 503 if the K8s API is unreachable.
    """
    role = await user_repo.get_role(conn, caller_id)
    if caller_id != user_id and role != UserRole.admin:
        raise_problem(
            403,
            ErrorCode.INSUFFICIENT_PERMISSIONS,
            f"User {caller_id} cannot view reprocess status for user {user_id}.",
        )

    expected_labels = {
        "grosh.app/job-kind": "reprocess",
        "grosh.app/user-id": str(user_id),
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
            f"Job {job_id!r} not found for user {user_id}.",
        )

    return result
