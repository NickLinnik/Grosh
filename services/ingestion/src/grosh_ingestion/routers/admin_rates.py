import asyncio
import logging
from datetime import date
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends
from grosh_shared.http.errors import ErrorCode, raise_problem
from grosh_shared.messaging.jobs import JobStatusResponse, JobTriggerResponse
from kubernetes.client.exceptions import ApiException
from pydantic import BaseModel

from grosh_ingestion.deps import (
    get_backfill_service,
    get_job_status_service,
    require_admin,
)
from grosh_ingestion.registry import RATE_PROVIDERS
from grosh_ingestion.services.backfill_service import BackfillService
from grosh_ingestion.services.job_status_service import JobStatusService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin", tags=["admin"])

# The absence of grosh.app/user-id distinguishes admin rates-backfill jobs
# from any hypothetical per-user jobs with the same kind label.
_RATES_BACKFILL_ABSENT_LABELS: frozenset[str] = frozenset({"grosh.app/user-id"})


class RateBackfillRequest(BaseModel):
    source: str
    from_date: date
    to_date: date


@router.post(
    "/rates-backfill",
    response_model=JobTriggerResponse,
    status_code=202,
)
async def trigger_rates_backfill(
    body: RateBackfillRequest,
    caller_id: Annotated[UUID, Depends(require_admin)],
    backfill_service: Annotated[BackfillService, Depends(get_backfill_service)],
) -> JobTriggerResponse:
    """Trigger a historical rate backfill K8s Job. Requires admin role."""

    if body.from_date > body.to_date:
        raise_problem(
            422, ErrorCode.INVALID_DATE_RANGE, "from_date must be <= to_date."
        )

    config = RATE_PROVIDERS.get(body.source)
    if config is None:
        raise_problem(
            422,
            ErrorCode.VALIDATION_ERROR,
            f"Unknown rate source: '{body.source}'.",
        )
    if config.fetch_historical is None:
        raise_problem(
            422,
            ErrorCode.VALIDATION_ERROR,
            f"Source '{body.source}' does not support historical rate backfill.",
        )

    job_name = backfill_service.trigger_rates_backfill(
        source=body.source,
        from_date=body.from_date.isoformat(),
        to_date=body.to_date.isoformat(),
    )
    status_url = f"/v1/admin/rates-backfill/{job_name}"
    return JobTriggerResponse(job_id=job_name, status_url=status_url)


@router.get(
    "/rates-backfill/{job_id}",
    response_model=JobStatusResponse,
)
async def get_rates_backfill_status(
    job_id: str,
    caller_id: Annotated[UUID, Depends(require_admin)],
    status_service: Annotated[JobStatusService, Depends(get_job_status_service)],
) -> JobStatusResponse:
    """Poll the status of an admin rates-backfill K8s Job.

    Verifies grosh.app/job-kind=rates_backfill and that grosh.app/user-id is absent.
    The absence check prevents per-user jobs from being visible via this endpoint.
    Returns 503 if the K8s API is unreachable.
    """

    expected_labels = {"grosh.app/job-kind": "rates_backfill"}
    try:
        result = await asyncio.to_thread(
            status_service.fetch_status,
            job_id,
            expected_labels,
            _RATES_BACKFILL_ABSENT_LABELS,
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
            f"Rates backfill job {job_id!r} not found.",
        )

    return result
