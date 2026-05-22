"""Shared response models for K8s Job trigger and status endpoints.

Used by the ingestion service for reprocess, backfill, and rates-backfill
job endpoints. Defined in grosh_shared so future services can import them
without introducing a cross-service dependency.
"""

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, model_validator


class PodCounters(BaseModel):
    model_config = ConfigDict(frozen=True)

    active: int
    succeeded: int
    failed: int


class JobTriggerResponse(BaseModel):
    model_config = ConfigDict(frozen=True)

    job_id: str
    status_url: str


class JobStatusResponse(BaseModel):
    model_config = ConfigDict(frozen=True)

    job_id: str
    kind: Literal["monobank_backfill", "rates_backfill", "reprocess"]
    status: Literal["pending", "running", "succeeded", "failed"]
    started_at: datetime
    completed_at: datetime | None
    pods: PodCounters
    failure_reason: str | None


class SkippedUser(BaseModel):
    model_config = ConfigDict(frozen=True)

    user_id: UUID
    reason: Literal["REPROCESS_LOCKED", "RATE_LIMITED"]


class BulkReprocessResponse(BaseModel):
    """Response for POST /v1/admin/reprocess.

    Invariant: job_id and status_url are either both present or both absent.
    Both absent means all target users were already locked (nothing was submitted).
    """

    job_id: str | None
    status_url: str | None
    skipped: list[SkippedUser]

    @model_validator(mode="after")
    def _job_id_and_status_url_must_be_co_present(self) -> "BulkReprocessResponse":
        if (self.job_id is None) != (self.status_url is None):
            raise ValueError(
                "job_id and status_url must be either both set or both None"
            )
        return self
