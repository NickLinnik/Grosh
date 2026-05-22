"""Stub K8s Job status service.

Slice 22 delivers the minimal interface needed by the reprocess status endpoints.
Slice 23 extends this service with the full backfill + rates-backfill variants.
"""

import logging
from datetime import UTC, datetime

import kubernetes.client
from grosh_shared.messaging.jobs import JobStatusResponse, PodCounters
from kubernetes.client.exceptions import ApiException

logger = logging.getLogger(__name__)

_KIND_MAP = {
    "reprocess": "reprocess",
    "monobank_backfill": "monobank_backfill",
    "rates_backfill": "rates_backfill",
}


class JobStatusService:
    def __init__(self, batch_api: kubernetes.client.BatchV1Api | None) -> None:
        self._batch_api = batch_api

    def find_active_job(self, label_selector: str) -> str | None:
        """Return the name of the most-recent active job matching label_selector.

        "Active" means the job has at least one active pod (not yet succeeded or
        failed). Returns None if no matching job is found or K8s is not configured.

        Raises ApiException on K8s connectivity failure.
        """
        if self._batch_api is None:
            raise ApiException(status=503, reason="K8s not configured")

        namespace = "grosh"
        job_list: kubernetes.client.V1JobList = self._batch_api.list_namespaced_job(
            namespace=namespace,
            label_selector=label_selector,
        )
        active_jobs = [
            job for job in (job_list.items or []) if (job.status.active or 0) > 0
        ]
        if not active_jobs:
            return None

        # Most-recently created job first
        active_jobs.sort(
            key=lambda j: (
                j.metadata.creation_timestamp or datetime.min.replace(tzinfo=UTC)
            ),
            reverse=True,
        )
        return active_jobs[0].metadata.name

    def fetch_status(
        self,
        job_id: str,
        expected_labels: dict[str, str],
        absent_labels: frozenset[str] | None = None,
    ) -> JobStatusResponse | None:
        """Fetch the K8s Job status and validate labels.

        Returns None if:
        - The job does not exist.
        - Any key in expected_labels is missing or has the wrong value.
        - Any key in absent_labels is present on the job (IDOR guard for admin
          bulk status: per-user jobs carry grosh.app/user-id which must be absent).

        Raises ApiException on K8s connectivity failure (caller maps to 503).
        """
        if self._batch_api is None:
            raise ApiException(status=503, reason="K8s not configured")

        namespace = "grosh"
        try:
            job: kubernetes.client.V1Job = self._batch_api.read_namespaced_job(
                name=job_id,
                namespace=namespace,
            )
        except ApiException as exc:
            if exc.status == 404:
                return None
            raise

        actual_labels: dict[str, str] = job.metadata.labels or {}
        for key, expected_value in expected_labels.items():
            if actual_labels.get(key) != expected_value:
                return None

        if absent_labels:
            for key in absent_labels:
                if key in actual_labels:
                    return None

        return _map_job_to_response(job)


def _map_job_to_response(job: kubernetes.client.V1Job) -> JobStatusResponse:
    status = job.status
    metadata = job.metadata
    labels: dict[str, str] = metadata.labels or {}

    raw_kind = labels.get("grosh.app/job-kind", "reprocess")
    kind = _KIND_MAP.get(raw_kind, "reprocess")

    started_at: datetime = metadata.creation_timestamp or datetime.now(UTC)

    completed_at: datetime | None = status.completion_time

    active = status.active or 0
    succeeded = status.succeeded or 0
    failed = status.failed or 0

    if succeeded > 0 and active == 0:
        job_status = "succeeded"
    elif failed > 0 and active == 0:
        job_status = "failed"
    elif active > 0:
        job_status = "running"
    else:
        job_status = "pending"

    failure_reason: str | None = None
    if job_status == "failed" and status.conditions:
        for condition in status.conditions:
            if condition.type == "Failed":
                failure_reason = condition.message
                break

    return JobStatusResponse(
        job_id=metadata.name,
        kind=kind,  # type: ignore[arg-type]
        status=job_status,  # type: ignore[arg-type]
        started_at=started_at,
        completed_at=completed_at,
        pods=PodCounters(active=active, succeeded=succeeded, failed=failed),
        failure_reason=failure_reason,
    )
