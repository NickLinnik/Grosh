"""Unit tests for JobStatusService and _map_job_to_response.

Tests the pure K8s V1Job → JobStatusResponse transformation. V1Job objects are
hand-built using the kubernetes.client model constructors — no mocking of the
function under test, only the BatchV1Api dependency.
"""

from datetime import UTC, datetime
from unittest.mock import MagicMock

import kubernetes.client
import pytest
from kubernetes.client.exceptions import ApiException

from grosh_ingestion.services.job_status_service import (
    JobStatusService,
    _map_job_to_response,
)

# ---------------------------------------------------------------------------
# Helpers — build minimal V1Job objects covering each status branch
# ---------------------------------------------------------------------------


def _make_job(
    *,
    name: str = "test-job-abc",
    labels: dict[str, str] | None = None,
    active: int = 0,
    succeeded: int = 0,
    failed: int = 0,
    completion_time: datetime | None = None,
    conditions: list[kubernetes.client.V1JobCondition] | None = None,
) -> kubernetes.client.V1Job:
    """Construct a V1Job with the given status fields."""
    return kubernetes.client.V1Job(
        metadata=kubernetes.client.V1ObjectMeta(
            name=name,
            labels=labels or {"grosh.app/job-kind": "reprocess"},
            creation_timestamp=datetime(2026, 1, 1, tzinfo=UTC),
        ),
        status=kubernetes.client.V1JobStatus(
            active=active or None,
            succeeded=succeeded or None,
            failed=failed or None,
            completion_time=completion_time,
            conditions=conditions,
        ),
    )


# ---------------------------------------------------------------------------
# _map_job_to_response — pure transformation, no K8s API call
# ---------------------------------------------------------------------------


def test_map_job_active_status() -> None:
    """active > 0 and succeeded == 0 → status='running'.

    Wrong status label causes the UI to show 'succeeded' for a still-running job,
    blocking re-trigger or misleading the user.
    """
    job = _make_job(active=1, succeeded=0, failed=0)

    result = _map_job_to_response(job)

    assert result.status == "running"
    assert result.pods.active == 1
    assert result.pods.succeeded == 0
    assert result.failure_reason is None


def test_map_job_succeeded_status() -> None:
    """succeeded > 0 and active == 0 → status='succeeded'."""
    completed = datetime(2026, 1, 2, tzinfo=UTC)
    job = _make_job(succeeded=1, active=0, completion_time=completed)

    result = _map_job_to_response(job)

    assert result.status == "succeeded"
    assert result.completed_at == completed
    assert result.pods.succeeded == 1


def test_map_job_failed_status_with_condition() -> None:
    """failed > 0 and active == 0 → status='failed'; condition msg in failure_reason.

    Without this, callers see status='failed' but no explanation — users can't
    diagnose whether it was a timeout, OOM, or application error.
    """
    condition = kubernetes.client.V1JobCondition(
        type="Failed",
        status="True",
        message="BackoffLimitExceeded",
    )
    job = _make_job(failed=3, active=0, conditions=[condition])

    result = _map_job_to_response(job)

    assert result.status == "failed"
    assert result.failure_reason == "BackoffLimitExceeded"
    assert result.pods.failed == 3


def test_map_job_pending_status() -> None:
    """No active, succeeded, or failed pods yet → status='pending'.

    Pending is the initial state before any pod is scheduled; wrong mapping
    would return 'failed' (the else-branch default) making the job appear broken.
    """
    job = _make_job(active=0, succeeded=0, failed=0)

    result = _map_job_to_response(job)

    assert result.status == "pending"
    assert result.failure_reason is None


def test_map_job_kind_from_label() -> None:
    """job-kind label is mapped to the kind field in the response.

    Wrong kind causes the frontend to show the wrong job type in the status
    view, e.g. displaying 'reprocess' for a backfill job.
    """
    job = _make_job(
        labels={"grosh.app/job-kind": "monobank_backfill"},
        succeeded=1,
    )

    result = _map_job_to_response(job)

    assert result.kind == "monobank_backfill"


def test_map_job_unknown_kind_falls_back_to_reprocess() -> None:
    """Unknown job-kind label falls back to 'reprocess' — does not raise."""
    job = _make_job(labels={"grosh.app/job-kind": "totally_unknown_kind"})

    result = _map_job_to_response(job)

    assert result.kind == "reprocess"


def test_map_job_missing_job_kind_label_falls_back_to_reprocess() -> None:
    """No job-kind label at all falls back to 'reprocess' — does not raise."""
    job = _make_job(labels={})

    result = _map_job_to_response(job)

    assert result.kind == "reprocess"


# ---------------------------------------------------------------------------
# JobStatusService.fetch_status — label guard (IDOR protection)
# ---------------------------------------------------------------------------


def test_fetch_status_returns_none_for_missing_expected_label() -> None:
    """fetch_status returns None when a required label is absent or wrong.

    This is the IDOR guard: user A must not see user B's job by guessing the
    job_id. If label mismatch returned a result, any job_id would leak status.
    """
    job = _make_job(
        name="some-job",
        labels={
            "grosh.app/job-kind": "monobank_backfill",
            "grosh.app/user-id": "user-b",
        },
        active=1,
    )
    mock_api = MagicMock()
    mock_api.read_namespaced_job.return_value = job

    svc = JobStatusService(batch_api=mock_api)
    result = svc.fetch_status(
        job_id="some-job",
        expected_labels={"grosh.app/user-id": "user-a"},  # wrong user
    )

    assert result is None


def test_fetch_status_returns_none_for_absent_label_violation() -> None:
    """fetch_status returns None when an absent_labels key is present on the job.

    The admin rates-backfill status endpoint uses absent_labels to ensure the
    job is not per-user. Without this check an admin could accidentally see a
    user's personal backfill job via the admin endpoint.
    """
    job = _make_job(
        name="some-job",
        labels={
            "grosh.app/job-kind": "rates_backfill",
            "grosh.app/user-id": "user-x",  # must be absent for admin endpoint
        },
        succeeded=1,
    )
    mock_api = MagicMock()
    mock_api.read_namespaced_job.return_value = job

    svc = JobStatusService(batch_api=mock_api)
    result = svc.fetch_status(
        job_id="some-job",
        expected_labels={"grosh.app/job-kind": "rates_backfill"},
        absent_labels=frozenset({"grosh.app/user-id"}),
    )

    assert result is None


def test_fetch_status_returns_none_on_404() -> None:
    """K8s 404 on read_namespaced_job returns None instead of raising.

    If 404 raised instead, the router would return 500 for any expired or
    non-existent job_id.
    """
    mock_api = MagicMock()
    mock_api.read_namespaced_job.side_effect = ApiException(
        status=404, reason="Not Found"
    )

    svc = JobStatusService(batch_api=mock_api)
    result = svc.fetch_status(
        job_id="gone-job",
        expected_labels={"grosh.app/job-kind": "reprocess"},
    )

    assert result is None


def test_fetch_status_reraises_non_404_api_exception() -> None:
    """Non-404 ApiException (e.g., 503) is re-raised so the caller maps it to 503.

    Swallowing connectivity errors would make a K8s-down situation look like
    'job not found', preventing operators from diagnosing the real issue.
    """
    mock_api = MagicMock()
    mock_api.read_namespaced_job.side_effect = ApiException(
        status=503, reason="Service Unavailable"
    )

    svc = JobStatusService(batch_api=mock_api)

    with pytest.raises(ApiException) as exc_info:
        svc.fetch_status(
            job_id="any-job",
            expected_labels={"grosh.app/job-kind": "reprocess"},
        )

    assert exc_info.value.status == 503


def test_fetch_status_raises_api_exception_when_no_k8s() -> None:
    """batch_api=None raises ApiException(503) — caller maps to 503 response."""
    svc = JobStatusService(batch_api=None)

    with pytest.raises(ApiException) as exc_info:
        svc.fetch_status(
            job_id="any-job",
            expected_labels={"grosh.app/job-kind": "reprocess"},
        )

    assert exc_info.value.status == 503


# ---------------------------------------------------------------------------
# find_active_job — label-filtered, sorted-most-recent-first
# ---------------------------------------------------------------------------


def test_find_active_job_returns_name_when_active_exists() -> None:
    """active > 0 → job name returned; the K8s label filter is the caller's contract."""
    job = _make_job(name="reprocess-active", active=1)
    job_list = kubernetes.client.V1JobList(items=[job])
    mock_api = MagicMock()
    mock_api.list_namespaced_job.return_value = job_list

    svc = JobStatusService(batch_api=mock_api)

    result = svc.find_active_job(label_selector="grosh.app/job-kind=reprocess")

    assert result == "reprocess-active"
    mock_api.list_namespaced_job.assert_called_once_with(
        namespace="grosh",
        label_selector="grosh.app/job-kind=reprocess",
    )


def test_find_active_job_returns_none_when_no_jobs() -> None:
    """Empty job list → None; missing return None would surface a stale active job."""
    job_list = kubernetes.client.V1JobList(items=[])
    mock_api = MagicMock()
    mock_api.list_namespaced_job.return_value = job_list

    svc = JobStatusService(batch_api=mock_api)

    assert svc.find_active_job(label_selector="any=value") is None


def test_find_active_job_returns_none_when_all_jobs_inactive() -> None:
    """succeeded-only / failed-only jobs are not "active" — filter excludes them.

    A regression that drops the `active > 0` filter would surface a finished
    job as still-running, blocking new submissions indefinitely.
    """
    finished = _make_job(name="finished", active=0, succeeded=1)
    failed = _make_job(name="failed", active=0, failed=1)
    job_list = kubernetes.client.V1JobList(items=[finished, failed])
    mock_api = MagicMock()
    mock_api.list_namespaced_job.return_value = job_list

    svc = JobStatusService(batch_api=mock_api)

    assert svc.find_active_job(label_selector="any=value") is None


def test_find_active_job_returns_most_recent_when_multiple_active() -> None:
    """When multiple active jobs match, the one with the latest creation timestamp wins.

    Picking the oldest would re-attach a status poll to a stale job after
    a new submission landed.
    """
    older = kubernetes.client.V1Job(
        metadata=kubernetes.client.V1ObjectMeta(
            name="older",
            labels={"grosh.app/job-kind": "reprocess"},
            creation_timestamp=datetime(2026, 1, 1, tzinfo=UTC),
        ),
        status=kubernetes.client.V1JobStatus(active=1),
    )
    newer = kubernetes.client.V1Job(
        metadata=kubernetes.client.V1ObjectMeta(
            name="newer",
            labels={"grosh.app/job-kind": "reprocess"},
            creation_timestamp=datetime(2026, 5, 1, tzinfo=UTC),
        ),
        status=kubernetes.client.V1JobStatus(active=1),
    )
    job_list = kubernetes.client.V1JobList(items=[older, newer])
    mock_api = MagicMock()
    mock_api.list_namespaced_job.return_value = job_list

    svc = JobStatusService(batch_api=mock_api)

    assert svc.find_active_job(label_selector="any=value") == "newer"


def test_find_active_job_handles_missing_creation_timestamp() -> None:
    """A job with no creation_timestamp does not crash the sort.

    K8s should always set it, but a defensive `datetime.min` fallback keeps
    the call safe if the field is ever absent.
    """
    no_ts = kubernetes.client.V1Job(
        metadata=kubernetes.client.V1ObjectMeta(
            name="no-ts",
            labels={"grosh.app/job-kind": "reprocess"},
            creation_timestamp=None,
        ),
        status=kubernetes.client.V1JobStatus(active=1),
    )
    job_list = kubernetes.client.V1JobList(items=[no_ts])
    mock_api = MagicMock()
    mock_api.list_namespaced_job.return_value = job_list

    svc = JobStatusService(batch_api=mock_api)

    assert svc.find_active_job(label_selector="any=value") == "no-ts"


def test_find_active_job_raises_api_exception_when_no_k8s() -> None:
    """batch_api=None raises ApiException(503) just like fetch_status."""
    svc = JobStatusService(batch_api=None)

    with pytest.raises(ApiException) as exc_info:
        svc.find_active_job(label_selector="any=value")

    assert exc_info.value.status == 503
