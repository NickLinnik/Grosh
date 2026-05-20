"""Unit tests for grosh_shared.jobs response models."""

from uuid import uuid4

import pytest

from grosh_shared.jobs import BulkReprocessResponse, SkippedUser


def test_bulk_reprocess_response_both_none_is_valid() -> None:
    response = BulkReprocessResponse(job_id=None, status_url=None, skipped=[])
    assert response.job_id is None
    assert response.status_url is None


def test_bulk_reprocess_response_both_set_is_valid() -> None:
    response = BulkReprocessResponse(
        job_id="grosh-reprocess-abc-123",
        status_url="/v1/admin/reprocess/grosh-reprocess-abc-123",
        skipped=[],
    )
    assert response.job_id == "grosh-reprocess-abc-123"
    assert response.status_url == "/v1/admin/reprocess/grosh-reprocess-abc-123"


def test_bulk_reprocess_response_job_id_without_status_url_raises() -> None:
    with pytest.raises(ValueError, match="both set or both None"):
        BulkReprocessResponse(
            job_id="grosh-reprocess-abc-123",
            status_url=None,
            skipped=[],
        )


def test_bulk_reprocess_response_status_url_without_job_id_raises() -> None:
    with pytest.raises(ValueError, match="both set or both None"):
        BulkReprocessResponse(
            job_id=None,
            status_url="/v1/admin/reprocess/grosh-reprocess-abc-123",
            skipped=[],
        )


def test_bulk_reprocess_response_with_skipped_users() -> None:
    user_id = uuid4()
    response = BulkReprocessResponse(
        job_id=None,
        status_url=None,
        skipped=[SkippedUser(user_id=user_id, reason="REPROCESS_LOCKED")],
    )
    assert len(response.skipped) == 1
    assert response.skipped[0].user_id == user_id
    assert response.skipped[0].reason == "REPROCESS_LOCKED"
