"""Unit tests for ReprocessDispatcher.submit()."""

import json
import re
from unittest.mock import MagicMock
from uuid import UUID

import pytest
from kubernetes.client.exceptions import ApiException

from grosh_ingestion.errors import K8sDispatchError
from grosh_ingestion.services.reprocess_dispatcher import ReprocessDispatcher

USER_ID = UUID("12345678-1234-5678-1234-567812345678")
ADMIN_USER_ID = UUID("aaaabbbb-cccc-dddd-eeee-ffffaaaabbbb")


def _make_batch_api() -> MagicMock:
    api = MagicMock()
    api.create_namespaced_job = MagicMock()
    return api


# (a) Per-user happy path — job created with user-id label
def test_submit_per_user_happy_path(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("REPROCESS_IMAGE", "grosh-consumer:latest")
    monkeypatch.delenv("K8S_NAMESPACE", raising=False)

    mock_api = _make_batch_api()
    dispatcher = ReprocessDispatcher(batch_api=mock_api)
    job_name = dispatcher.submit(user_ids=[USER_ID], caller_user_id=USER_ID)

    mock_api.create_namespaced_job.assert_called_once()
    call_kwargs = mock_api.create_namespaced_job.call_args.kwargs
    assert call_kwargs["namespace"] == "grosh"

    body = call_kwargs["body"]
    assert re.match(r"^grosh-reprocess-[0-9a-f]{8}-\d+$", body.metadata.name)

    labels = body.metadata.labels
    assert labels["app.kubernetes.io/managed-by"] == "grosh-ingestion"
    assert labels["grosh.app/job-kind"] == "reprocess"
    assert labels["grosh.app/user-id"] == str(USER_ID)

    container = body.spec.template.spec.containers[0]
    assert container.command == ["python", "-m", "grosh_normalizer.reprocess_main"]

    env_map = {e.name: e.value for e in container.env}
    assert env_map["USER_IDS_JSON"] == json.dumps([str(USER_ID)])

    assert body.spec.active_deadline_seconds == 7200
    assert body.spec.ttl_seconds_after_finished == 3600

    assert container.resources.requests["cpu"] == "100m"
    assert container.resources.requests["memory"] == "256Mi"
    assert container.resources.limits["cpu"] == "500m"
    assert container.resources.limits["memory"] == "512Mi"

    assert job_name == body.metadata.name


# (b) Admin bulk — no user-id label, "admin" short name
def test_submit_admin_bulk_no_user_label(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("REPROCESS_IMAGE", "grosh-consumer:latest")

    mock_api = _make_batch_api()
    dispatcher = ReprocessDispatcher(batch_api=mock_api)
    job_name = dispatcher.submit(user_ids=[USER_ID, ADMIN_USER_ID], caller_user_id=None)

    body = mock_api.create_namespaced_job.call_args.kwargs["body"]
    labels = body.metadata.labels
    assert "grosh.app/user-id" not in labels
    assert labels["grosh.app/job-kind"] == "reprocess"
    assert "admin" in body.metadata.name

    env_map = {e.name: e.value for e in body.spec.template.spec.containers[0].env}
    submitted = json.loads(env_map["USER_IDS_JSON"])
    assert set(submitted) == {str(USER_ID), str(ADMIN_USER_ID)}

    assert job_name == body.metadata.name


# (c) ApiException is wrapped as K8sDispatchError
def test_submit_api_exception(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("REPROCESS_IMAGE", "grosh-consumer:latest")

    mock_api = _make_batch_api()
    mock_api.create_namespaced_job.side_effect = ApiException("kaboom")
    dispatcher = ReprocessDispatcher(batch_api=mock_api)

    with pytest.raises(K8sDispatchError) as exc_info:
        dispatcher.submit(user_ids=[USER_ID], caller_user_id=USER_ID)

    assert "Cannot create reprocess job" in str(exc_info.value)
    assert "kaboom" in str(exc_info.value)


# (d) None batch_api raises K8sDispatchError
def test_submit_no_k8s(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("REPROCESS_IMAGE", "grosh-consumer:latest")

    dispatcher = ReprocessDispatcher(batch_api=None)

    with pytest.raises(
        K8sDispatchError, match="Cannot create reprocess job — K8s not configured"
    ):
        dispatcher.submit(user_ids=[USER_ID], caller_user_id=USER_ID)


# (e) Missing REPROCESS_IMAGE raises K8sDispatchError on construction
def test_construction_fails_without_reprocess_image(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("REPROCESS_IMAGE", raising=False)

    with pytest.raises(K8sDispatchError, match="REPROCESS_IMAGE"):
        ReprocessDispatcher(batch_api=None)


# (f) Empty REPROCESS_IMAGE also raises K8sDispatchError on construction
def test_construction_fails_with_empty_reprocess_image(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("REPROCESS_IMAGE", "")
    with pytest.raises(K8sDispatchError, match="REPROCESS_IMAGE"):
        ReprocessDispatcher(batch_api=None)
