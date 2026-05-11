"""Unit tests for ReprocessDispatcher."""

import json
import re
from unittest.mock import MagicMock
from uuid import UUID

import pytest
from kubernetes.client.exceptions import ApiException

from grosh_ingestion.errors import K8sDispatchError
from grosh_ingestion.services.reprocess_dispatcher import ReprocessDispatcher

USER_ID = UUID("12345678-1234-5678-1234-567812345678")


def _make_batch_api() -> MagicMock:
    api = MagicMock()
    api.create_namespaced_job = MagicMock()
    return api


# (a) Happy path — job created, correct call args, correct return value
def test_trigger_reprocess_happy_path(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("REPROCESS_IMAGE", "grosh-consumer:latest")
    monkeypatch.delenv("K8S_NAMESPACE", raising=False)

    mock_api = _make_batch_api()
    dispatcher = ReprocessDispatcher(batch_api=mock_api)
    job_name = dispatcher.trigger_reprocess(USER_ID)

    mock_api.create_namespaced_job.assert_called_once()
    call_kwargs = mock_api.create_namespaced_job.call_args.kwargs
    assert call_kwargs["namespace"] == "grosh"

    body = call_kwargs["body"]
    assert re.match(r"^grosh-reprocess-[0-9a-f]{8}-[0-9]+$", body.metadata.name)

    container = body.spec.template.spec.containers[0]
    assert container.command == ["python", "-m", "grosh_consumer.jobs.run_reprocess"]

    env_map = {e.name: e.value for e in container.env}
    assert env_map["USER_IDS_JSON"] == json.dumps([str(USER_ID)])

    assert body.spec.active_deadline_seconds == 7200

    assert container.resources.requests["cpu"] == "100m"
    assert container.resources.requests["memory"] == "256Mi"
    assert container.resources.limits["cpu"] == "500m"
    assert container.resources.limits["memory"] == "512Mi"

    # (c) Returned name matches what was sent in the body
    assert job_name == body.metadata.name


# (b) ApiException is wrapped as K8sDispatchError
def test_trigger_reprocess_api_exception(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("REPROCESS_IMAGE", "grosh-consumer:latest")

    mock_api = _make_batch_api()
    mock_api.create_namespaced_job.side_effect = ApiException("kaboom")
    dispatcher = ReprocessDispatcher(batch_api=mock_api)

    with pytest.raises(K8sDispatchError) as exc_info:
        dispatcher.trigger_reprocess(USER_ID)

    msg = str(exc_info.value)
    assert "Cannot create reprocess job" in msg
    assert "kaboom" in msg


# (d) None batch_api raises K8sDispatchError
def test_trigger_reprocess_no_k8s(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("REPROCESS_IMAGE", "grosh-consumer:latest")

    dispatcher = ReprocessDispatcher(batch_api=None)

    with pytest.raises(
        K8sDispatchError, match="Cannot create reprocess job — K8s not configured"
    ):
        dispatcher.trigger_reprocess(USER_ID)


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
