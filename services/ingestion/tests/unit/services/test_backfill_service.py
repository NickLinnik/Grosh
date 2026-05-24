"""Unit tests for BackfillService — K8s Job manifest construction and error handling.

Uses a real fake BatchV1Api (not MagicMock) that records create_namespaced_job calls
and can simulate ApiException. The factory load_batch_api() is bypassed by injecting
the fake directly into the constructor.
"""

from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

import pytest
from kubernetes.client.exceptions import ApiException

from grosh_ingestion.errors import BackfillAlreadyRunningError
from grosh_ingestion.services.backfill_service import BackfillService

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

USER_ID = UUID("11111111-2222-3333-4444-555555555555")
ACCOUNT_ID = UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
INTEGRATION_ID = UUID("12345678-1234-5678-1234-567812345678")
ACCOUNT_EXTERNAL_ID = "mono-ext-abc"


@dataclass
class _FakeJobList:
    items: list[Any] = field(default_factory=list)


@dataclass
class _FakeJobStatus:
    completion_time: object = None
    failed: int = 0


@dataclass
class _FakeJobSpec:
    backoff_limit: int = 1


@dataclass
class _FakeJob:
    status: _FakeJobStatus = field(default_factory=_FakeJobStatus)
    spec: _FakeJobSpec = field(default_factory=_FakeJobSpec)


class FakeBatchApi:
    """Real fake that records create_namespaced_job calls.

    Does not use MagicMock — uses real state so tests assert actual behaviour,
    not call sequences that could pass even if logic is wrong.
    """

    def __init__(
        self,
        list_response: _FakeJobList | None = None,
        create_side_effect: Exception | None = None,
    ) -> None:
        self._list_response = list_response or _FakeJobList()
        self._create_side_effect = create_side_effect
        self.created_jobs: list[tuple[str, Any]] = []  # (namespace, body)

    def list_namespaced_job(
        self, *, namespace: str, label_selector: str
    ) -> _FakeJobList:
        return self._list_response

    def create_namespaced_job(self, *, namespace: str, body: Any) -> None:
        if self._create_side_effect is not None:
            raise self._create_side_effect
        self.created_jobs.append((namespace, body))


# ---------------------------------------------------------------------------
# BackfillService.trigger_transactions_backfill
# ---------------------------------------------------------------------------


def test_transactions_backfill_job_name_format(monkeypatch: pytest.MonkeyPatch) -> None:
    """Job name follows expected prefix — wrong prefix breaks K8s label selector."""
    monkeypatch.setenv("INGESTION_IMAGE", "grosh-ingestion:test")
    monkeypatch.setenv("K8S_NAMESPACE", "grosh")
    fake = FakeBatchApi()
    svc = BackfillService(batch_api=fake)

    name = svc.trigger_transactions_backfill(
        account_id=ACCOUNT_ID,
        integration_id=INTEGRATION_ID,
        user_id=USER_ID,
        account_external_id=ACCOUNT_EXTERNAL_ID,
        from_timestamp=1_700_000_000,
        to_timestamp=1_702_678_400,
    )

    assert name.startswith("monobank-backfill-"), f"Unexpected job name: {name!r}"
    assert len(fake.created_jobs) == 1


def test_transactions_backfill_labels(monkeypatch: pytest.MonkeyPatch) -> None:
    """Job carries grosh.app/job-kind and grosh.app/user-id labels.

    Missing labels break the status-poll endpoint's IDOR guard and the
    _has_active_job selector — wrong labels silently allow duplicate jobs.
    """
    monkeypatch.setenv("INGESTION_IMAGE", "grosh-ingestion:test")
    fake = FakeBatchApi()
    svc = BackfillService(batch_api=fake)

    svc.trigger_transactions_backfill(
        account_id=ACCOUNT_ID,
        integration_id=INTEGRATION_ID,
        user_id=USER_ID,
        account_external_id=ACCOUNT_EXTERNAL_ID,
        from_timestamp=1_700_000_000,
        to_timestamp=1_702_678_400,
    )

    _, body = fake.created_jobs[0]
    labels = body.metadata.labels
    assert labels["grosh.app/job-kind"] == "monobank_backfill"
    assert labels["grosh.app/user-id"] == str(USER_ID)
    assert labels["grosh.app/account-id"] == str(ACCOUNT_ID)
    assert labels["app.kubernetes.io/managed-by"] == "grosh-ingestion"


def test_transactions_backfill_env_vars(monkeypatch: pytest.MonkeyPatch) -> None:
    """Job env vars carry correct timestamps and IDs.

    Wrong env var values cause the backfill Job pod to fetch the wrong date range
    — silent data corruption that only surfaces when checking transaction history.
    """
    monkeypatch.setenv("INGESTION_IMAGE", "grosh-ingestion:test")
    fake = FakeBatchApi()
    svc = BackfillService(batch_api=fake)

    svc.trigger_transactions_backfill(
        account_id=ACCOUNT_ID,
        integration_id=INTEGRATION_ID,
        user_id=USER_ID,
        account_external_id=ACCOUNT_EXTERNAL_ID,
        from_timestamp=1_700_000_000,
        to_timestamp=1_702_678_400,
    )

    _, body = fake.created_jobs[0]
    container = body.spec.template.spec.containers[0]
    env_map = {e.name: e.value for e in container.env}

    assert env_map["BACKFILL_INTEGRATION_ID"] == str(INTEGRATION_ID)
    assert env_map["BACKFILL_USER_ID"] == str(USER_ID)
    assert env_map["BACKFILL_ACCOUNT_EXTERNAL_ID"] == ACCOUNT_EXTERNAL_ID
    assert env_map["BACKFILL_FROM_TIMESTAMP"] == "1700000000"
    assert env_map["BACKFILL_TO_TIMESTAMP"] == "1702678400"


def test_transactions_backfill_command_and_image(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Job runs the correct Python module with the injected image name.

    Wrong command → Job pod exits doing nothing; wrong image → ImagePullBackOff.
    """
    monkeypatch.setenv("INGESTION_IMAGE", "grosh-ingestion:v42")
    fake = FakeBatchApi()
    svc = BackfillService(batch_api=fake)

    svc.trigger_transactions_backfill(
        account_id=ACCOUNT_ID,
        integration_id=INTEGRATION_ID,
        user_id=USER_ID,
        account_external_id=ACCOUNT_EXTERNAL_ID,
        from_timestamp=1_700_000_000,
        to_timestamp=1_702_678_400,
    )

    _, body = fake.created_jobs[0]
    container = body.spec.template.spec.containers[0]
    assert container.image == "grosh-ingestion:v42"
    assert container.command == [
        "python",
        "-m",
        "grosh_ingestion.jobs.run_transactions_backfill",
    ]


def test_transactions_backfill_raises_when_active_job_exists(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Second trigger while a job is active raises BackfillAlreadyRunningError.

    Without this guard two concurrent backfill jobs overlap, causing duplicate
    transactions in the pipeline.
    """
    monkeypatch.setenv("INGESTION_IMAGE", "grosh-ingestion:test")
    # Simulate one active (non-completed, non-exhausted) job
    active_job = _FakeJob(
        status=_FakeJobStatus(completion_time=None, failed=0),
        spec=_FakeJobSpec(backoff_limit=3),
    )
    fake = FakeBatchApi(list_response=_FakeJobList(items=[active_job]))
    svc = BackfillService(batch_api=fake)

    with pytest.raises(BackfillAlreadyRunningError):
        svc.trigger_transactions_backfill(
            account_id=ACCOUNT_ID,
            integration_id=INTEGRATION_ID,
            user_id=USER_ID,
            account_external_id=ACCOUNT_EXTERNAL_ID,
            from_timestamp=1_700_000_000,
            to_timestamp=1_702_678_400,
        )


def test_transactions_backfill_api_exception_propagates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """ApiException from K8s bubbles up so the router returns 503 instead of swallowing.

    Silently swallowing would make the user think a job started when it didn't.
    """
    monkeypatch.setenv("INGESTION_IMAGE", "grosh-ingestion:test")
    fake = FakeBatchApi(create_side_effect=ApiException(status=403, reason="Forbidden"))
    svc = BackfillService(batch_api=fake)

    with pytest.raises(ApiException):
        svc.trigger_transactions_backfill(
            account_id=ACCOUNT_ID,
            integration_id=INTEGRATION_ID,
            user_id=USER_ID,
            account_external_id=ACCOUNT_EXTERNAL_ID,
            from_timestamp=1_700_000_000,
            to_timestamp=1_702_678_400,
        )


def test_transactions_backfill_raises_runtime_error_without_k8s() -> None:
    """batch_api=None raises RuntimeError instead of silently producing nothing."""
    svc = BackfillService(batch_api=None)

    with pytest.raises(RuntimeError, match="K8s not configured"):
        svc.trigger_transactions_backfill(
            account_id=ACCOUNT_ID,
            integration_id=INTEGRATION_ID,
            user_id=USER_ID,
            account_external_id=ACCOUNT_EXTERNAL_ID,
            from_timestamp=1_700_000_000,
            to_timestamp=1_702_678_400,
        )


# ---------------------------------------------------------------------------
# BackfillService.trigger_rates_backfill
# ---------------------------------------------------------------------------


def test_rates_backfill_labels_and_command(monkeypatch: pytest.MonkeyPatch) -> None:
    """Rates-backfill Job has correct job-kind label and module command.

    Wrong job-kind would make the status-poll IDOR guard miss it; wrong command
    runs nothing.
    """
    monkeypatch.setenv("INGESTION_IMAGE", "grosh-ingestion:test")
    fake = FakeBatchApi()
    svc = BackfillService(batch_api=fake)

    name = svc.trigger_rates_backfill(
        source="nbu", from_date="2025-01-01", to_date="2025-01-31"
    )

    assert name.startswith("rates-backfill-")
    assert len(fake.created_jobs) == 1
    _, body = fake.created_jobs[0]

    labels = body.metadata.labels
    assert labels["grosh.app/job-kind"] == "rates_backfill"
    assert labels["grosh.app/source"] == "nbu"

    container = body.spec.template.spec.containers[0]
    assert container.command == [
        "python",
        "-m",
        "grosh_ingestion.jobs.run_rates_backfill",
    ]
    env_map = {e.name: e.value for e in container.env}
    assert env_map["BACKFILL_SOURCE"] == "nbu"
    assert env_map["BACKFILL_FROM_DATE"] == "2025-01-01"
    assert env_map["BACKFILL_TO_DATE"] == "2025-01-31"


def test_rates_backfill_raises_when_active(monkeypatch: pytest.MonkeyPatch) -> None:
    """Duplicate rates-backfill trigger raises BackfillAlreadyRunningError.

    Two concurrent rates-backfill jobs would overwrite each other's rate rows
    non-atomically, producing corrupt valid_from/valid_to SCD2 windows.
    """
    monkeypatch.setenv("INGESTION_IMAGE", "grosh-ingestion:test")
    active = _FakeJob(status=_FakeJobStatus(completion_time=None, failed=0))
    fake = FakeBatchApi(list_response=_FakeJobList(items=[active]))
    svc = BackfillService(batch_api=fake)

    with pytest.raises(BackfillAlreadyRunningError, match="nbu"):
        svc.trigger_rates_backfill(
            source="nbu", from_date="2025-01-01", to_date="2025-01-31"
        )
