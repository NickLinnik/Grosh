"""Integration tests for backfill job status endpoints (Slice 23).

Monobank per-account backfill:
  (a) Trigger backfill → poll status → 200 with kind=monobank_backfill.
  (b) Correct job_id but wrong account_id in URL → 404 JOB_NOT_FOUND.
  (c) User A polls a job belonging to user B → 404 JOB_NOT_FOUND (IDOR defense).
  (d) K8s API unreachable → 503 JOB_STATUS_UNAVAILABLE.

Admin rates-backfill:
  (e) Trigger rates backfill → poll status → 200 with kind=rates_backfill.
  (f) Non-admin polls rates-backfill status → 403 INSUFFICIENT_PERMISSIONS.
"""

import os
from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import MagicMock
from uuid import UUID, uuid4

import asyncpg
import jwt
import pytest
import pytest_asyncio
from grosh_shared.messaging.jobs import JobStatusResponse, PodCounters
from httpx import ASGITransport, AsyncClient

from grosh_ingestion.deps import (
    get_backfill_service,
    get_db_conn,
    get_job_status_service,
)
from grosh_ingestion.main import app
from grosh_ingestion.services.backfill_service import BackfillService
from grosh_ingestion.services.job_status_service import JobStatusService

_JWT_SECRET = os.environ.get("JWT_SECRET", "super-secret-dev-jwt-key-change-in-prod")
_MOCK_JOB_NAME = "monobank-backfill-20260101-000000-abcd1234"
_MOCK_RATES_JOB_NAME = "rates-backfill-20260101-000000-abcd5678"


# ---------------------------------------------------------------------------
# Pool shim
# ---------------------------------------------------------------------------


class _SingleConnPool:
    def __init__(self, conn: asyncpg.Connection) -> None:
        self._conn = conn

    def acquire(self) -> "_SingleConnPool":
        return self

    async def __aenter__(self) -> asyncpg.Connection:
        return self._conn

    async def __aexit__(self, *_: Any) -> None:
        pass


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_token(user_id: UUID, role: str = "member") -> str:
    payload = {
        "sub": str(user_id),
        "exp": datetime.now(UTC) + timedelta(minutes=15),
    }
    return jwt.encode(payload, _JWT_SECRET, algorithm="HS256")


async def _insert_user(
    conn: asyncpg.Connection, user_id: UUID, role: str = "member"
) -> None:
    await conn.execute(
        """
        INSERT INTO users
            (id, email, password_hash, display_name, role)
        VALUES
            ($1, $2, 'x', 'Test User', $3)
        """,
        user_id,
        f"test-{user_id}@example.com",
        role,
    )


async def _insert_account(
    conn: asyncpg.Connection,
    account_id: UUID,
    user_id: UUID,
) -> None:
    """Insert a minimal monobank account row."""
    integration_id = uuid4()
    await conn.execute(
        """
        INSERT INTO bank_integrations
            (id, user_id, bank, status, config)
        VALUES
            ($1, $2, 'monobank', 'active', '{}')
        """,
        integration_id,
        user_id,
    )
    await conn.execute(
        """
        INSERT INTO accounts
            (id, user_id, integration_id, source, type, currency_code,
             external_id, is_active)
        VALUES
            ($1, $2, $3, 'monobank', 'black', 'UAH', $4, true)
        """,
        account_id,
        user_id,
        integration_id,
        f"ext-{account_id}",
    )


def _make_job_status_response(
    job_id: str,
    kind: str = "monobank_backfill",
    status: str = "running",
) -> JobStatusResponse:
    return JobStatusResponse(
        job_id=job_id,
        kind=kind,  # type: ignore[arg-type]
        status=status,  # type: ignore[arg-type]
        started_at=datetime.now(UTC),
        completed_at=None,
        pods=PodCounters(active=1, succeeded=0, failed=0),
        failure_reason=None,
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="function")
def mock_backfill_service() -> MagicMock:
    service = MagicMock(spec=BackfillService)
    service.trigger_transactions_backfill.return_value = _MOCK_JOB_NAME
    service.trigger_rates_backfill.return_value = _MOCK_RATES_JOB_NAME
    return service


@pytest.fixture(scope="function")
def mock_status_service() -> MagicMock:
    service = MagicMock(spec=JobStatusService)
    service.fetch_status.return_value = None
    return service


@pytest_asyncio.fixture(loop_scope="session", scope="function")
async def client(
    conn: asyncpg.Connection,
    mock_backfill_service: MagicMock,
    mock_status_service: MagicMock,
) -> AsyncGenerator[AsyncClient, None]:
    app.state.pool = _SingleConnPool(conn)

    async def _override_db_conn() -> AsyncGenerator[asyncpg.Connection, None]:
        async with conn.transaction():
            yield conn

    prev_db_conn = app.dependency_overrides.get(get_db_conn)
    prev_backfill = app.dependency_overrides.get(get_backfill_service)
    prev_status = app.dependency_overrides.get(get_job_status_service)

    app.dependency_overrides[get_db_conn] = _override_db_conn
    app.dependency_overrides[get_backfill_service] = lambda: mock_backfill_service
    app.dependency_overrides[get_job_status_service] = lambda: mock_status_service

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as c:
        yield c

    if prev_db_conn is None:
        app.dependency_overrides.pop(get_db_conn, None)
    else:
        app.dependency_overrides[get_db_conn] = prev_db_conn

    if prev_backfill is None:
        app.dependency_overrides.pop(get_backfill_service, None)
    else:
        app.dependency_overrides[get_backfill_service] = prev_backfill

    if prev_status is None:
        app.dependency_overrides.pop(get_job_status_service, None)
    else:
        app.dependency_overrides[get_job_status_service] = prev_status


# ---------------------------------------------------------------------------
# Case (a) — trigger + poll monobank backfill → 200 with correct kind
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_monobank_backfill_trigger_and_status(
    conn: asyncpg.Connection,
    client: AsyncClient,
    mock_backfill_service: MagicMock,
    mock_status_service: MagicMock,
) -> None:
    """Trigger backfill, then poll status → 200 with kind=monobank_backfill."""
    user_id = uuid4()
    account_id = uuid4()
    await _insert_user(conn, user_id)
    await _insert_account(conn, account_id, user_id)
    token = _make_token(user_id)

    # Trigger — explicit 7-day window to stay within the 31-day cap
    resp = await client.post(
        f"/v1/monobank/accounts/{account_id}/backfill",
        params={"from": "2025-01-01", "to": "2025-01-08"},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 202, resp.text
    body = resp.json()
    assert body["job_id"] == _MOCK_JOB_NAME
    assert (
        body["status_url"]
        == f"/v1/monobank/accounts/{account_id}/backfill/{_MOCK_JOB_NAME}"
    )

    # Wire status service to return a running job
    mock_status_service.fetch_status.return_value = _make_job_status_response(
        _MOCK_JOB_NAME, kind="monobank_backfill", status="running"
    )

    # Poll status
    resp2 = await client.get(
        f"/v1/monobank/accounts/{account_id}/backfill/{_MOCK_JOB_NAME}",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp2.status_code == 200, resp2.text
    status_body = resp2.json()
    assert status_body["job_id"] == _MOCK_JOB_NAME
    assert status_body["kind"] == "monobank_backfill"
    assert status_body["status"] in {"running", "succeeded", "pending"}

    # Verify fetch_status was called with correct labels
    call_kwargs = mock_status_service.fetch_status.call_args
    passed_labels = call_kwargs[0][1]  # second positional arg
    assert passed_labels["grosh.app/job-kind"] == "monobank_backfill"
    assert passed_labels["grosh.app/account-id"] == str(account_id)
    assert passed_labels["grosh.app/user-id"] == str(user_id)


# ---------------------------------------------------------------------------
# Case (b) — wrong account_id in URL, correct job_id → 404 JOB_NOT_FOUND
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_monobank_backfill_wrong_account_id_returns_404(
    conn: asyncpg.Connection,
    client: AsyncClient,
    mock_status_service: MagicMock,
) -> None:
    """Status poll with wrong account_id → 404 JOB_NOT_FOUND (label mismatch)."""
    user_id = uuid4()
    account_id = uuid4()
    wrong_account_id = uuid4()
    await _insert_user(conn, user_id)
    await _insert_account(conn, account_id, user_id)
    # wrong_account_id does NOT exist → get_user_id returns None → account not found
    token = _make_token(user_id)

    # Status service returns None regardless (label mismatch)
    mock_status_service.fetch_status.return_value = None

    resp = await client.get(
        f"/v1/monobank/accounts/{wrong_account_id}/backfill/{_MOCK_JOB_NAME}",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 404, resp.text
    body = resp.json()
    # wrong_account_id has no owner → account not found before even hitting K8s
    assert body["code"] in {"ACCOUNT_NOT_FOUND", "JOB_NOT_FOUND"}


# ---------------------------------------------------------------------------
# Case (c) — user A polls user B's job → 404 JOB_NOT_FOUND (IDOR defense)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_monobank_backfill_idor_returns_404(
    conn: asyncpg.Connection,
    client: AsyncClient,
    mock_status_service: MagicMock,
) -> None:
    """User A requests status for an account owned by user B → 404 (not 403)."""
    user_a = uuid4()
    user_b = uuid4()
    account_id = uuid4()
    await _insert_user(conn, user_a)
    await _insert_user(conn, user_b)
    await _insert_account(conn, account_id, user_b)
    token_a = _make_token(user_a)

    # Status service would return a result, but the auth check must reject first
    mock_status_service.fetch_status.return_value = _make_job_status_response(
        _MOCK_JOB_NAME
    )

    resp = await client.get(
        f"/v1/monobank/accounts/{account_id}/backfill/{_MOCK_JOB_NAME}",
        headers={"Authorization": f"Bearer {token_a}"},
    )
    assert resp.status_code == 404, resp.text
    body = resp.json()
    assert body["code"] == "ACCOUNT_NOT_FOUND"
    # Must NOT be 403 — that would reveal the account's existence to user A
    assert resp.status_code != 403


# ---------------------------------------------------------------------------
# Case (d) — K8s unreachable → 503 JOB_STATUS_UNAVAILABLE
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_monobank_backfill_k8s_unreachable_returns_503(
    conn: asyncpg.Connection,
    client: AsyncClient,
    mock_status_service: MagicMock,
) -> None:
    """K8s API unreachable → 503 JOB_STATUS_UNAVAILABLE."""
    from kubernetes.client.exceptions import ApiException

    user_id = uuid4()
    account_id = uuid4()
    await _insert_user(conn, user_id)
    await _insert_account(conn, account_id, user_id)
    token = _make_token(user_id)

    mock_status_service.fetch_status.side_effect = ApiException(
        status=503, reason="Service Unavailable"
    )

    resp = await client.get(
        f"/v1/monobank/accounts/{account_id}/backfill/{_MOCK_JOB_NAME}",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 503, resp.text
    body = resp.json()
    assert body["code"] == "JOB_STATUS_UNAVAILABLE"

    # Reset side effect
    mock_status_service.fetch_status.side_effect = None


# ---------------------------------------------------------------------------
# Case (e) — admin rates-backfill trigger + poll
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_admin_rates_backfill_trigger_and_status(
    conn: asyncpg.Connection,
    client: AsyncClient,
    mock_backfill_service: MagicMock,
    mock_status_service: MagicMock,
) -> None:
    """Admin triggers rates backfill, then polls status → 200 kind=rates_backfill."""
    admin_id = uuid4()
    await _insert_user(conn, admin_id, role="admin")
    token = _make_token(admin_id)

    # Trigger
    resp = await client.post(
        "/v1/admin/rates-backfill",
        json={"source": "nbu", "from_date": "2020-01-01", "to_date": "2020-01-31"},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 202, resp.text
    body = resp.json()
    assert body["job_id"] == _MOCK_RATES_JOB_NAME
    assert body["status_url"] == f"/v1/admin/rates-backfill/{_MOCK_RATES_JOB_NAME}"

    # Wire status
    mock_status_service.fetch_status.return_value = _make_job_status_response(
        _MOCK_RATES_JOB_NAME, kind="rates_backfill", status="succeeded"
    )

    # Poll
    resp2 = await client.get(
        f"/v1/admin/rates-backfill/{_MOCK_RATES_JOB_NAME}",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp2.status_code == 200, resp2.text
    status_body = resp2.json()
    assert status_body["job_id"] == _MOCK_RATES_JOB_NAME
    assert status_body["kind"] == "rates_backfill"
    assert status_body["status"] == "succeeded"

    # Verify correct labels and absent_labels passed to fetch_status
    call_args = mock_status_service.fetch_status.call_args
    passed_labels = call_args[0][1]
    absent_labels = call_args[0][2]
    assert passed_labels["grosh.app/job-kind"] == "rates_backfill"
    assert "grosh.app/user-id" in absent_labels


# ---------------------------------------------------------------------------
# Case (f) — non-admin polls rates-backfill status → 403
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_rates_backfill_status_non_admin_forbidden(
    conn: asyncpg.Connection,
    client: AsyncClient,
) -> None:
    """Non-admin caller on GET /v1/admin/rates-backfill/{job_id} → 403."""
    member_id = uuid4()
    await _insert_user(conn, member_id, role="member")
    token = _make_token(member_id)

    resp = await client.get(
        f"/v1/admin/rates-backfill/{_MOCK_RATES_JOB_NAME}",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 403, resp.text
    body = resp.json()
    assert body["code"] == "INSUFFICIENT_PERMISSIONS"
