"""Unit tests for the Monobank webhook router.

Mocks: asyncpg connection, Kafka producer, MonobankRepo.
All tests use AsyncClient over ASGITransport — no real network or DB calls.
"""

from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from grosh_shared.models import Topic
from httpx import ASGITransport, AsyncClient

from grosh_ingestion.deps import get_db_conn, get_producer
from grosh_ingestion.main import app
from grosh_ingestion.sources.monobank.repo import (
    AccountRef,
    IntegrationRef,
    MonobankRepo,
)
from grosh_ingestion.sources.monobank.router import get_monobank_repo

# ---------------------------------------------------------------------------
# Shared mock objects (module-level so they can be reset between tests)
# ---------------------------------------------------------------------------

_mock_conn = AsyncMock()
_mock_producer = MagicMock()
_mock_repo = AsyncMock(spec=MonobankRepo)

_INTEGRATION = IntegrationRef(id=uuid4(), user_id=uuid4())
_ACCOUNT = AccountRef(id=uuid4())

_VALID_PAYLOAD: dict[str, object] = {
    "type": "StatementItem",
    "data": {
        "account": "acc-001",
        "statementItem": {
            "id": "tx-001",
            "time": 1700000000,
            "description": "Test",
            "mcc": 5411,
            "originalMcc": 5411,
            "hold": False,
            "amount": -5000,
            "operationAmount": -5000,
            "currencyCode": 980,
            "cashbackAmount": 25,
            "balance": 95000,
        },
    },
}

# ---------------------------------------------------------------------------
# Dependency overrides and cleanup
# ---------------------------------------------------------------------------

app.dependency_overrides[get_db_conn] = lambda: _mock_conn
app.dependency_overrides[get_producer] = lambda: _mock_producer
app.dependency_overrides[get_monobank_repo] = lambda: _mock_repo


@pytest.fixture(autouse=True)
def _reset_mocks() -> None:
    _mock_producer.reset_mock()
    _mock_repo.reset_mock()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _post(payload: object) -> int:
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.post("/monobank/webhook/test-secret", json=payload)
    return response.status_code


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


async def test_get_returns_200() -> None:
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get("/monobank/webhook/any-secret")

    assert response.status_code == 200


async def test_unknown_secret_returns_404_no_produce() -> None:
    _mock_repo.get_active_integration_by_webhook_secret.return_value = None

    status = await _post(_VALID_PAYLOAD)

    assert status == 404
    _mock_producer.produce.assert_not_called()


async def test_unknown_account_returns_404_no_produce() -> None:
    _mock_repo.get_active_integration_by_webhook_secret.return_value = _INTEGRATION
    _mock_repo.get_account_by_external_id.return_value = None

    status = await _post(_VALID_PAYLOAD)

    assert status == 404
    _mock_producer.produce.assert_not_called()


async def test_valid_payload_produces_to_kafka() -> None:
    _mock_repo.get_active_integration_by_webhook_secret.return_value = _INTEGRATION
    _mock_repo.get_account_by_external_id.return_value = _ACCOUNT

    status = await _post(_VALID_PAYLOAD)

    assert status == 200
    _mock_producer.produce.assert_called_once()
    call_kwargs = _mock_producer.produce.call_args[1]
    assert call_kwargs["topic"] == Topic.raw_transactions


async def test_malformed_statement_returns_422_no_produce() -> None:
    _mock_repo.get_active_integration_by_webhook_secret.return_value = _INTEGRATION
    _mock_repo.get_account_by_external_id.return_value = _ACCOUNT

    # Missing required fields: mcc, originalMcc, hold, amount, etc.
    bad_payload = {
        "type": "StatementItem",
        "data": {
            "account": "acc-001",
            "statementItem": {"id": "tx-bad"},
        },
    }

    status = await _post(bad_payload)

    assert status == 422
    _mock_producer.produce.assert_not_called()
