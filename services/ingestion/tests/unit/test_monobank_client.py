"""Unit tests for MonobankClient.

The client holds a single httpx.AsyncClient as self._client.
We patch httpx.AsyncClient at the module level so the constructor
receives our mock, then control get/post responses via AsyncMock.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from grosh_shared.iso_4217 import numeric_to_alpha

from grosh_ingestion.sources.monobank.client import MonobankAPIError, MonobankClient
from grosh_ingestion.sources.monobank.models import (
    MonobankClientInfo,
    MonobankStatementItem,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_CLIENT_INFO_JSON = {
    "clientId": "cid-001",
    "name": "John Doe",
    "accounts": [
        {
            "id": "acc-001",
            "sendId": "send-001",
            "balance": 100000,
            "creditLimit": 0,
            "type": "black",
            "currencyCode": 980,
            "cashbackType": "UAH",
            "maskedPan": ["537541******1234"],
            "iban": "UA123456789012345678901234567",
        }
    ],
}

_STATEMENT_ITEM_JSON = {
    "id": "stmt-001",
    "time": 1_700_000_000,
    "description": "ATB Market",
    "mcc": 5411,
    "originalMcc": 5411,
    "hold": False,
    "amount": -5000,
    "operationAmount": -5000,
    "currencyCode": 980,
    "cashbackAmount": 25,
    "balance": 95000,
    "comment": None,
    "receiptId": None,
    "invoiceId": None,
    "counterEdrpou": None,
    "counterIban": None,
    "counterName": None,
}


def _make_mock_response(json_data: object, status_code: int = 200) -> MagicMock:
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.is_success = 200 <= status_code < 300
    resp.json.return_value = json_data
    resp.text = ""
    return resp


def _make_mock_http_client(response: MagicMock) -> MagicMock:
    """Return a mock that stands in for the persistent httpx.AsyncClient instance."""
    mock = MagicMock()
    mock.get = AsyncMock(return_value=response)
    mock.post = AsyncMock(return_value=response)
    mock.aclose = AsyncMock()
    return mock


def _patch_async_client(response: MagicMock):
    """Patch httpx.AsyncClient so MonobankClient.__init__ receives our mock."""
    mock_http = _make_mock_http_client(response)
    return patch(
        "grosh_ingestion.sources.monobank.client.httpx.AsyncClient",
        return_value=mock_http,
    ), mock_http


# ---------------------------------------------------------------------------
# Tests — public methods
# ---------------------------------------------------------------------------


async def test_get_client_info_returns_parsed_model() -> None:
    resp = _make_mock_response(_CLIENT_INFO_JSON)
    ctx, mock_http = _patch_async_client(resp)

    with ctx:
        client = MonobankClient(token="test-token")
        info = await client.get_client_info()

    assert isinstance(info, MonobankClientInfo)
    assert info.client_id == "cid-001"
    assert info.name == "John Doe"
    assert len(info.accounts) == 1
    account = info.accounts[0]
    assert account.id == "acc-001"
    assert account.currency_code == 980
    assert account.cashback_type == "UAH"
    assert account.masked_pan == ["537541******1234"]

    mock_http.get.assert_awaited_once_with("/personal/client-info")


async def test_get_statements_returns_parsed_list() -> None:
    resp = _make_mock_response([_STATEMENT_ITEM_JSON])
    ctx, mock_http = _patch_async_client(resp)

    with ctx:
        client = MonobankClient(token="test-token")
        items = await client.get_statements(
            account_id="acc-001",
            from_ts=1_700_000_000,
            to_ts=1_700_086_400,
        )

    assert len(items) == 1
    item = items[0]
    assert isinstance(item, MonobankStatementItem)
    assert item.id == "stmt-001"
    assert item.description == "ATB Market"
    assert item.mcc == 5411
    assert item.original_mcc == 5411
    assert item.amount == -5000
    assert item.operation_amount == -5000
    assert item.currency_code == 980
    assert item.cashback_amount == 25
    assert item.balance == 95000

    call_url: str = mock_http.get.call_args[0][0]
    assert "acc-001/1700000000/1700086400" in call_url


async def test_set_webhook_posts_correct_body() -> None:
    resp = _make_mock_response({})
    ctx, mock_http = _patch_async_client(resp)

    webhook_url = "https://example.com/webhook"

    with ctx:
        client = MonobankClient(token="test-token")
        await client.set_webhook(webhook_url)

    mock_http.post.assert_awaited_once()
    call_kwargs = mock_http.post.call_args[1]
    assert call_kwargs["json"] == {"webHookUrl": webhook_url}

    call_url: str = mock_http.post.call_args[0][0]
    assert call_url.endswith("/personal/webhook")


# ---------------------------------------------------------------------------
# Tests — error handling
# ---------------------------------------------------------------------------


async def test_api_error_raised_on_401() -> None:
    error_body = {"errorDescription": "Unauthorized"}
    resp = _make_mock_response(error_body, status_code=401)
    ctx, _ = _patch_async_client(resp)

    with ctx:
        client = MonobankClient(token="bad-token")
        with pytest.raises(MonobankAPIError) as exc_info:
            await client.get_client_info()

    assert exc_info.value.status_code == 401
    assert exc_info.value.message == "Unauthorized"


async def test_api_error_raised_on_429() -> None:
    resp = _make_mock_response(
        {"errorDescription": "Too Many Requests"}, status_code=429
    )
    ctx, _ = _patch_async_client(resp)

    with ctx:
        client = MonobankClient(token="test-token")
        with pytest.raises(MonobankAPIError) as exc_info:
            await client.get_statements("acc-001", 0, 1)

    assert exc_info.value.status_code == 429


# ---------------------------------------------------------------------------
# Tests — constructor / context manager
# ---------------------------------------------------------------------------


async def test_client_sends_token_header() -> None:
    """Token is passed to AsyncClient as a header at construction time."""
    resp = _make_mock_response(_CLIENT_INFO_JSON)

    with patch("grosh_ingestion.sources.monobank.client.httpx.AsyncClient") as mock_cls:
        mock_instance = _make_mock_http_client(resp)
        mock_cls.return_value = mock_instance

        MonobankClient(token="my-secret-token")

    _, kwargs = mock_cls.call_args
    assert kwargs["headers"]["X-Token"] == "my-secret-token"


async def test_base_url_override() -> None:
    resp = _make_mock_response(_CLIENT_INFO_JSON)

    with patch("grosh_ingestion.sources.monobank.client.httpx.AsyncClient") as mock_cls:
        mock_instance = _make_mock_http_client(resp)
        mock_cls.return_value = mock_instance

        MonobankClient(token="test-token", base_url="https://mock.local")

    _, kwargs = mock_cls.call_args
    assert kwargs["base_url"] == "https://mock.local"


async def test_context_manager_calls_close() -> None:
    resp = _make_mock_response(_CLIENT_INFO_JSON)
    ctx, mock_http = _patch_async_client(resp)

    with ctx:
        async with MonobankClient(token="test-token") as client:
            await client.get_client_info()

    mock_http.aclose.assert_awaited_once()


# ---------------------------------------------------------------------------
# Tests — numeric_to_alpha (shared iso_4217 module)
# ---------------------------------------------------------------------------


def test_iso_4217_known_codes() -> None:
    assert numeric_to_alpha(980) == "UAH"
    assert numeric_to_alpha(840) == "USD"
    assert numeric_to_alpha(978) == "EUR"
    assert numeric_to_alpha(826) == "GBP"
    assert numeric_to_alpha(985) == "PLN"


def test_iso_4217_unknown_code_raises() -> None:
    with pytest.raises(ValueError, match="Unknown ISO 4217 numeric code: 999"):
        numeric_to_alpha(999)
