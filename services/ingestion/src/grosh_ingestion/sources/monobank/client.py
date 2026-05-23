from typing import Any

import httpx

from grosh_ingestion.sources.monobank.models import (
    MonobankClientInfo,
    MonobankCurrencyRate,
    MonobankStatementItem,
)

BASE_URL = "https://api.monobank.ua"

_TIMEOUT = httpx.Timeout(30.0, connect=5.0)


class MonobankAPIError(Exception):
    def __init__(self, status_code: int, message: str) -> None:
        self.status_code = status_code
        self.message = message
        super().__init__(f"Monobank API error {status_code}: {message}")


async def fetch_currency_rates() -> list[MonobankCurrencyRate]:
    """Fetch public currency rates from Monobank (no auth required)."""
    async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
        resp = await client.get(f"{BASE_URL}/bank/currency")
        resp.raise_for_status()
        return [MonobankCurrencyRate.model_validate(item) for item in resp.json()]


class MonobankClient:
    def __init__(self, token: str, base_url: str = BASE_URL) -> None:
        self._base_url = base_url.rstrip("/")
        self._client = httpx.AsyncClient(
            base_url=self._base_url,
            headers={"X-Token": token},
            timeout=_TIMEOUT,
        )

    def set_timeout(self, read: float, connect: float) -> None:
        self._client.timeout = httpx.Timeout(read, connect=connect)

    async def close(self) -> None:
        await self._client.aclose()

    async def __aenter__(self) -> "MonobankClient":
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    async def get_client_info(self) -> MonobankClientInfo:
        response = await self._get("/personal/client-info")
        return MonobankClientInfo.model_validate(response)

    async def set_webhook(self, url: str) -> None:
        await self._post("/personal/webhook", {"webHookUrl": url})

    async def get_statements(
        self, account_id: str, from_ts: int, to_ts: int
    ) -> list[MonobankStatementItem]:
        path = f"/personal/statement/{account_id}/{from_ts}/{to_ts}"
        response = await self._get(path)
        return [MonobankStatementItem.model_validate(item) for item in response]

    async def _get(self, path: str) -> Any:
        resp = await self._client.get(path)
        self._raise_for_status(resp)
        return resp.json()

    async def _post(self, path: str, body: dict[str, Any]) -> None:
        resp = await self._client.post(path, json=body)
        self._raise_for_status(resp)

    @staticmethod
    def _raise_for_status(resp: httpx.Response) -> None:
        if resp.is_success:
            return
        try:
            message = resp.json().get("errorDescription", resp.text)
        except ValueError:
            message = resp.text
        raise MonobankAPIError(status_code=resp.status_code, message=message)
