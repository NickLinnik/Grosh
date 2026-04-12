from typing import Any

import httpx
from pydantic import BaseModel, ConfigDict, Field

BASE_URL = "https://api.monobank.ua"

_TIMEOUT = httpx.Timeout(30.0, connect=5.0)

_ISO_4217_NUMERIC_TO_ALPHA: dict[int, str] = {
    980: "UAH",
    840: "USD",
    978: "EUR",
    826: "GBP",
    985: "PLN",
    203: "CZK",
    756: "CHF",
    392: "JPY",
    156: "CNY",
    949: "TRY",
}


def iso_4217_to_alpha(code: int) -> str:
    """Convert ISO 4217 numeric currency code to alpha-3 code."""
    alpha = _ISO_4217_NUMERIC_TO_ALPHA.get(code)
    if alpha is None:
        raise ValueError(f"Unknown ISO 4217 numeric code: {code}")
    return alpha


class MonobankAPIError(Exception):
    def __init__(self, status_code: int, message: str) -> None:
        self.status_code = status_code
        self.message = message
        super().__init__(f"Monobank API error {status_code}: {message}")


class MonobankAccount(BaseModel):
    model_config = ConfigDict(frozen=True, populate_by_name=True)

    id: str
    send_id: str = Field(alias="sendId")
    balance: int
    credit_limit: int = Field(alias="creditLimit")
    type: str
    currency_code: int = Field(alias="currencyCode")
    cashback_type: str = Field(alias="cashbackType")
    masked_pan: list[str] = Field(alias="maskedPan", default_factory=list)
    iban: str


class MonobankClientInfo(BaseModel):
    model_config = ConfigDict(frozen=True, populate_by_name=True)

    client_id: str = Field(alias="clientId")
    name: str
    accounts: list[MonobankAccount]


class MonobankStatementItem(BaseModel):
    model_config = ConfigDict(frozen=True, populate_by_name=True)

    id: str
    time: int
    description: str
    mcc: int
    original_mcc: int = Field(alias="originalMcc")
    hold: bool
    amount: int
    operation_amount: int = Field(alias="operationAmount")
    currency_code: int = Field(alias="currencyCode")
    cashback_amount: int = Field(alias="cashbackAmount")
    balance: int
    comment: str | None = Field(default=None)
    receipt_id: str | None = Field(alias="receiptId", default=None)
    invoice_id: str | None = Field(alias="invoiceId", default=None)
    counter_edrpou: str | None = Field(alias="counterEdrpou", default=None)
    counter_iban: str | None = Field(alias="counterIban", default=None)
    counter_name: str | None = Field(alias="counterName", default=None)


class MonobankClient:
    def __init__(self, token: str, base_url: str = BASE_URL) -> None:
        self._base_url = base_url.rstrip("/")
        self._client = httpx.AsyncClient(
            base_url=self._base_url,
            headers={"X-Token": token},
            timeout=_TIMEOUT,
        )

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
