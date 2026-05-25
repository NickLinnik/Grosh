from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class MonobankAccount(BaseModel):
    model_config = ConfigDict(frozen=True, populate_by_name=True)

    id: str
    send_id: str = Field(alias="sendId")
    balance: int
    credit_limit: int = Field(alias="creditLimit")
    type: str
    currency_code: int = Field(alias="currencyCode")
    cashback_type: str | None = Field(alias="cashbackType", default=None)
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


class MonobankWebhookData(BaseModel):
    model_config = ConfigDict(frozen=True, populate_by_name=True)

    account: str
    statement_item: dict[str, Any] = Field(alias="statementItem")


class MonobankWebhookPayload(BaseModel):
    model_config = ConfigDict(frozen=True, populate_by_name=True)

    type: str
    data: MonobankWebhookData


class MonobankCurrencyRate(BaseModel):
    model_config = ConfigDict(frozen=True, populate_by_name=True)

    currency_code_a: int = Field(alias="currencyCodeA")
    currency_code_b: int = Field(alias="currencyCodeB")
    date: int
    rate_buy: float | None = Field(alias="rateBuy", default=None)
    rate_sell: float | None = Field(alias="rateSell", default=None)
    rate_cross: float | None = Field(alias="rateCross", default=None)
