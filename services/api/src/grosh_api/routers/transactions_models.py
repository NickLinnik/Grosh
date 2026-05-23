from datetime import datetime
from enum import StrEnum
from uuid import UUID

from pydantic import BaseModel, Field


class Currency(StrEnum):
    UAH = "UAH"
    USD = "USD"
    EUR = "EUR"


class AggregateField(StrEnum):
    income = "income"
    expense = "expense"
    delta = "delta"


class Bucket(StrEnum):
    day = "day"
    week = "week"
    month = "month"
    quarter = "quarter"
    year = "year"


class TransactionResponse(BaseModel):
    id: UUID
    account_id: UUID
    time: datetime
    amount_cents: int
    operation_amount_cents: int | None
    currency_code: str
    operation_currency_code: str | None
    amount_uah_cents: int | None
    amount_usd_cents: int | None
    amount_eur_cents: int | None
    description: str | None
    mcc: str | None
    cashback_amount_cents: int
    balance_cents: int | None
    hold: bool | None
    direction: str
    special_category: str | None
    counterparty_iban: str | None
    rate_source: str | None
    metadata: dict[str, object] | None
    source: str
    origin: str
    related_transaction_id: UUID | None


class CurrencyAggregate(BaseModel):
    total_income_cents: int | None = None
    total_expense_cents: int | None = None
    delta_cents: int | None = None
    converted_pct: float


class AggregateItem(BaseModel):
    period_start: datetime = Field(
        description=(
            "Bucket start in UTC. The bucket boundary is computed in the user's"
            " timezone (from user_settings.timezone) but serialized as a UTC"
            " ISO timestamp. For a Europe/Kyiv user, the January 2026 bucket"
            " appears as '2025-12-31T22:00:00Z' (= 2026-01-01T00:00 Kyiv)."
        )
    )
    currencies: dict[str, CurrencyAggregate]


class AggregatesResponse(BaseModel):
    bucket: str
    items: list[AggregateItem]
