from enum import StrEnum
from uuid import UUID

from pydantic import BaseModel, ConfigDict


class UserRole(StrEnum):
    admin = "admin"
    member = "member"


class BankSource(StrEnum):
    monobank = "monobank"


class TransactionDirection(StrEnum):
    income = "income"
    expense = "expense"
    zero = "zero"


class SpecialCategory(StrEnum):
    transfer = "transfer"


class TransactionSource(StrEnum):
    monobank = "monobank"
    manual = "manual"


class TransactionOrigin(StrEnum):
    bank = "bank"
    manual = "manual"


class Currency(StrEnum):
    UAH = "UAH"
    USD = "USD"
    EUR = "EUR"
    GBP = "GBP"
    PLN = "PLN"
    CZK = "CZK"
    CHF = "CHF"
    JPY = "JPY"
    CNY = "CNY"
    TRY = "TRY"


class Topic(StrEnum):
    raw_transactions_monobank = "raw_transactions.monobank"
    raw_transactions_manual = "raw_transactions.manual"
    normalized_transactions = "normalized_transactions"


class RateSource(StrEnum):
    monobank = "monobank"
    nbu = "nbu"


class IntegrationStatus(StrEnum):
    active = "active"
    inactive = "inactive"
    error = "error"


class User(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: UUID
    email: str
    display_name: str
    role: UserRole
    is_active: bool = True


class Category(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: UUID
    name: str
    parent_id: UUID | None = None


class BankIntegration(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: UUID
    user_id: UUID
    bank: BankSource
    webhook_secret: str
    webhook_url: str | None = None
    status: IntegrationStatus = IntegrationStatus.active


class Account(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: UUID
    user_id: UUID
    integration_id: UUID | None = None
    source: TransactionSource
    type: str
    currency_code: str
    masked_pan: str | None = None
    iban: str | None = None
    external_id: str | None = None
    cashback_type: str | None = None
    is_active: bool = True
