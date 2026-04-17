from datetime import datetime
from enum import StrEnum
from uuid import UUID

from pydantic import BaseModel, ConfigDict


class UserRole(StrEnum):
    admin = "admin"
    member = "member"


class BankSource(StrEnum):
    monobank = "monobank"


class AccountType(StrEnum):
    black = "black"
    white = "white"
    platinum = "platinum"
    fop = "fop"
    cash = "cash"


class TransactionType(StrEnum):
    income = "income"
    expense = "expense"
    transfer = "transfer"
    check = "check"


class TransactionSource(StrEnum):
    monobank = "monobank"
    manual = "manual"


class TransactionOrigin(StrEnum):
    bank = "bank"
    manual = "manual"
    derived = "derived"


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
    type: AccountType
    currency_code: str
    masked_pan: str | None = None
    iban: str | None = None
    external_id: str | None = None
    cashback_type: str | None = None
    is_active: bool = True


class Transaction(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: UUID
    source_id: str
    user_id: UUID
    account_id: UUID
    time: datetime
    amount_cents: int
    operation_amount_cents: int | None = None
    currency_code: str
    description: str | None = None
    mcc: int | None = None
    cashback_amount_cents: int = 0
    balance_cents: int | None = None
    hold: bool = False
    transaction_type: TransactionType
    counterparty_iban: str | None = None
    metadata: dict | None = None
    source: TransactionSource
    origin: TransactionOrigin = TransactionOrigin.bank
    related_transaction_id: UUID | None = None
