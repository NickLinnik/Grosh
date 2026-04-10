from datetime import datetime
from enum import Enum
from uuid import UUID

from pydantic import BaseModel, ConfigDict


class UserRole(str, Enum):
    admin = "admin"
    member = "member"


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


class Transaction(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: str  # Monobank uses string IDs
    user_id: UUID
    account_id: UUID
    time: datetime
    amount: int  # cents
    currency_code: int  # ISO 4217
    description: str
    mcc: int | None = None
    category_id: UUID | None = None
    classified_by: str | None = None  # "rule" | "mcc" | "ml" | "manual"
