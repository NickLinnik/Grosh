"""Pydantic response models for Monobank lifecycle endpoints."""

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict


class MonobankAccountResponse(BaseModel):
    model_config = ConfigDict(frozen=True)

    account_id: UUID
    external_account_id: str
    currency_code: str
    was_rebound: bool


class MonobankLinkResponse(BaseModel):
    model_config = ConfigDict(frozen=True)

    integration_id: UUID
    is_new: bool
    webhook_registered: bool
    accounts: list[MonobankAccountResponse]


class MonobankIntegrationResponse(BaseModel):
    model_config = ConfigDict(frozen=True)

    integration_id: UUID
    monobank_client_id: str
    accounts: list[MonobankAccountResponse]
    created_at: datetime
