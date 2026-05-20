from datetime import datetime
from typing import Annotated
from uuid import UUID

import asyncpg
from confluent_kafka import Producer
from fastapi import APIRouter, Depends
from grosh_shared.errors import ErrorCode, raise_problem
from grosh_shared.models import TransactionDirection
from pydantic import BaseModel, Field

from grosh_ingestion.deps import get_current_user_id, get_db_conn, get_producer
from grosh_ingestion.repositories.account_repo import AccountRepo
from grosh_ingestion.sources.manual.schemas import (
    ManualAccountResponse,
    ManualTransactionResponse,
)
from grosh_ingestion.sources.manual.service import ManualService, get_manual_service

router = APIRouter(prefix="/manual", tags=["manual"])

_account_repo = AccountRepo()


def get_account_repo() -> AccountRepo:
    return _account_repo


# -- Request models --


class UpdateAccountRequest(BaseModel):
    name: str = Field(min_length=1, max_length=255)


class CreateAccountRequest(BaseModel):
    type: str
    currency_code: str
    name: str


class CreateTransactionRequest(BaseModel):
    account_id: UUID
    amount_cents: int = Field(gt=0)
    operation_currency_code: str
    description: str | None = None
    time: datetime
    direction: TransactionDirection
    mcc: str | None = None
    rate_source: str | None = None
    idempotency_key: str | None = None


# -- Endpoints --


@router.post("/accounts", status_code=201, response_model=ManualAccountResponse)
async def create_account(
    body: CreateAccountRequest,
    user_id: Annotated[UUID, Depends(get_current_user_id)],
    conn: Annotated[asyncpg.Connection, Depends(get_db_conn)],
    service: Annotated[ManualService, Depends(get_manual_service)],
) -> ManualAccountResponse:
    """Create a manual (cash) account."""
    if body.type != "cash":
        raise_problem(
            422,
            ErrorCode.VALIDATION_ERROR,
            "Only 'cash' account type is supported for manual accounts.",
        )

    created = await service.create_account(
        conn=conn,
        user_id=user_id,
        account_type=body.type,
        currency_code=body.currency_code,
        name=body.name,
    )

    return ManualAccountResponse(
        id=created.id,
        type=created.type,
        currency_code=created.currency_code,
        name=created.name or body.name,
        created_at=created.created_at,
    )


@router.post("/transactions", status_code=201, response_model=ManualTransactionResponse)
async def create_transaction(
    body: CreateTransactionRequest,
    user_id: Annotated[UUID, Depends(get_current_user_id)],
    conn: Annotated[asyncpg.Connection, Depends(get_db_conn)],
    producer: Annotated[Producer, Depends(get_producer)],
    service: Annotated[ManualService, Depends(get_manual_service)],
) -> ManualTransactionResponse:
    """Record a manual transaction and publish it to the raw_transactions topic."""
    if body.direction not in (
        TransactionDirection.income,
        TransactionDirection.expense,
    ):
        raise_problem(
            422,
            ErrorCode.VALIDATION_ERROR,
            "direction must be 'income' or 'expense'.",
        )

    event = await service.create_transaction(
        conn=conn,
        user_id=user_id,
        account_id=body.account_id,
        amount_cents=body.amount_cents,
        currency_code=body.operation_currency_code,
        description=body.description,
        time=body.time,
        direction=body.direction,
        mcc=body.mcc,
        rate_source=body.rate_source,
        producer=producer,
        idempotency_key=body.idempotency_key,
    )

    return ManualTransactionResponse(
        id=event.id,
        source=event.source,
        source_id=event.source_id,
        account_id=event.account_id,
        time=event.time,
        amount_cents=event.amount_cents,
        operation_currency_code=event.operation_currency_code,
        direction=event.direction,
        description=event.description,
        origin=event.origin,
    )


@router.put(
    "/accounts/{account_id}", status_code=200, response_model=ManualAccountResponse
)
async def update_account(
    account_id: UUID,
    body: UpdateAccountRequest,
    user_id: Annotated[UUID, Depends(get_current_user_id)],
    conn: Annotated[asyncpg.Connection, Depends(get_db_conn)],
    repo: Annotated[AccountRepo, Depends(get_account_repo)],
) -> ManualAccountResponse:
    """Update a manual account's name. Bank accounts cannot be edited."""
    existing = await repo.get_by_id(conn, account_id, user_id)
    if existing is None:
        raise_problem(404, ErrorCode.ACCOUNT_NOT_FOUND, "Account not found.")
    if existing["source"] != "manual":
        raise_problem(
            403,
            ErrorCode.INSUFFICIENT_PERMISSIONS,
            "Only manual accounts can be edited.",
        )

    try:
        updated = await repo.update_name(conn, account_id, user_id, body.name)
    except asyncpg.UniqueViolationError:
        raise_problem(
            409,
            ErrorCode.VALIDATION_ERROR,
            f"Another account named '{body.name}' already exists.",
        )
    if updated is None:
        raise_problem(404, ErrorCode.ACCOUNT_NOT_FOUND, "Account not found.")
    return ManualAccountResponse(
        id=updated.id,
        type=updated.type,
        currency_code=updated.currency_code,
        name=updated.name or body.name,
        created_at=updated.created_at,
    )


@router.delete("/accounts/{account_id}", status_code=204)
async def delete_account(
    account_id: UUID,
    user_id: Annotated[UUID, Depends(get_current_user_id)],
    conn: Annotated[asyncpg.Connection, Depends(get_db_conn)],
    repo: Annotated[AccountRepo, Depends(get_account_repo)],
) -> None:
    """Soft-delete a manual account (sets is_active=false)."""
    existing = await repo.get_by_id(conn, account_id, user_id)
    if existing is None:
        raise_problem(404, ErrorCode.ACCOUNT_NOT_FOUND, "Account not found.")
    if existing["source"] != "manual":
        raise_problem(
            403,
            ErrorCode.INSUFFICIENT_PERMISSIONS,
            "Cannot delete bank-connected accounts.",
        )
    deleted = await repo.soft_delete(conn, account_id, user_id)
    if not deleted:
        raise_problem(404, ErrorCode.ACCOUNT_NOT_FOUND, "Account not found.")
