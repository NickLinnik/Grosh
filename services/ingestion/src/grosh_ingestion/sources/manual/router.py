from datetime import datetime
from typing import Annotated
from uuid import UUID

import asyncpg
from confluent_kafka import Producer
from fastapi import APIRouter, Depends, HTTPException
from grosh_shared.models import AccountType, TransactionType
from pydantic import BaseModel, Field

from grosh_ingestion.deps import get_current_user_id, get_db_conn, get_producer
from grosh_ingestion.sources.manual.service import ManualService, get_manual_service

router = APIRouter(prefix="/manual", tags=["manual"])


# -- Request models --


class CreateAccountRequest(BaseModel):
    type: AccountType
    currency_code: str
    name: str


class CreateTransactionRequest(BaseModel):
    account_id: UUID
    amount_cents: int = Field(gt=0)
    currency_code: str
    description: str | None = None
    time: datetime
    transaction_type: TransactionType
    mcc: int | None = None
    rate_source: str | None = None


# -- Endpoints --


@router.post("/accounts", status_code=201)
async def create_account(
    body: CreateAccountRequest,
    user_id: Annotated[UUID, Depends(get_current_user_id)],
    conn: Annotated[asyncpg.Connection, Depends(get_db_conn)],
    service: Annotated[ManualService, Depends(get_manual_service)],
) -> dict:
    """Create a manual (cash) account."""
    if body.type != AccountType.cash:
        raise HTTPException(
            status_code=422,
            detail="Only 'cash' account type is supported for manual accounts.",
        )

    account_id = await service.create_account(
        conn=conn,
        user_id=user_id,
        account_type=body.type,
        currency_code=body.currency_code,
        name=body.name,
    )

    return {
        "id": account_id,
        "type": str(body.type),
        "currency_code": body.currency_code,
        "name": body.name,
    }


@router.post("/transactions", status_code=201)
async def create_transaction(
    body: CreateTransactionRequest,
    user_id: Annotated[UUID, Depends(get_current_user_id)],
    conn: Annotated[asyncpg.Connection, Depends(get_db_conn)],
    producer: Annotated[Producer, Depends(get_producer)],
    service: Annotated[ManualService, Depends(get_manual_service)],
) -> dict:
    """Record a manual transaction and publish it to the raw_transactions topic."""
    if body.transaction_type not in (TransactionType.income, TransactionType.expense):
        raise HTTPException(
            status_code=422,
            detail="transaction_type must be 'income' or 'expense'.",
        )

    try:
        event = await service.create_transaction(
            conn=conn,
            user_id=user_id,
            account_id=body.account_id,
            amount_cents=body.amount_cents,
            currency_code=body.currency_code,
            description=body.description,
            time=body.time,
            transaction_type=body.transaction_type,
            mcc=body.mcc,
            rate_source=body.rate_source,
            producer=producer,
        )
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))

    return {
        "id": str(event.id),
        "source": event.source,
        "source_id": event.source_id,
        "account_id": str(event.account_id),
        "time": event.time.isoformat(),
        "amount_cents": event.amount_cents,
        "currency_code": event.currency_code,
        "description": event.description,
        "transaction_type": event.transaction_type,
        "rate_source": event.rate_source,
    }
