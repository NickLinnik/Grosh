from datetime import datetime
from typing import Annotated
from uuid import UUID

import asyncpg
from fastapi import APIRouter, Depends, HTTPException, Query
from grosh_shared.models import User
from pydantic import BaseModel

from grosh_api.deps import get_current_user, get_db_conn
from grosh_api.pagination import CursorPage, decode_cursor, encode_cursor
from grosh_api.repositories.transaction_repo import TransactionRepo

router = APIRouter(prefix="/transactions", tags=["transactions"])

_transaction_repo = TransactionRepo()


def get_transaction_repo() -> TransactionRepo:
    return _transaction_repo


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
    hold: bool
    direction: str
    special_category: str | None
    counterparty_iban: str | None
    rate_source: str | None
    metadata: dict[str, object] | None
    source: str
    origin: str
    related_transaction_id: UUID | None


@router.get("", status_code=200)
async def list_transactions(
    user: Annotated[User, Depends(get_current_user)],
    conn: Annotated[asyncpg.Connection, Depends(get_db_conn)],
    repo: Annotated[TransactionRepo, Depends(get_transaction_repo)],
    direction: str | None = Query(None),
    special_category: str | None = Query(None),
    account_id: UUID | None = Query(None),
    from_time: datetime | None = Query(None, alias="from"),
    to_time: datetime | None = Query(None, alias="to"),
    unconverted_currency: str | None = Query(None),
    limit: int = Query(50, ge=1, le=200),
    cursor: str | None = Query(None),
) -> CursorPage[TransactionResponse]:
    cursor_time: datetime | None = None
    cursor_id: UUID | None = None
    if cursor is not None:
        try:
            ts_str, id_str = decode_cursor(cursor)
            cursor_time = datetime.fromisoformat(ts_str)
            cursor_id = UUID(id_str)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid cursor.")

    total = await repo.count_transactions(
        conn,
        user.id,
        direction=direction,
        special_category=special_category,
        account_id=account_id,
        from_time=from_time,
        to_time=to_time,
        unconverted_currency=unconverted_currency,
    )
    rows = await repo.list_transactions(
        conn,
        user.id,
        direction=direction,
        special_category=special_category,
        account_id=account_id,
        from_time=from_time,
        to_time=to_time,
        unconverted_currency=unconverted_currency,
        cursor_time=cursor_time,
        cursor_id=cursor_id,
        limit=limit,
    )
    items = [
        TransactionResponse(
            id=row.id,
            account_id=row.account_id,
            time=row.time,
            amount_cents=row.amount_cents,
            operation_amount_cents=row.operation_amount_cents,
            currency_code=row.currency_code,
            operation_currency_code=row.operation_currency_code,
            amount_uah_cents=row.amount_uah_cents,
            amount_usd_cents=row.amount_usd_cents,
            amount_eur_cents=row.amount_eur_cents,
            description=row.description,
            mcc=row.mcc,
            cashback_amount_cents=row.cashback_amount_cents,
            balance_cents=row.balance_cents,
            hold=row.hold,
            direction=row.direction,
            special_category=row.special_category,
            counterparty_iban=row.counterparty_iban,
            rate_source=row.rate_source,
            metadata=row.metadata,
            source=row.source,
            origin=row.origin,
            related_transaction_id=row.related_transaction_id,
        )
        for row in rows
    ]
    next_cursor = None
    if len(items) == limit:
        last = rows[-1]
        next_cursor = encode_cursor(last.time, str(last.id))

    return CursorPage(items=items, total=total, limit=limit, next_cursor=next_cursor)
