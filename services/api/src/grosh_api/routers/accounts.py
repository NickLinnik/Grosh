from typing import Annotated
from uuid import UUID

import asyncpg
from fastapi import APIRouter, Depends
from grosh_shared.models import AccountType, TransactionSource, User
from pydantic import BaseModel

from grosh_api.deps import get_current_user, get_db_conn
from grosh_api.repositories.account_repo import AccountRepo

router = APIRouter(prefix="/accounts", tags=["accounts"])

_account_repo = AccountRepo()


def get_account_repo() -> AccountRepo:
    return _account_repo


class AccountResponse(BaseModel):
    id: UUID
    source: TransactionSource
    type: AccountType
    currency_code: str
    masked_pan: str | None
    iban: str | None
    cashback_type: str | None
    is_active: bool


@router.get("", status_code=200)
async def list_accounts(
    user: Annotated[User, Depends(get_current_user)],
    conn: Annotated[asyncpg.Connection, Depends(get_db_conn)],
    repo: Annotated[AccountRepo, Depends(get_account_repo)],
) -> list[AccountResponse]:
    rows = await repo.list_by_user(conn, user.id)
    return [
        AccountResponse(
            id=row.id,
            source=row.source,
            type=row.type,
            currency_code=row.currency_code,
            masked_pan=row.masked_pan,
            iban=row.iban,
            cashback_type=row.cashback_type,
            is_active=row.is_active,
        )
        for row in rows
    ]
