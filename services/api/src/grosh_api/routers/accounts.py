from typing import Annotated
from uuid import UUID

import asyncpg
from fastapi import APIRouter, Depends, HTTPException, Query
from grosh_shared.models import TransactionSource, User
from pydantic import BaseModel

from grosh_api.deps import get_current_user, get_db_conn
from grosh_api.repositories.account_repo import AccountRepo, AccountRow

router = APIRouter(prefix="/accounts", tags=["accounts"])

_account_repo = AccountRepo()


def get_account_repo() -> AccountRepo:
    return _account_repo


class AccountResponse(BaseModel):
    id: UUID
    source: TransactionSource
    type: str
    currency_code: str
    masked_pan: str | None
    iban: str | None
    cashback_type: str | None
    name: str | None
    is_active: bool


class UpdateAccountRequest(BaseModel):
    name: str


def _row_to_response(row: "AccountRow") -> AccountResponse:
    return AccountResponse(
        id=row.id,
        source=row.source,
        type=row.type,
        currency_code=row.currency_code,
        masked_pan=row.masked_pan,
        iban=row.iban,
        cashback_type=row.cashback_type,
        name=row.name,
        is_active=row.is_active,
    )


@router.get("", status_code=200)
async def list_accounts(
    user: Annotated[User, Depends(get_current_user)],
    conn: Annotated[asyncpg.Connection, Depends(get_db_conn)],
    repo: Annotated[AccountRepo, Depends(get_account_repo)],
    source: str | None = Query(None),
    account_type: str | None = Query(None, alias="type"),
    currency_code: str | None = Query(None),
    name: str | None = Query(None),
) -> list[AccountResponse]:
    rows = await repo.list_by_user(
        conn,
        user.id,
        source=source,
        account_type=account_type,
        currency_code=currency_code,
        name=name,
    )
    return [_row_to_response(row) for row in rows]


@router.get("/{account_id}", status_code=200)
async def get_account(
    account_id: UUID,
    user: Annotated[User, Depends(get_current_user)],
    conn: Annotated[asyncpg.Connection, Depends(get_db_conn)],
    repo: Annotated[AccountRepo, Depends(get_account_repo)],
) -> AccountResponse:
    row = await repo.get_by_id(conn, account_id, user.id)
    if row is None:
        raise HTTPException(status_code=404, detail="Account not found.")
    return _row_to_response(row)


@router.put("/{account_id}", status_code=200)
async def update_account(
    account_id: UUID,
    body: UpdateAccountRequest,
    user: Annotated[User, Depends(get_current_user)],
    conn: Annotated[asyncpg.Connection, Depends(get_db_conn)],
    repo: Annotated[AccountRepo, Depends(get_account_repo)],
) -> AccountResponse:
    """Update a manual account's name. Bank accounts cannot be edited."""
    existing = await repo.get_by_id(conn, account_id, user.id)
    if existing is None:
        raise HTTPException(status_code=404, detail="Account not found.")
    if existing.source != TransactionSource.manual:
        raise HTTPException(
            status_code=403,
            detail="Only manual accounts can be edited.",
        )

    updated = await repo.update_name(conn, account_id, user.id, body.name)
    if updated is None:
        raise HTTPException(status_code=404, detail="Account not found.")
    return _row_to_response(updated)


@router.delete("/{account_id}", status_code=204)
async def delete_account(
    account_id: UUID,
    user: Annotated[User, Depends(get_current_user)],
    conn: Annotated[asyncpg.Connection, Depends(get_db_conn)],
    repo: Annotated[AccountRepo, Depends(get_account_repo)],
) -> None:
    """Soft-delete a manual account (sets is_active=false)."""
    existing = await repo.get_by_id(conn, account_id, user.id)
    if existing is None:
        raise HTTPException(status_code=404, detail="Account not found.")
    if existing.source != TransactionSource.manual:
        raise HTTPException(
            status_code=403,
            detail="Cannot delete bank-connected accounts.",
        )
    deleted = await repo.soft_delete(conn, account_id, user.id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Account not found.")
