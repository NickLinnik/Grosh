from datetime import datetime
from typing import Annotated
from uuid import UUID

import asyncpg
from fastapi import APIRouter, Depends, Query
from grosh_shared.errors import ErrorCode, raise_problem
from grosh_shared.models import User, UserRole
from pydantic import BaseModel, ConfigDict, EmailStr, field_validator

from grosh_api.deps import get_db_conn, get_user_repo, get_user_service, require_admin
from grosh_api.pagination import CursorPage, decode_cursor, encode_cursor
from grosh_api.repositories.user_repo import UserRepo
from grosh_api.services.user_service import UserService

router = APIRouter(prefix="/admin", tags=["admin"])


class CreateUserRequest(BaseModel):
    email: EmailStr
    password: str
    display_name: str
    role: UserRole = UserRole.member

    @field_validator("email")
    @classmethod
    def _lowercase(cls, v: str) -> str:
        return v.lower()


class AdminUserResponse(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: UUID
    email: str
    role: UserRole
    created_at: datetime
    last_active_at: datetime | None


class UserCreatedResponse(BaseModel):
    id: UUID
    email: str
    display_name: str
    role: UserRole


@router.post("/users", response_model=UserCreatedResponse, status_code=201)
async def create_user(
    body: CreateUserRequest,
    conn: Annotated[asyncpg.Connection, Depends(get_db_conn)],
    users: Annotated[UserService, Depends(get_user_service)],
    _admin: Annotated[User, Depends(require_admin)],
) -> UserCreatedResponse:
    record = await users.create_user(
        conn,
        email=body.email,
        password=body.password,
        display_name=body.display_name,
        role=body.role,
    )
    return UserCreatedResponse(
        id=record.id,
        email=record.email,
        display_name=record.display_name,
        role=record.role,
    )


@router.get("/users", response_model=CursorPage[AdminUserResponse])
async def list_users(
    conn: Annotated[asyncpg.Connection, Depends(get_db_conn)],
    user_repo: Annotated[UserRepo, Depends(get_user_repo)],
    _admin: Annotated[User, Depends(require_admin)],
    limit: int = Query(50, ge=1, le=200),
    cursor: str | None = Query(None),
) -> CursorPage[AdminUserResponse]:
    cursor_created_at: datetime | None = None
    cursor_id: UUID | None = None
    if cursor is not None:
        try:
            ts_str, id_str = decode_cursor(cursor)
            cursor_created_at = datetime.fromisoformat(ts_str)
            cursor_id = UUID(id_str)
        except ValueError:
            raise_problem(400, ErrorCode.INVALID_CURSOR, "Invalid pagination cursor.")

    total = await user_repo.count_admin_users(conn)
    rows = await user_repo.list_admin_users(
        conn,
        limit=limit + 1,
        cursor_created_at=cursor_created_at,
        cursor_id=cursor_id,
    )

    has_more = len(rows) > limit
    if has_more:
        rows = rows[:limit]

    next_cursor: str | None = None
    if has_more:
        last = rows[-1]
        next_cursor = encode_cursor(last.created_at, str(last.id))

    items = [
        AdminUserResponse(
            id=row.id,
            email=row.email,
            role=row.role,
            created_at=row.created_at,
            last_active_at=row.last_active_at,
        )
        for row in rows
    ]

    return CursorPage(items=items, total=total, limit=limit, next_cursor=next_cursor)


@router.delete("/users/{user_id}", status_code=204)
async def delete_user(
    user_id: UUID,
    conn: Annotated[asyncpg.Connection, Depends(get_db_conn)],
    users: Annotated[UserService, Depends(get_user_service)],
    current_user: Annotated[User, Depends(require_admin)],
) -> None:
    await users.delete_user(conn, admin_id=current_user.id, target_id=user_id)
