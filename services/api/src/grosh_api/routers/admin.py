from typing import Annotated
from uuid import UUID

import asyncpg
from fastapi import APIRouter, Depends
from grosh_shared.models import User, UserRole
from pydantic import BaseModel, EmailStr, field_validator

from grosh_api.deps import get_db_conn, get_user_service, require_admin
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


@router.delete("/users/{user_id}", status_code=204)
async def delete_user(
    user_id: UUID,
    conn: Annotated[asyncpg.Connection, Depends(get_db_conn)],
    users: Annotated[UserService, Depends(get_user_service)],
    current_user: Annotated[User, Depends(require_admin)],
) -> None:
    await users.delete_user(conn, admin_id=current_user.id, target_id=user_id)
