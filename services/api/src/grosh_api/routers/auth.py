from typing import Annotated
from uuid import UUID

import asyncpg
from fastapi import APIRouter, Depends, Request, Response
from fastapi.responses import JSONResponse
from grosh_shared.errors import ErrorCode, raise_problem
from grosh_shared.models import User, UserRole
from pydantic import BaseModel, EmailStr, field_validator

from grosh_api.constants import REFRESH_TOKEN_COOKIE
from grosh_api.deps import get_auth_service, get_current_user, get_db_conn
from grosh_api.services.auth_service import AuthService

router = APIRouter(tags=["auth"])

_REFRESH_TOKEN_MAX_AGE = 30 * 24 * 60 * 60  # 30 days in seconds


def _set_refresh_cookie(response: Response, value: str) -> None:
    response.set_cookie(
        key=REFRESH_TOKEN_COOKIE,
        value=value,
        httponly=True,
        secure=True,
        samesite="lax",
        max_age=_REFRESH_TOKEN_MAX_AGE,
    )


def _clear_refresh_cookie(response: Response) -> None:
    response.set_cookie(
        key=REFRESH_TOKEN_COOKIE,
        value="",
        httponly=True,
        secure=True,
        samesite="lax",
        max_age=0,
    )


class LoginRequest(BaseModel):
    email: EmailStr
    password: str

    @field_validator("email")
    @classmethod
    def _lowercase(cls, v: str) -> str:
        return v.lower()


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"


class UserResponse(BaseModel):
    id: UUID
    email: str
    display_name: str
    role: UserRole
    is_active: bool


@router.post("/auth/login", response_model=TokenResponse)
async def login(
    body: LoginRequest,
    conn: Annotated[asyncpg.Connection, Depends(get_db_conn)],
    auth: Annotated[AuthService, Depends(get_auth_service)],
) -> JSONResponse:
    access_token, refresh_token = await auth.login(conn, body.email, body.password)
    response = JSONResponse(
        content={"access_token": access_token, "token_type": "bearer"}
    )
    _set_refresh_cookie(response, refresh_token)
    return response


@router.post("/auth/refresh", response_model=TokenResponse)
async def refresh(
    request: Request,
    conn: Annotated[asyncpg.Connection, Depends(get_db_conn)],
    auth: Annotated[AuthService, Depends(get_auth_service)],
) -> JSONResponse:
    raw_token = request.cookies.get(REFRESH_TOKEN_COOKIE)
    if raw_token is None:
        raise_problem(
            401,
            ErrorCode.AUTHENTICATION_REQUIRED,
            "Session expired. Please log in again.",
            instance=str(request.url.path),
        )

    access_token, new_raw = await auth.refresh(conn, raw_token)
    response = JSONResponse(
        content={"access_token": access_token, "token_type": "bearer"}
    )
    _set_refresh_cookie(response, new_raw)
    return response


@router.post("/auth/logout", status_code=204)
async def logout(
    request: Request,
    conn: Annotated[asyncpg.Connection, Depends(get_db_conn)],
    auth: Annotated[AuthService, Depends(get_auth_service)],
) -> Response:
    raw_token = request.cookies.get(REFRESH_TOKEN_COOKIE)

    access_token = None
    auth_header = request.headers.get("Authorization")
    if auth_header and auth_header.startswith("Bearer "):
        access_token = auth_header.removeprefix("Bearer ")

    if raw_token is None and access_token is None:
        return Response(status_code=204)

    await auth.logout(conn, raw_token or "", access_token)
    response = Response(status_code=204)
    _clear_refresh_cookie(response)
    return response


@router.get("/auth/me", response_model=UserResponse)
async def me(current_user: Annotated[User, Depends(get_current_user)]) -> UserResponse:
    return UserResponse(
        id=current_user.id,
        email=current_user.email,
        display_name=current_user.display_name,
        role=current_user.role,
        is_active=current_user.is_active,
    )


@router.post("/auth/logout-all", status_code=204)
async def logout_all(
    request: Request,
    conn: Annotated[asyncpg.Connection, Depends(get_db_conn)],
    auth: Annotated[AuthService, Depends(get_auth_service)],
    current_user: Annotated[User, Depends(get_current_user)],
) -> Response:
    access_token = None
    auth_header = request.headers.get("Authorization")
    if auth_header and auth_header.startswith("Bearer "):
        access_token = auth_header.removeprefix("Bearer ")

    await auth.logout_all(conn, current_user.id, access_token)
    response = Response(status_code=204)
    _clear_refresh_cookie(response)
    return response
