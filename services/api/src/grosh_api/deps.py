from collections.abc import AsyncGenerator
from typing import Annotated
from uuid import UUID

import asyncpg
from fastapi import Depends, HTTPException, Request
from grosh_shared.auth import (
    AUTH_HEADER,
    BEARER_PREFIX,
    InvalidAccessTokenError,
    extract_user_id,
)
from grosh_shared.models import User, UserRole
from grosh_shared.user_db import set_rls_user_id

from grosh_api.repositories.revoked_token_repo import RevokedTokenRepo
from grosh_api.repositories.token_repo import TokenRepo
from grosh_api.repositories.user_repo import UserRepo
from grosh_api.services.auth_service import AuthService
from grosh_api.services.user_service import UserService

# -- Composition root ---------------------------------------------------------
# Stateless singletons wired once at startup. Exposed to routes via the
# provider functions below so FastAPI can inject them through Depends().

_user_repo = UserRepo()
_token_repo = TokenRepo()
_revoked_token_repo = RevokedTokenRepo()
_auth_service = AuthService(_user_repo, _token_repo, _revoked_token_repo)
_user_service = UserService(_user_repo, _token_repo, _auth_service)

# -- Provider functions (FastAPI dependencies) --------------------------------


def get_user_repo() -> UserRepo:
    return _user_repo


def get_token_repo() -> TokenRepo:
    return _token_repo


def get_auth_service() -> AuthService:
    return _auth_service


def get_user_service() -> UserService:
    return _user_service


async def get_db_conn(request: Request) -> AsyncGenerator[asyncpg.Connection, None]:
    async with request.app.state.pool.acquire() as conn:
        async with conn.transaction():
            yield conn


async def get_current_user(
    request: Request,
    conn: Annotated[asyncpg.Connection, Depends(get_db_conn)],
    auth: Annotated[AuthService, Depends(get_auth_service)],
    user_repo: Annotated[UserRepo, Depends(get_user_repo)],
) -> User:
    auth_header = request.headers.get(AUTH_HEADER)
    if not auth_header or not auth_header.startswith(BEARER_PREFIX):
        raise HTTPException(status_code=401, detail="Not authenticated.")

    token = auth_header.removeprefix(BEARER_PREFIX)
    payload = auth.decode_access_token(token)

    jti_str = payload.get("jti")
    if jti_str:
        try:
            jti = UUID(str(jti_str))
            if await auth.is_token_revoked(conn, jti):
                raise HTTPException(status_code=401, detail="Not authenticated.")
        except ValueError:
            pass

    try:
        user_id = extract_user_id(payload)
    except InvalidAccessTokenError:
        raise HTTPException(status_code=401, detail="Not authenticated.")

    await set_rls_user_id(conn, user_id)

    record = await user_repo.get_by_id(conn, user_id)
    if record is None or not record.is_active:
        raise HTTPException(status_code=401, detail="Not authenticated.")

    return record.to_user()


def require_admin(current_user: Annotated[User, Depends(get_current_user)]) -> User:
    if current_user.role != UserRole.admin:
        raise HTTPException(status_code=403, detail="Admin access required.")
    return current_user
