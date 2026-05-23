import os
from collections.abc import AsyncGenerator
from typing import Annotated
from uuid import UUID

import asyncpg
from confluent_kafka import Producer
from fastapi import Depends, Request
from grosh_shared.db.rls import set_rls_user_id, set_rls_user_role
from grosh_shared.domain.models import UserRole
from grosh_shared.http.auth import (
    AUTH_HEADER,
    BEARER_PREFIX,
    InvalidAccessTokenError,
    decode_access_token,
    extract_user_id,
)
from grosh_shared.http.errors import ErrorCode, raise_problem

from grosh_ingestion.repositories.reprocess_repo import ReprocessRepo
from grosh_ingestion.repositories.revoked_token_repo import RevokedTokenRepo
from grosh_ingestion.repositories.user_repo import UserRepo
from grosh_ingestion.services.backfill_service import BackfillService, load_batch_api
from grosh_ingestion.services.job_status_service import JobStatusService
from grosh_ingestion.services.reprocess_dispatcher import ReprocessDispatcher

_user_repo = UserRepo()
_revoked_token_repo = RevokedTokenRepo()
_backfill_service: BackfillService | None = None
_reprocess_repo = ReprocessRepo()
_reprocess_dispatcher: ReprocessDispatcher | None = None


def get_user_repo() -> UserRepo:
    return _user_repo


def get_revoked_token_repo() -> RevokedTokenRepo:
    return _revoked_token_repo


def get_backfill_service() -> BackfillService:
    global _backfill_service
    if _backfill_service is None:
        _backfill_service = BackfillService(batch_api=load_batch_api())
    return _backfill_service


def get_reprocess_repo() -> ReprocessRepo:
    return _reprocess_repo


def get_reprocess_dispatcher(request: Request) -> ReprocessDispatcher:
    return request.app.state.reprocess_dispatcher


def get_job_status_service(request: Request) -> JobStatusService:
    return request.app.state.job_status_service


async def get_db_conn(request: Request) -> AsyncGenerator[asyncpg.Connection, None]:
    async with request.app.state.pool.acquire() as conn:
        async with conn.transaction():
            yield conn


def get_producer(request: Request) -> Producer:
    return request.app.state.producer


async def get_current_user_id(
    request: Request,
    conn: Annotated[asyncpg.Connection, Depends(get_db_conn)],
    revoked_token_repo: Annotated[RevokedTokenRepo, Depends(get_revoked_token_repo)],
) -> UUID:
    auth_header = request.headers.get(AUTH_HEADER)
    if not auth_header or not auth_header.startswith(BEARER_PREFIX):
        raise_problem(
            401,
            ErrorCode.AUTHENTICATION_REQUIRED,
            "Not authenticated.",
            instance=str(request.url.path),
        )

    token = auth_header.removeprefix(BEARER_PREFIX)

    try:
        payload = decode_access_token(token, os.environ["JWT_SECRET"])
        user_id = extract_user_id(payload)
    except InvalidAccessTokenError:
        raise_problem(
            401,
            ErrorCode.AUTHENTICATION_REQUIRED,
            "Not authenticated.",
            instance=str(request.url.path),
        )

    jti_str = payload.get("jti")
    if jti_str:
        try:
            jti = UUID(str(jti_str))
        except ValueError:
            jti = None
        if jti is not None and await revoked_token_repo.is_revoked(conn, jti):
            raise_problem(
                401,
                ErrorCode.AUTHENTICATION_REQUIRED,
                "Not authenticated.",
                instance=str(request.url.path),
            )

    await set_rls_user_id(conn, user_id)

    role = await _user_repo.get_role(conn, user_id)
    if role is not None:
        await set_rls_user_role(conn, role.value)

    if not await _user_repo.is_active(conn, user_id):
        raise_problem(
            401,
            ErrorCode.AUTHENTICATION_REQUIRED,
            "Not authenticated.",
            instance=str(request.url.path),
        )

    return user_id


async def require_admin(
    caller_id: Annotated[UUID, Depends(get_current_user_id)],
    conn: Annotated[asyncpg.Connection, Depends(get_db_conn)],
    user_repo: Annotated[UserRepo, Depends(get_user_repo)],
) -> UUID:
    """Verify the caller has admin role; return their user_id on success.

    Raises 403 INSUFFICIENT_PERMISSIONS if the caller is not an admin.
    Pair with `get_current_user_id` for endpoints that need both the
    admin check and the caller identity (the returned UUID is the caller's).
    """
    role = await user_repo.get_role(conn, caller_id)
    if role != UserRole.admin:
        raise_problem(
            403,
            ErrorCode.INSUFFICIENT_PERMISSIONS,
            "Admin role required.",
        )
    return caller_id
