import os
from collections.abc import AsyncGenerator
from typing import Annotated
from uuid import UUID

import asyncpg
from confluent_kafka import Producer
from fastapi import Depends, Request
from grosh_shared.auth import (
    AUTH_HEADER,
    BEARER_PREFIX,
    InvalidAccessTokenError,
    decode_access_token,
    extract_user_id,
)
from grosh_shared.errors import ErrorCode, raise_problem
from grosh_shared.user_db import set_rls_user_id

from grosh_ingestion.repositories.reprocess_repo import ReprocessRepo
from grosh_ingestion.repositories.revoked_token_repo import RevokedTokenRepo
from grosh_ingestion.repositories.user_repo import UserRepo
from grosh_ingestion.services.backfill_service import BackfillService
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
        _backfill_service = BackfillService()
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

    if not await _user_repo.is_active(conn, user_id):
        raise_problem(
            401,
            ErrorCode.AUTHENTICATION_REQUIRED,
            "Not authenticated.",
            instance=str(request.url.path),
        )

    return user_id
