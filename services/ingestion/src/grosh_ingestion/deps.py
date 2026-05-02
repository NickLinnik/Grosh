import os
from collections.abc import AsyncGenerator
from typing import Annotated
from uuid import UUID

import asyncpg
from confluent_kafka import Producer
from fastapi import Depends, HTTPException, Request
from grosh_shared.auth import (
    AUTH_HEADER,
    BEARER_PREFIX,
    CURRENT_USER_ID_SESSION_VAR,
    InvalidAccessTokenError,
    decode_access_token,
    extract_user_id,
)

from grosh_ingestion.repositories.user_repo import UserRepo
from grosh_ingestion.services.backfill_service import BackfillService

_user_repo = UserRepo()
_backfill_service: BackfillService | None = None


def get_user_repo() -> UserRepo:
    return _user_repo


def get_backfill_service() -> BackfillService:
    global _backfill_service
    if _backfill_service is None:
        _backfill_service = BackfillService()
    return _backfill_service


async def get_db_conn(request: Request) -> AsyncGenerator[asyncpg.Connection, None]:
    async with request.app.state.pool.acquire() as conn:
        async with conn.transaction():
            yield conn


def get_producer(request: Request) -> Producer:
    return request.app.state.producer


async def get_current_user_id(
    request: Request,
    conn: Annotated[asyncpg.Connection, Depends(get_db_conn)],
) -> UUID:
    auth_header = request.headers.get(AUTH_HEADER)
    if not auth_header or not auth_header.startswith(BEARER_PREFIX):
        raise HTTPException(status_code=401, detail="Not authenticated.")

    token = auth_header.removeprefix(BEARER_PREFIX)

    try:
        payload = decode_access_token(token, os.environ["JWT_SECRET"])
        user_id = extract_user_id(payload)
    except InvalidAccessTokenError:
        raise HTTPException(status_code=401, detail="Not authenticated.")

    jti_str = payload.get("jti")
    if jti_str:
        jti = UUID(jti_str)
        revoked = await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM revoked_tokens WHERE jti = $1)",
            jti,
        )
        if revoked:
            raise HTTPException(status_code=401, detail="Not authenticated.")

    await conn.execute(
        "SELECT set_config($1, $2, true)",
        CURRENT_USER_ID_SESSION_VAR,
        str(user_id),
    )

    if not await _user_repo.is_active(conn, user_id):
        raise HTTPException(status_code=401, detail="Not authenticated.")

    return user_id
