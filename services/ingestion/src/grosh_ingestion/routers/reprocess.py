import asyncio
import logging
from datetime import UTC, datetime, timedelta
from typing import Annotated
from uuid import UUID

import asyncpg
from fastapi import APIRouter, Depends, HTTPException

from grosh_ingestion.deps import (
    get_current_user_id,
    get_db_conn,
    get_reprocess_dispatcher,
    get_reprocess_repo,
)
from grosh_ingestion.repositories.reprocess_repo import ReprocessRepo
from grosh_ingestion.services.reprocess_dispatcher import ReprocessDispatcher

logger = logging.getLogger(__name__)

router = APIRouter(tags=["reprocess"])

_COOLDOWN = timedelta(hours=1)


@router.post("/reprocess", status_code=202)
async def trigger_reprocess(
    user_id: Annotated[UUID, Depends(get_current_user_id)],
    conn: Annotated[asyncpg.Connection, Depends(get_db_conn)],
    repo: Annotated[ReprocessRepo, Depends(get_reprocess_repo)],
    dispatcher: Annotated[ReprocessDispatcher, Depends(get_reprocess_dispatcher)],
) -> dict:
    """Trigger a full transaction reprocess K8s Job for the authenticated user."""
    if await repo.lock_exists(conn, user_id):
        raise HTTPException(status_code=409, detail="Reprocess already in progress")

    last = await repo.last_reprocess_at(conn, user_id)
    if last is not None and (datetime.now(UTC) - last) < _COOLDOWN:
        cooldown_until = last + _COOLDOWN
        raise HTTPException(
            status_code=429,
            detail=f"Cooldown active until {cooldown_until.isoformat()}",
        )

    job_name = await asyncio.to_thread(dispatcher.trigger_reprocess, user_id)
    logger.info("Triggered reprocess job %s for user %s", job_name, user_id)
    return {"status": "started", "user_id": str(user_id), "job_name": job_name}
