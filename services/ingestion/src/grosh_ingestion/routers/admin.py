import logging
from datetime import date
from typing import Annotated
from uuid import UUID

import asyncpg
from fastapi import APIRouter, Depends, HTTPException
from grosh_shared.models import UserRole
from pydantic import BaseModel

from grosh_ingestion.deps import (
    get_backfill_service,
    get_current_user_id,
    get_db_conn,
    get_user_repo,
)
from grosh_ingestion.registry import RATE_PROVIDERS
from grosh_ingestion.repositories.user_repo import UserRepo
from grosh_ingestion.services.backfill_service import BackfillService

logger = logging.getLogger(__name__)


router = APIRouter(prefix="/admin", tags=["admin"])


class RateBackfillRequest(BaseModel):
    source: str
    from_date: date
    to_date: date


@router.post("/rates-backfill", status_code=202)
async def trigger_rates_backfill(
    body: RateBackfillRequest,
    user_id: Annotated[UUID, Depends(get_current_user_id)],
    conn: Annotated[asyncpg.Connection, Depends(get_db_conn)],
    user_repo: Annotated[UserRepo, Depends(get_user_repo)],
    backfill_service: Annotated[BackfillService, Depends(get_backfill_service)],
) -> dict:
    """Trigger a historical rate backfill K8s Job. Requires admin role."""
    role = await user_repo.get_role(conn, user_id)
    if role != UserRole.admin:
        raise HTTPException(status_code=403, detail="Admin access required.")

    if body.from_date > body.to_date:
        raise HTTPException(
            status_code=422,
            detail="from_date must be <= to_date.",
        )

    config = RATE_PROVIDERS.get(body.source)
    if config is None:
        raise HTTPException(
            status_code=422,
            detail=f"Unknown rate source: '{body.source}'.",
        )
    if config.fetch_historical is None:
        raise HTTPException(
            status_code=422,
            detail=f"Source '{body.source}' does not support historical rate backfill.",
        )

    job_name = backfill_service.trigger_rates_backfill(
        source=body.source,
        from_date=body.from_date.isoformat(),
        to_date=body.to_date.isoformat(),
    )
    return {"job_name": job_name, "status": "accepted"}
