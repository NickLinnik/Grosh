from datetime import UTC, datetime
from decimal import Decimal
from typing import Annotated
from uuid import UUID

import asyncpg
from fastapi import APIRouter, Depends, HTTPException, Query
from grosh_shared.models import User
from pydantic import BaseModel

from grosh_api.deps import get_current_user, get_db_conn
from grosh_api.pagination import CursorPage, decode_cursor, encode_cursor
from grosh_api.repositories.rate_repo import RateRepo

router = APIRouter(prefix="/rates", tags=["rates"])

_rate_repo = RateRepo()


def get_rate_repo() -> RateRepo:
    return _rate_repo


class RateResponse(BaseModel):
    id: UUID
    source: str
    currency_from: str
    currency_to: str
    rate_buy: Decimal | None
    rate_sell: Decimal | None
    rate_mid: Decimal
    valid_from: datetime
    valid_to: datetime | None
    last_polled_at: datetime | None
    update_cadence_seconds: int | None


@router.get("", status_code=200)
async def list_rates(
    user: Annotated[User, Depends(get_current_user)],
    conn: Annotated[asyncpg.Connection, Depends(get_db_conn)],
    repo: Annotated[RateRepo, Depends(get_rate_repo)],
    source: str | None = Query(None),
    currency_from: str | None = Query(None),
    currency_to: str | None = Query(None),
    from_time: datetime | None = Query(
        None, alias="from", description="Inclusive lower bound (time >= from)."
    ),
    to_time: datetime | None = Query(
        None,
        alias="to",
        description=(
            "Exclusive upper bound (time < to). Pass the start of the next period"
            " to include a full period — e.g. to=2026-02-01T00:00:00Z"
            " for all of January 2026."
        ),
    ),
    limit: int = Query(50, ge=1, le=500),
    cursor: str | None = Query(None),
) -> CursorPage[RateResponse]:
    cursor_valid_from: datetime | None = None
    cursor_id: UUID | None = None
    if cursor is not None:
        try:
            ts_str, id_str = decode_cursor(cursor)
            cursor_valid_from = datetime.fromisoformat(ts_str)
            cursor_id = UUID(id_str)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid cursor.")

    total = await repo.count_rates(
        conn,
        source=source,
        currency_from=currency_from,
        currency_to=currency_to,
        from_time=from_time,
        to_time=to_time,
    )
    rows = await repo.list_rates(
        conn,
        source=source,
        currency_from=currency_from,
        currency_to=currency_to,
        from_time=from_time,
        to_time=to_time,
        cursor_valid_from=cursor_valid_from,
        cursor_id=cursor_id,
        limit=limit,
    )
    items = [
        RateResponse(
            id=row.id,
            source=row.source,
            currency_from=row.currency_from,
            currency_to=row.currency_to,
            rate_buy=row.rate_buy,
            rate_sell=row.rate_sell,
            rate_mid=row.rate_mid,
            valid_from=row.valid_from,
            valid_to=row.valid_to,
            last_polled_at=row.last_polled_at,
            update_cadence_seconds=row.update_cadence_seconds,
        )
        for row in rows
    ]
    next_cursor = None
    if len(items) == limit:
        last = rows[-1]
        next_cursor = encode_cursor(last.valid_from, str(last.id))

    return CursorPage(items=items, total=total, limit=limit, next_cursor=next_cursor)


@router.get("/at", status_code=200)
async def get_rates_at(
    user: Annotated[User, Depends(get_current_user)],
    conn: Annotated[asyncpg.Connection, Depends(get_db_conn)],
    repo: Annotated[RateRepo, Depends(get_rate_repo)],
    at: datetime = Query(
        default_factory=lambda: datetime.now(UTC),
        description=(
            "Point-in-time SCD2 lookup; returns rows where"
            " valid_from <= at AND (valid_to IS NULL OR valid_to > at)."
        ),
    ),
    source: str | None = Query(None),
    currency_from: str | None = Query(None),
    currency_to: str | None = Query(None),
) -> list[RateResponse]:
    """Return all rates that were active at a given timestamp."""
    rows = await repo.get_rates_at(
        conn, at, source=source, currency_from=currency_from, currency_to=currency_to
    )
    return [
        RateResponse(
            id=row.id,
            source=row.source,
            currency_from=row.currency_from,
            currency_to=row.currency_to,
            rate_buy=row.rate_buy,
            rate_sell=row.rate_sell,
            rate_mid=row.rate_mid,
            valid_from=row.valid_from,
            valid_to=row.valid_to,
            last_polled_at=row.last_polled_at,
            update_cadence_seconds=row.update_cadence_seconds,
        )
        for row in rows
    ]
