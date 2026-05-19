from datetime import UTC, datetime
from enum import StrEnum
from typing import Annotated
from uuid import UUID

import asyncpg
from fastapi import APIRouter, Depends, HTTPException, Query
from grosh_shared.models import TransactionDirection, User
from pydantic import BaseModel, Field

from grosh_api.deps import get_current_user, get_db_conn
from grosh_api.pagination import CursorPage, decode_cursor, encode_cursor
from grosh_api.repositories.settings_repo import SettingsRepo
from grosh_api.repositories.transaction_repo import (
    InvalidUserTimezoneError,
    TransactionRepo,
)

router = APIRouter(prefix="/transactions", tags=["transactions"])

_transaction_repo = TransactionRepo()
_settings_repo = SettingsRepo()


class Currency(StrEnum):
    UAH = "UAH"
    USD = "USD"
    EUR = "EUR"


class AggregateField(StrEnum):
    income = "income"
    expense = "expense"
    delta = "delta"


class Bucket(StrEnum):
    day = "day"
    week = "week"
    month = "month"
    quarter = "quarter"
    year = "year"


class SpecialCategory(StrEnum):
    transfer = "transfer"


def get_transaction_repo() -> TransactionRepo:
    return _transaction_repo


def get_settings_repo() -> SettingsRepo:
    return _settings_repo


class TransactionResponse(BaseModel):
    id: UUID
    account_id: UUID
    time: datetime
    amount_cents: int
    operation_amount_cents: int | None
    currency_code: str
    operation_currency_code: str | None
    amount_uah_cents: int | None
    amount_usd_cents: int | None
    amount_eur_cents: int | None
    description: str | None
    mcc: str | None
    cashback_amount_cents: int
    balance_cents: int | None
    hold: bool | None
    direction: str
    special_category: str | None
    counterparty_iban: str | None
    rate_source: str | None
    metadata: dict[str, object] | None
    source: str
    origin: str
    related_transaction_id: UUID | None


@router.get("", status_code=200)
async def list_transactions(
    user: Annotated[User, Depends(get_current_user)],
    conn: Annotated[asyncpg.Connection, Depends(get_db_conn)],
    repo: Annotated[TransactionRepo, Depends(get_transaction_repo)],
    direction: TransactionDirection | None = Query(None),
    special_category: SpecialCategory | None = Query(None),
    account_id: UUID | None = Query(None),
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
    limit: int = Query(50, ge=1, le=200),
    cursor: str | None = Query(None),
) -> CursorPage[TransactionResponse]:
    cursor_time: datetime | None = None
    cursor_id: UUID | None = None
    if cursor is not None:
        try:
            ts_str, id_str = decode_cursor(cursor)
            cursor_time = datetime.fromisoformat(ts_str)
            cursor_id = UUID(id_str)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid cursor.")

    total = await repo.count_transactions(
        conn,
        user.id,
        direction=direction,
        special_category=special_category,
        account_id=account_id,
        from_time=from_time,
        to_time=to_time,
    )
    rows = await repo.list_transactions(
        conn,
        user.id,
        direction=direction,
        special_category=special_category,
        account_id=account_id,
        from_time=from_time,
        to_time=to_time,
        cursor_time=cursor_time,
        cursor_id=cursor_id,
        limit=limit,
    )
    items = [
        TransactionResponse(
            id=row.id,
            account_id=row.account_id,
            time=row.time,
            amount_cents=row.amount_cents,
            operation_amount_cents=row.operation_amount_cents,
            currency_code=row.currency_code,
            operation_currency_code=row.operation_currency_code,
            amount_uah_cents=row.amount_uah_cents,
            amount_usd_cents=row.amount_usd_cents,
            amount_eur_cents=row.amount_eur_cents,
            description=row.description,
            mcc=row.mcc,
            cashback_amount_cents=row.cashback_amount_cents,
            balance_cents=row.balance_cents,
            hold=row.hold,
            direction=row.direction,
            special_category=row.special_category,
            counterparty_iban=row.counterparty_iban,
            rate_source=row.rate_source,
            metadata=row.metadata,
            source=row.source,
            origin=row.origin,
            related_transaction_id=row.related_transaction_id,
        )
        for row in rows
    ]
    next_cursor = None
    if len(items) == limit:
        last = rows[-1]
        next_cursor = encode_cursor(last.time, str(last.id))

    return CursorPage(items=items, total=total, limit=limit, next_cursor=next_cursor)


class CurrencyAggregate(BaseModel):
    total_income_cents: int | None = None
    total_expense_cents: int | None = None
    delta_cents: int | None = None
    converted_pct: float


class AggregateItem(BaseModel):
    period_start: datetime = Field(
        description=(
            "Bucket start in UTC. The bucket boundary is computed in the user's"
            " timezone (from user_settings.timezone) but serialized as a UTC"
            " ISO timestamp. For a Europe/Kyiv user, the January 2026 bucket"
            " appears as '2025-12-31T22:00:00Z' (= 2026-01-01T00:00 Kyiv)."
        )
    )
    currencies: dict[str, CurrencyAggregate]


class AggregatesResponse(BaseModel):
    bucket: str
    items: list[AggregateItem]


@router.get("/aggregates", status_code=200, response_model_exclude_none=True)
async def get_aggregates(
    user: Annotated[User, Depends(get_current_user)],
    conn: Annotated[asyncpg.Connection, Depends(get_db_conn)],
    repo: Annotated[TransactionRepo, Depends(get_transaction_repo)],
    settings_repo: Annotated[SettingsRepo, Depends(get_settings_repo)],
    currency: list[Currency] = Query(
        default=[Currency.UAH, Currency.USD, Currency.EUR]
    ),
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
    bucket: Bucket = Query(default=Bucket.month),
    fields: list[AggregateField] = Query(
        default=[AggregateField.income, AggregateField.expense, AggregateField.delta]
    ),
) -> AggregatesResponse:
    settings = await settings_repo.get_by_user_id(conn, user.id)
    user_tz = settings.timezone if settings is not None else "UTC"

    try:
        aggregate_rows = await repo.get_aggregates(
            conn,
            user.id,
            from_dt=from_time,
            to_dt=to_time,
            bucket=bucket,
            user_tz=user_tz,
        )
    except InvalidUserTimezoneError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    items: list[AggregateItem] = []
    for row in aggregate_rows:
        period_start_utc = row.period_start.astimezone(UTC)
        currencies: dict[str, CurrencyAggregate] = {}

        for cur in currency:
            cur_lower = cur.lower()
            income = getattr(row, f"{cur_lower}_income_cents")
            expense = getattr(row, f"{cur_lower}_expense_cents")
            delta = getattr(row, f"{cur_lower}_delta_cents")
            converted_pct = getattr(row, f"{cur_lower}_converted_pct")

            currencies[cur] = CurrencyAggregate(
                total_income_cents=income if AggregateField.income in fields else None,
                total_expense_cents=expense
                if AggregateField.expense in fields
                else None,
                delta_cents=delta if AggregateField.delta in fields else None,
                converted_pct=converted_pct,
            )

        items.append(
            AggregateItem(period_start=period_start_utc, currencies=currencies)
        )

    return AggregatesResponse(bucket=bucket, items=items)
