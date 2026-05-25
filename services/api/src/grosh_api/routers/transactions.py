from datetime import UTC, datetime
from typing import Annotated
from uuid import UUID

import asyncpg
from fastapi import APIRouter, Depends, Query
from grosh_shared.domain.models import SpecialCategory, TransactionDirection, User
from grosh_shared.http.errors import ErrorCode, raise_problem

from grosh_api.deps import get_current_user, get_db_conn
from grosh_api.pagination import CursorPage, decode_cursor, encode_cursor
from grosh_api.repositories.settings_repo import SettingsRepo
from grosh_api.repositories.transaction_repo import (
    InvalidUserTimezoneError,
    TransactionRepo,
)
from grosh_api.routers.transactions_models import (
    AggregateField,
    AggregateItem,
    AggregatesResponse,
    Bucket,
    Currency,
    CurrencyAggregate,
    TransactionResponse,
)

router = APIRouter(prefix="/transactions", tags=["transactions"])

_transaction_repo = TransactionRepo()
_settings_repo = SettingsRepo()


def get_transaction_repo() -> TransactionRepo:
    return _transaction_repo


def get_settings_repo() -> SettingsRepo:
    return _settings_repo


@router.get("", status_code=200)
async def list_transactions(
    user: Annotated[User, Depends(get_current_user)],
    conn: Annotated[asyncpg.Connection, Depends(get_db_conn)],
    repo: Annotated[TransactionRepo, Depends(get_transaction_repo)],
    direction: TransactionDirection | None = Query(None),
    category: Annotated[
        list[SpecialCategory] | None,
        Query(
            description=(
                "Whitelist: include only rows whose special_category is in this set."
                " NULL-category rows (ordinary transactions) are NEVER matched by"
                " this filter — use exclude_category to hide categories while"
                " keeping ordinary rows."
                " Repeated-key (e.g. ?category=transfer&category=cancellation)."
            ),
        ),
    ] = None,
    exclude_category: Annotated[
        list[SpecialCategory] | None,
        Query(
            description=(
                "Blacklist: hide rows whose special_category is in this set."
                " NULL-category rows (ordinary transactions) are ALWAYS included."
                " Use this for the main feed (?exclude_category=transfer hides internal"
                " transfers while keeping ordinary income/expense)."
                " Repeated-key."
                " Returns 422 if any value also appears in `category`."
            ),
        ),
    ] = None,
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
            raise_problem(400, ErrorCode.INVALID_CURSOR, "Invalid cursor.")

    if category and exclude_category:
        conflict = set(category) & set(exclude_category)
        if conflict:
            raise_problem(
                422,
                ErrorCode.VALIDATION_ERROR,
                "category and exclude_category cannot contain the same values: "
                f"{sorted(v.value for v in conflict)}",
            )

    total = await repo.count_transactions(
        conn,
        user.id,
        direction=direction,
        category=[c.value for c in category] if category else None,
        exclude_category=[c.value for c in exclude_category]
        if exclude_category
        else None,
        account_id=account_id,
        from_time=from_time,
        to_time=to_time,
    )
    rows = await repo.list_transactions(
        conn,
        user.id,
        direction=direction,
        category=[c.value for c in category] if category else None,
        exclude_category=[c.value for c in exclude_category]
        if exclude_category
        else None,
        account_id=account_id,
        from_time=from_time,
        to_time=to_time,
        cursor_time=cursor_time,
        cursor_id=cursor_id,
        limit=limit + 1,
    )
    has_more = len(rows) > limit
    if has_more:
        rows = rows[:limit]
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
    if has_more:
        last = rows[-1]
        next_cursor = encode_cursor(last.time, str(last.id))

    return CursorPage(items=items, total=total, limit=limit, next_cursor=next_cursor)


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
        raise_problem(422, ErrorCode.INVALID_DATE_RANGE, str(exc))

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
