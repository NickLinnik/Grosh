from dataclasses import dataclass
from datetime import datetime
from uuid import UUID
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import asyncpg

from grosh_api.repositories._filters import build_where


class InvalidUserTimezoneError(ValueError):
    """Raised when user_settings.timezone holds a value ZoneInfo cannot resolve."""


@dataclass(frozen=True)
class AggregateRow:
    period_start: datetime
    uah_income_cents: int
    uah_expense_cents: int
    uah_delta_cents: int
    uah_converted_pct: float
    usd_income_cents: int
    usd_expense_cents: int
    usd_delta_cents: int
    usd_converted_pct: float
    eur_income_cents: int
    eur_expense_cents: int
    eur_delta_cents: int
    eur_converted_pct: float


@dataclass(frozen=True)
class TransactionRow:
    id: UUID
    source_id: str
    user_id: UUID
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
    hold: bool
    direction: str
    special_category: str | None
    counterparty_iban: str | None
    rate_source: str | None
    metadata: dict[str, object] | None
    source: str
    origin: str
    related_transaction_id: UUID | None
    created_at: datetime


class TransactionRepo:
    @staticmethod
    def _row_to_transaction(row: asyncpg.Record) -> TransactionRow:
        return TransactionRow(
            id=row["id"],
            source_id=row["source_id"],
            user_id=row["user_id"],
            account_id=row["account_id"],
            time=row["time"],
            amount_cents=row["amount_cents"],
            operation_amount_cents=row["operation_amount_cents"],
            currency_code=row["currency_code"],
            operation_currency_code=row["operation_currency_code"],
            amount_uah_cents=row["amount_uah_cents"],
            amount_usd_cents=row["amount_usd_cents"],
            amount_eur_cents=row["amount_eur_cents"],
            description=row["description"],
            mcc=row["mcc"],
            cashback_amount_cents=row["cashback_amount_cents"],
            balance_cents=row["balance_cents"],
            hold=row["hold"],
            direction=row["direction"],
            special_category=row["special_category"],
            counterparty_iban=row["counterparty_iban"],
            rate_source=row["rate_source"],
            metadata=row["metadata"],
            source=row["source"],
            origin=row["origin"],
            related_transaction_id=row["related_transaction_id"],
            created_at=row["created_at"],
        )

    @staticmethod
    def _build_transaction_conditions(
        user_id: UUID,
        direction: str | None,
        special_category: str | None,
        account_id: UUID | None,
        from_time: datetime | None,
        to_time: datetime | None,
    ) -> tuple[str, list[object]]:
        return build_where(
            [
                ("user_id =", user_id),
                ("direction =", direction),
                ("special_category =", special_category),
                ("account_id =", account_id),
                ("time >=", from_time),
                ("time <", to_time),
            ]
        )

    async def count_transactions(
        self,
        conn: asyncpg.Connection,
        user_id: UUID,
        *,
        direction: str | None = None,
        special_category: str | None = None,
        account_id: UUID | None = None,
        from_time: datetime | None = None,
        to_time: datetime | None = None,
    ) -> int:
        where_clause, params = self._build_transaction_conditions(
            user_id,
            direction,
            special_category,
            account_id,
            from_time,
            to_time,
        )
        count: int = await conn.fetchval(
            f"SELECT COUNT(*) FROM transactions WHERE {where_clause}",
            *params,
        )
        return count

    async def list_transactions(
        self,
        conn: asyncpg.Connection,
        user_id: UUID,
        *,
        direction: str | None = None,
        special_category: str | None = None,
        account_id: UUID | None = None,
        from_time: datetime | None = None,
        to_time: datetime | None = None,
        cursor_time: datetime | None = None,
        cursor_id: UUID | None = None,
        limit: int = 50,
    ) -> list[TransactionRow]:
        where_clause, params = self._build_transaction_conditions(
            user_id,
            direction,
            special_category,
            account_id,
            from_time,
            to_time,
        )
        if cursor_time is not None and cursor_id is not None:
            idx = len(params) + 1
            where_clause += f" AND (time, id) < (${idx}, ${idx + 1})"
            params.extend([cursor_time, cursor_id])

        limit_idx = len(params) + 1
        query = f"""
            SELECT
                id,
                source_id,
                user_id,
                account_id,
                time,
                amount_cents,
                operation_amount_cents,
                currency_code,
                operation_currency_code,
                amount_uah_cents,
                amount_usd_cents,
                amount_eur_cents,
                description,
                mcc,
                cashback_amount_cents,
                balance_cents,
                hold,
                direction,
                special_category,
                counterparty_iban,
                rate_source,
                metadata,
                source,
                origin,
                related_transaction_id,
                created_at
            FROM transactions
            WHERE {where_clause}
            ORDER BY
                time DESC,
                id DESC
            LIMIT ${limit_idx}
        """
        params.append(limit)

        rows = await conn.fetch(query, *params)
        return [self._row_to_transaction(row) for row in rows]

    async def get_aggregates(
        self,
        conn: asyncpg.Connection,
        user_id: UUID,
        *,
        from_dt: datetime | None = None,
        to_dt: datetime | None = None,
        bucket: str = "month",
        user_tz: str = "UTC",
    ) -> list[AggregateRow]:
        # Defends against corrupt DB writes; PUT /settings is the primary validator.
        # Must run before the query — Postgres uses user_tz inside `AT TIME ZONE` and
        # would otherwise raise asyncpg.InvalidParameterValueError → 500.
        try:
            tz = ZoneInfo(user_tz)
        except (ZoneInfoNotFoundError, ValueError) as exc:
            raise InvalidUserTimezoneError(
                f"Invalid user timezone in DB: {user_tz!r}"
            ) from exc

        query = """
            SELECT
                date_trunc($2, time AT TIME ZONE $3) AS period_start,
                COALESCE(SUM(amount_uah_cents) FILTER (WHERE direction = 'income'),  0) AS uah_income_cents,
                COALESCE(SUM(amount_uah_cents) FILTER (WHERE direction = 'expense'), 0) AS uah_expense_cents,
                COALESCE(SUM(amount_uah_cents) FILTER (WHERE direction = 'income'),  0)
                    - COALESCE(SUM(amount_uah_cents) FILTER (WHERE direction = 'expense'), 0) AS uah_delta_cents,
                ROUND(100.0 * COUNT(amount_uah_cents) / COUNT(*), 1) AS uah_converted_pct,
                COALESCE(SUM(amount_usd_cents) FILTER (WHERE direction = 'income'),  0) AS usd_income_cents,
                COALESCE(SUM(amount_usd_cents) FILTER (WHERE direction = 'expense'), 0) AS usd_expense_cents,
                COALESCE(SUM(amount_usd_cents) FILTER (WHERE direction = 'income'),  0)
                    - COALESCE(SUM(amount_usd_cents) FILTER (WHERE direction = 'expense'), 0) AS usd_delta_cents,
                ROUND(100.0 * COUNT(amount_usd_cents) / COUNT(*), 1) AS usd_converted_pct,
                COALESCE(SUM(amount_eur_cents) FILTER (WHERE direction = 'income'),  0) AS eur_income_cents,
                COALESCE(SUM(amount_eur_cents) FILTER (WHERE direction = 'expense'), 0) AS eur_expense_cents,
                COALESCE(SUM(amount_eur_cents) FILTER (WHERE direction = 'income'),  0)
                    - COALESCE(SUM(amount_eur_cents) FILTER (WHERE direction = 'expense'), 0) AS eur_delta_cents,
                ROUND(100.0 * COUNT(amount_eur_cents) / COUNT(*), 1) AS eur_converted_pct
            FROM transactions
            WHERE user_id = $1
              AND direction IN ('income', 'expense')
              AND special_category IS NULL
              AND time >= COALESCE($4, '-infinity'::timestamptz)
              AND time <  COALESCE($5, 'infinity'::timestamptz)
            GROUP BY period_start
            ORDER BY period_start
        """
        rows = await conn.fetch(query, user_id, bucket, user_tz, from_dt, to_dt)

        result: list[AggregateRow] = []
        for row in rows:
            period_start: datetime = row["period_start"].replace(tzinfo=tz)
            result.append(
                AggregateRow(
                    period_start=period_start,
                    uah_income_cents=row["uah_income_cents"],
                    uah_expense_cents=row["uah_expense_cents"],
                    uah_delta_cents=row["uah_delta_cents"],
                    uah_converted_pct=float(row["uah_converted_pct"]),
                    usd_income_cents=row["usd_income_cents"],
                    usd_expense_cents=row["usd_expense_cents"],
                    usd_delta_cents=row["usd_delta_cents"],
                    usd_converted_pct=float(row["usd_converted_pct"]),
                    eur_income_cents=row["eur_income_cents"],
                    eur_expense_cents=row["eur_expense_cents"],
                    eur_delta_cents=row["eur_delta_cents"],
                    eur_converted_pct=float(row["eur_converted_pct"]),
                )
            )
        return result
