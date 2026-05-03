from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

import asyncpg


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
    mcc: int | None
    cashback_amount_cents: int
    balance_cents: int | None
    hold: bool
    raw_transaction_type: str
    transaction_type: str
    counterparty_iban: str | None
    metadata: dict[str, object] | None
    source: str
    origin: str
    related_transaction_id: UUID | None
    created_at: datetime


@dataclass(frozen=True)
class MonthlyAggregateRow:
    month: datetime
    user_id: UUID
    total_income_uah_cents: int
    total_expense_uah_cents: int
    delta_uah_cents: int
    total_income_usd_cents: int
    total_expense_usd_cents: int
    delta_usd_cents: int
    total_income_eur_cents: int
    total_expense_eur_cents: int
    delta_eur_cents: int
    null_uah_count: int
    null_usd_count: int
    null_eur_count: int


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
            raw_transaction_type=row["raw_transaction_type"],
            transaction_type=row["transaction_type"],
            counterparty_iban=row["counterparty_iban"],
            metadata=row["metadata"],
            source=row["source"],
            origin=row["origin"],
            related_transaction_id=row["related_transaction_id"],
            created_at=row["created_at"],
        )

    @staticmethod
    def _row_to_aggregate(row: asyncpg.Record) -> MonthlyAggregateRow:
        return MonthlyAggregateRow(
            month=row["month"],
            user_id=row["user_id"],
            total_income_uah_cents=row["total_income_uah_cents"],
            total_expense_uah_cents=row["total_expense_uah_cents"],
            delta_uah_cents=row["delta_uah_cents"],
            total_income_usd_cents=row["total_income_usd_cents"],
            total_expense_usd_cents=row["total_expense_usd_cents"],
            delta_usd_cents=row["delta_usd_cents"],
            total_income_eur_cents=row["total_income_eur_cents"],
            total_expense_eur_cents=row["total_expense_eur_cents"],
            delta_eur_cents=row["delta_eur_cents"],
            null_uah_count=row["null_uah_count"],
            null_usd_count=row["null_usd_count"],
            null_eur_count=row["null_eur_count"],
        )

    @staticmethod
    def _build_transaction_conditions(
        user_id: UUID,
        transaction_type: str | None,
        account_id: UUID | None,
        from_time: datetime | None,
        to_time: datetime | None,
    ) -> tuple[str, list[object]]:
        conditions: list[str] = ["user_id = $1"]
        params: list[object] = [user_id]
        param_idx = 2

        if transaction_type is not None:
            conditions.append(f"transaction_type = ${param_idx}")
            params.append(transaction_type)
            param_idx += 1

        if account_id is not None:
            conditions.append(f"account_id = ${param_idx}")
            params.append(account_id)
            param_idx += 1

        if from_time is not None:
            conditions.append(f"time >= ${param_idx}")
            params.append(from_time)
            param_idx += 1

        if to_time is not None:
            conditions.append(f"time <= ${param_idx}")
            params.append(to_time)
            param_idx += 1

        return " AND ".join(conditions), params

    async def count_transactions(
        self,
        conn: asyncpg.Connection,
        user_id: UUID,
        *,
        transaction_type: str | None = None,
        account_id: UUID | None = None,
        from_time: datetime | None = None,
        to_time: datetime | None = None,
    ) -> int:
        where_clause, params = self._build_transaction_conditions(
            user_id, transaction_type, account_id, from_time, to_time
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
        transaction_type: str | None = None,
        account_id: UUID | None = None,
        from_time: datetime | None = None,
        to_time: datetime | None = None,
        cursor_time: datetime | None = None,
        cursor_id: UUID | None = None,
        limit: int = 50,
    ) -> list[TransactionRow]:
        where_clause, params = self._build_transaction_conditions(
            user_id, transaction_type, account_id, from_time, to_time
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
                raw_transaction_type,
                transaction_type,
                counterparty_iban,
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

    async def get_monthly_aggregates(
        self,
        conn: asyncpg.Connection,
        user_id: UUID,
    ) -> list[MonthlyAggregateRow]:
        rows = await conn.fetch(
            """
            SELECT
                month,
                user_id,
                total_income_uah_cents,
                total_expense_uah_cents,
                delta_uah_cents,
                total_income_usd_cents,
                total_expense_usd_cents,
                delta_usd_cents,
                total_income_eur_cents,
                total_expense_eur_cents,
                delta_eur_cents,
                null_uah_count,
                null_usd_count,
                null_eur_count
            FROM monthly_aggregates
            WHERE user_id = $1
            ORDER BY month DESC
            """,
            user_id,
        )
        return [self._row_to_aggregate(row) for row in rows]
