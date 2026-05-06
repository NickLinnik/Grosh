from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from uuid import UUID

import asyncpg


@dataclass(frozen=True)
class RateRow:
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


class RateRepo:
    _SELECT = """
        SELECT
            id,
            source,
            currency_from,
            currency_to,
            rate_buy,
            rate_sell,
            rate_mid,
            valid_from,
            valid_to,
            last_polled_at,
            update_cadence_seconds
        FROM currency_rates
    """

    @staticmethod
    def _build_where(
        filters: list[tuple[str, object | None]],
    ) -> tuple[str, list[object]]:
        """Build a WHERE clause from (expression, value) pairs, skipping Nones."""
        active = [(expr, value) for expr, value in filters if value is not None]
        conditions = [f"{expr} ${i + 1}" for i, (expr, _) in enumerate(active)]
        params: list[object] = [value for _, value in active]
        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        return where, params

    @staticmethod
    def _row_to_rate(row: asyncpg.Record) -> RateRow:
        return RateRow(
            id=row["id"],
            source=row["source"],
            currency_from=row["currency_from"],
            currency_to=row["currency_to"],
            rate_buy=row["rate_buy"],
            rate_sell=row["rate_sell"],
            rate_mid=row["rate_mid"],
            valid_from=row["valid_from"],
            valid_to=row["valid_to"],
            last_polled_at=row["last_polled_at"],
            update_cadence_seconds=row["update_cadence_seconds"],
        )

    def _rate_filters(
        self,
        source: str | None,
        currency_from: str | None,
        currency_to: str | None,
        from_time: datetime | None,
        to_time: datetime | None,
    ) -> list[tuple[str, object | None]]:
        return [
            ("source =", source),
            ("currency_from =", currency_from),
            ("currency_to =", currency_to),
            ("valid_from >=", from_time),
            ("valid_from <=", to_time),
        ]

    async def count_rates(
        self,
        conn: asyncpg.Connection,
        *,
        source: str | None = None,
        currency_from: str | None = None,
        currency_to: str | None = None,
        from_time: datetime | None = None,
        to_time: datetime | None = None,
    ) -> int:
        where, params = self._build_where(
            self._rate_filters(source, currency_from, currency_to, from_time, to_time)
        )
        count: int = await conn.fetchval(
            f"SELECT COUNT(*) FROM currency_rates {where}", *params
        )
        return count

    async def list_rates(
        self,
        conn: asyncpg.Connection,
        *,
        source: str | None = None,
        currency_from: str | None = None,
        currency_to: str | None = None,
        from_time: datetime | None = None,
        to_time: datetime | None = None,
        cursor_valid_from: datetime | None = None,
        cursor_id: UUID | None = None,
        limit: int = 50,
    ) -> list[RateRow]:
        where, params = self._build_where(
            self._rate_filters(source, currency_from, currency_to, from_time, to_time)
        )
        if cursor_valid_from is not None and cursor_id is not None:
            idx = len(params) + 1
            cursor_cond = f"(valid_from, id) < (${idx}, ${idx + 1})"
            if where:
                where += f" AND {cursor_cond}"
            else:
                where = f"WHERE {cursor_cond}"
            params.extend([cursor_valid_from, cursor_id])

        limit_idx = len(params) + 1
        query = f"""
            {self._SELECT}
            {where}
            ORDER BY
                valid_from DESC,
                id DESC
            LIMIT ${limit_idx}
        """
        params.append(limit)

        rows = await conn.fetch(query, *params)
        return [self._row_to_rate(row) for row in rows]

    async def get_rates_at(
        self,
        conn: asyncpg.Connection,
        at: datetime,
        *,
        source: str | None = None,
        currency_from: str | None = None,
        currency_to: str | None = None,
    ) -> list[RateRow]:
        where, params = self._build_where(
            [
                ("valid_from <=", at),
                ("source =", source),
                ("currency_from =", currency_from),
                ("currency_to =", currency_to),
            ]
        )
        # SCD2 window: valid_to must be NULL or after the queried timestamp.
        # `at` is always $1 (first non-None filter).
        scd2_condition = "(valid_to IS NULL OR valid_to > $1)"
        where_clause = where.replace("WHERE ", f"WHERE {scd2_condition} AND ")
        if not where:
            where_clause = f"WHERE {scd2_condition}"

        query = f"""
            {self._SELECT}
            {where_clause}
            ORDER BY
                source,
                currency_from,
                currency_to
        """

        rows = await conn.fetch(query, *params)
        return [self._row_to_rate(row) for row in rows]
