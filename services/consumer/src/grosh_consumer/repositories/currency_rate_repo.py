from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

import asyncpg


@dataclass(frozen=True)
class RateRow:
    id: int
    source: str
    rate_mid: Decimal
    rate_buy: Decimal | None = None
    rate_sell: Decimal | None = None
    last_polled_at: datetime | None = None


@dataclass(frozen=True)
class SourceConfig:
    source: str
    max_staleness_seconds: int
    base_currencies: list[str]


class RateSourceChainError(RuntimeError):
    """Fallback chain is cyclic, too long, or otherwise misconfigured."""


_MAX_CLOSEST_RATE_AGE_SECONDS = 7 * 86400  # 7 days
_MAX_CHAIN_DEPTH = 10


class CurrencyRateRepo:
    async def find_rate_at_time(
        self,
        conn: asyncpg.Connection,
        source: str,
        currency_from: str,
        currency_to: str,
        at_time: datetime,
    ) -> RateRow | None:
        row = await conn.fetchrow(
            """
            SELECT id, rate_mid, rate_buy, rate_sell, last_polled_at
            FROM currency_rates
            WHERE source = $1
              AND currency_from = $2
              AND currency_to = $3
              AND valid_from <= $4
              AND (valid_to IS NULL OR valid_to > $4)
            ORDER BY valid_from DESC
            LIMIT 1
            """,
            source,
            currency_from,
            currency_to,
            at_time,
        )
        if row is None:
            return None
        return _row(source, row)

    async def find_closest_rate(
        self,
        conn: asyncpg.Connection,
        currency_from: str,
        currency_to: str,
        at_time: datetime,
        sources: list[str],
    ) -> RateRow | None:
        row = await conn.fetchrow(
            """
            SELECT id, source, rate_mid, rate_buy, rate_sell
            FROM currency_rates
            WHERE currency_from = $1
              AND currency_to = $2
              AND source = ANY($3)
              AND last_polled_at IS NOT NULL
              AND (
                  -- past: at_time after confirmed-live interval
                  (last_polled_at < $4
                   AND last_polled_at >= $4 - make_interval(secs => $5))
                  OR
                  -- future row: at_time is before the confirmed-live interval
                  (valid_from > $4 AND valid_from <= $4 + make_interval(secs => $5))
              )
            ORDER BY
                LEAST(
                    ABS(EXTRACT(EPOCH FROM ($4 - last_polled_at))),
                    ABS(EXTRACT(EPOCH FROM (valid_from - $4)))
                )
            LIMIT 1
            """,
            currency_from,
            currency_to,
            sources,
            at_time,
            _MAX_CLOSEST_RATE_AGE_SECONDS,
        )
        if row is None:
            return None
        return _row(row["source"], row)

    async def load_source_chain(
        self,
        conn: asyncpg.Connection,
        entry_source: str,
    ) -> list[SourceConfig]:
        rows = await conn.fetch(
            """
            WITH RECURSIVE chain AS (
                SELECT
                    source,
                    fallback_source,
                    max_staleness_seconds,
                    base_currencies,
                    1 AS depth
                FROM rate_source_config
                WHERE source = $1

                UNION ALL

                SELECT
                    r.source,
                    r.fallback_source,
                    r.max_staleness_seconds,
                    r.base_currencies,
                    c.depth + 1
                FROM rate_source_config r
                JOIN chain c ON r.source = c.fallback_source
                WHERE c.depth < $2
            )
            SELECT
                source,
                fallback_source,
                max_staleness_seconds,
                base_currencies,
                depth
            FROM chain
            ORDER BY depth
            """,
            entry_source,
            _MAX_CHAIN_DEPTH,
        )

        if (
            rows
            and rows[-1]["depth"] == _MAX_CHAIN_DEPTH
            and rows[-1]["fallback_source"] is not None
        ):
            raise RateSourceChainError(
                f"Fallback chain for source={entry_source!r} hit depth cap "
                f"({_MAX_CHAIN_DEPTH}); possible cycle or misconfiguration. "
                f"Chain: {[r['source'] for r in rows]}"
            )

        return [
            SourceConfig(
                source=row["source"],
                max_staleness_seconds=row["max_staleness_seconds"],
                base_currencies=list(row["base_currencies"]),
            )
            for row in rows
        ]


def _row(source: str, row: asyncpg.Record) -> RateRow:
    return RateRow(
        id=row["id"],
        source=source,
        rate_mid=row["rate_mid"],
        rate_buy=row["rate_buy"],
        rate_sell=row["rate_sell"],
        last_polled_at=row.get("last_polled_at"),
    )
