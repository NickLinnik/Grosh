"""Seed helpers for integration tests."""

from datetime import datetime
from decimal import Decimal

import asyncpg


async def insert_source_config(
    conn: asyncpg.Connection,
    *,
    source: str,
    fallback_source: str | None = None,
    max_staleness_seconds: int = 300,
    base_currencies: list[str] | None = None,
) -> None:
    await conn.execute(
        """
        INSERT INTO rate_source_config
            (source, fallback_source, max_staleness_seconds, base_currencies)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (source) DO UPDATE
            SET fallback_source       = EXCLUDED.fallback_source,
                max_staleness_seconds = EXCLUDED.max_staleness_seconds,
                base_currencies       = EXCLUDED.base_currencies
        """,
        source,
        fallback_source,
        max_staleness_seconds,
        base_currencies or ["UAH"],
    )


async def insert_rate(
    conn: asyncpg.Connection,
    *,
    source: str,
    currency_from: str,
    currency_to: str,
    rate_mid: float | Decimal,
    rate_buy: float | Decimal | None = None,
    rate_sell: float | Decimal | None = None,
    valid_from: datetime,
    valid_to: datetime | None = None,
    last_polled_at: datetime | None = None,
) -> None:
    await conn.execute(
        """
        INSERT INTO currency_rates
            (source, currency_from, currency_to, rate_mid,
             rate_buy, rate_sell, valid_from, valid_to, last_polled_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        """,
        source,
        currency_from,
        currency_to,
        Decimal(str(rate_mid)),
        Decimal(str(rate_buy)) if rate_buy is not None else None,
        Decimal(str(rate_sell)) if rate_sell is not None else None,
        valid_from,
        valid_to,
        last_polled_at or valid_from,
    )
