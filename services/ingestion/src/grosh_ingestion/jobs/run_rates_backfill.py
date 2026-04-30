"""Standalone backfill script for historical currency rates.

Intended to run as a K8s Job:
    python -m grosh_ingestion.jobs.run_rates_backfill

Required environment variables:
    BACKFILL_SOURCE      Rate source to backfill (must support historical fetch)
    BACKFILL_FROM_DATE   Start date in ISO-8601 format, e.g. "2024-01-01"
    BACKFILL_TO_DATE     End date in ISO-8601 format, e.g. "2025-12-31"
    DATABASE_URL         PostgreSQL connection string
"""

import asyncio
import logging
import os
from datetime import date

import asyncpg
from grosh_shared.db_url import for_asyncpg

from grosh_ingestion.registry import RATE_PROVIDERS
from grosh_ingestion.repositories.currency_rate_repo import CurrencyRateRepo

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

_currency_rate_repo = CurrencyRateRepo()


async def main() -> None:
    source = os.environ["BACKFILL_SOURCE"]
    from_date = date.fromisoformat(os.environ["BACKFILL_FROM_DATE"])
    to_date = date.fromisoformat(os.environ["BACKFILL_TO_DATE"])
    dsn = for_asyncpg(os.environ["DATABASE_URL"])

    config = RATE_PROVIDERS.get(source)
    if config is None:
        raise ValueError(f"Unknown rate source: '{source}'")
    if config.fetch_historical is None:
        raise ValueError(f"Source '{source}' does not support historical rate backfill")

    logger.info("Fetching %s rates from %s to %s", source, from_date, to_date)
    rates = await config.fetch_historical(from_date, to_date)

    if not rates:
        logger.warning(
            "fetch_historical returned 0 rates for %s (%s to %s)"
            " — check date range and source availability",
            source,
            from_date,
            to_date,
        )
        return

    logger.info("Fetched %d historical rates", len(rates))

    pool = await asyncpg.create_pool(dsn)
    try:
        async with pool.acquire() as conn:
            for i, rate in enumerate(rates):
                await _currency_rate_repo.upsert_historical(
                    conn=conn,
                    source=rate.source,
                    currency_from=rate.currency_from,
                    currency_to=rate.currency_to,
                    rate_buy=rate.rate_buy,
                    rate_sell=rate.rate_sell,
                    rate_mid=rate.rate_mid,
                    at_time=rate.at_time,
                )
                if (i + 1) % 500 == 0:
                    logger.info("Upserted %d / %d rates", i + 1, len(rates))

        logger.info("Rate backfill complete: %d rates upserted", len(rates))
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
