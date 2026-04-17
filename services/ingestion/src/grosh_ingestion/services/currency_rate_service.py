import logging

import asyncpg

from grosh_ingestion.models import NormalizedRate
from grosh_ingestion.repositories.currency_rate_repo import CurrencyRateRepo

logger = logging.getLogger(__name__)


class CurrencyRateService:
    def __init__(self, repo: CurrencyRateRepo) -> None:
        self._repo = repo

    async def ingest_rates(
        self, pool: asyncpg.Pool, rates: list[NormalizedRate], source_name: str
    ) -> None:
        async with pool.acquire() as conn:
            for r in rates:
                await self._repo.upsert(
                    conn,
                    r.source,
                    r.currency_from,
                    r.currency_to,
                    r.rate_buy,
                    r.rate_sell,
                    r.rate_mid,
                )
        logger.info("Ingested %d %s currency rates", len(rates), source_name)
