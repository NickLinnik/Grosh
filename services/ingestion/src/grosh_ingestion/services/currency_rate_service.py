import logging

import asyncpg

from grosh_ingestion.models import NormalizedRate, RateKind, RateProviderConfig
from grosh_ingestion.repositories.currency_rate_repo import CurrencyRateRepo

logger = logging.getLogger(__name__)


class CurrencyRateService:
    def __init__(self, repo: CurrencyRateRepo) -> None:
        self._repo = repo

    async def ingest_rates(
        self,
        pool: asyncpg.Pool,
        rates: list[NormalizedRate],
        config: RateProviderConfig,
    ) -> None:
        kind = config.kind
        async with pool.acquire() as conn:
            if kind is RateKind.HISTORICAL:
                for r in rates:
                    await self._repo.upsert_historical(
                        conn=conn,
                        source=r.source,
                        currency_from=r.currency_from,
                        currency_to=r.currency_to,
                        rate_buy=r.rate_buy,
                        rate_sell=r.rate_sell,
                        rate_mid=r.rate_mid,
                        at_time=r.at_time,
                    )
            else:
                for r in rates:
                    await self._repo.upsert_polled(
                        conn=conn,
                        source=r.source,
                        currency_from=r.currency_from,
                        currency_to=r.currency_to,
                        rate_buy=r.rate_buy,
                        rate_sell=r.rate_sell,
                        rate_mid=r.rate_mid,
                        update_cadence_seconds=config.interval_seconds,
                        at_time=r.at_time,
                    )
        logger.info("Ingested %d %s %s currency rates", len(rates), kind, config.source)
