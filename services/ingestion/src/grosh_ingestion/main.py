import asyncio
import logging
import os
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import asyncpg
from confluent_kafka import Producer
from fastapi import FastAPI
from grosh_shared.db_url import for_asyncpg
from grosh_shared.models import RateSource

from grosh_ingestion.banks.monobank.rates_provider import (
    fetch_rates as monobank_fetch_rates,
)
from grosh_ingestion.banks.monobank.webhook import router as monobank_webhook_router
from grosh_ingestion.banks.nbu.rates_provider import fetch_rates as nbu_fetch_rates
from grosh_ingestion.models import RateKind, RateProviderConfig
from grosh_ingestion.repositories.currency_rate_repo import CurrencyRateRepo
from grosh_ingestion.services.currency_rate_service import CurrencyRateService

logger = logging.getLogger(__name__)

_RATE_PROVIDERS: list[RateProviderConfig] = [
    RateProviderConfig(
        source=RateSource.monobank,
        fetch=monobank_fetch_rates,
        interval_seconds=300,
        kind=RateKind.POLLED,
    ),
    RateProviderConfig(
        source=RateSource.nbu,
        fetch=nbu_fetch_rates,
        interval_seconds=86400,
        kind=RateKind.HISTORICAL,
    ),
]


async def _rate_loop(
    pool: asyncpg.Pool,
    service: CurrencyRateService,
    config: RateProviderConfig,
) -> None:
    while True:
        try:
            rates = await config.fetch()
        except Exception:
            logger.exception("Failed to fetch %s currency rates", config.source)
        else:
            try:
                await service.ingest_rates(pool, rates, config)
            except Exception:
                logger.exception(
                    "Failed to write %s currency rates to DB", config.source
                )

        await asyncio.sleep(config.interval_seconds)


@asynccontextmanager
async def lifespan(application: FastAPI) -> AsyncGenerator[None, None]:
    dsn = for_asyncpg(os.environ["DATABASE_URL"])
    application.state.pool = await asyncpg.create_pool(dsn)

    bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
    application.state.producer = Producer({"bootstrap.servers": bootstrap_servers})

    service = CurrencyRateService(CurrencyRateRepo())

    application.state.rate_tasks = [
        asyncio.create_task(_rate_loop(application.state.pool, service, cfg))
        for cfg in _RATE_PROVIDERS
    ]

    yield

    for task in application.state.rate_tasks:
        task.cancel()
    for task in application.state.rate_tasks:
        try:
            await task
        except asyncio.CancelledError:
            pass
    application.state.producer.flush(timeout=10)
    await application.state.pool.close()


app = FastAPI(title="Grosh Ingestion", version="0.1.0", lifespan=lifespan)
app.include_router(monobank_webhook_router)


@app.get("/health", tags=["ops"], status_code=200)
async def health() -> dict[str, str]:
    return {"status": "ok"}
