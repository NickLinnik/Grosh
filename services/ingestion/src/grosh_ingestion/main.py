import asyncio
import logging
import os
from collections.abc import AsyncGenerator, Callable, Coroutine
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
from grosh_ingestion.models import NormalizedRate
from grosh_ingestion.repositories.currency_rate_repo import CurrencyRateRepo
from grosh_ingestion.services.currency_rate_service import CurrencyRateService

logger = logging.getLogger(__name__)

RateProvider = Callable[[], Coroutine[None, None, list[NormalizedRate]]]

_RATE_PROVIDERS: list[tuple[RateSource, RateProvider]] = [
    (RateSource.monobank, monobank_fetch_rates),
    (RateSource.nbu, nbu_fetch_rates),
]


async def _currency_rate_loop(
    pool: asyncpg.Pool,
    service: CurrencyRateService,
    interval_seconds: int,
) -> None:
    while True:
        for name, provider in _RATE_PROVIDERS:
            try:
                rates = await provider()
            except Exception:
                logger.exception("Failed to fetch %s currency rates", name)
                continue

            try:
                await service.ingest_rates(pool, rates, name)
            except Exception:
                logger.exception("Failed to write %s currency rates to DB", name)

        await asyncio.sleep(interval_seconds)


@asynccontextmanager
async def lifespan(application: FastAPI) -> AsyncGenerator[None, None]:
    dsn = for_asyncpg(os.environ["DATABASE_URL"])
    application.state.pool = await asyncpg.create_pool(dsn)

    bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
    application.state.producer = Producer({"bootstrap.servers": bootstrap_servers})

    interval = int(os.environ.get("RATE_POLL_INTERVAL_SECONDS", "3600"))
    service = CurrencyRateService(CurrencyRateRepo())

    application.state.currency_task = asyncio.create_task(
        _currency_rate_loop(application.state.pool, service, interval)
    )

    yield

    application.state.currency_task.cancel()
    try:
        await application.state.currency_task
    except asyncio.CancelledError:
        pass
    application.state.producer.flush(timeout=10)
    await application.state.pool.close()


app = FastAPI(title="Grosh Ingestion", version="0.1.0", lifespan=lifespan)
app.include_router(monobank_webhook_router)


@app.get("/health", tags=["ops"], status_code=200)
async def health() -> dict[str, str]:
    return {"status": "ok"}
