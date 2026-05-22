import asyncio
import logging
import os
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import asyncpg
from confluent_kafka import Producer
from fastapi import FastAPI
from grosh_shared.db_url import for_asyncpg

from grosh_ingestion.error_handlers import register_all_error_handlers
from grosh_ingestion.models import RateProviderConfig
from grosh_ingestion.registry import RATE_PROVIDERS
from grosh_ingestion.repositories.currency_rate_repo import CurrencyRateRepo
from grosh_ingestion.routers.admin_rates import router as admin_rates_router
from grosh_ingestion.routers.admin_reprocess import router as admin_reprocess_router
from grosh_ingestion.routers.reprocess import router as reprocess_router
from grosh_ingestion.services.backfill_service import BackfillService
from grosh_ingestion.services.currency_rate_service import CurrencyRateService
from grosh_ingestion.services.job_status_service import JobStatusService
from grosh_ingestion.services.reprocess_dispatcher import ReprocessDispatcher
from grosh_ingestion.sources.manual.router import router as manual_router
from grosh_ingestion.sources.monobank.router import (
    lifecycle_router as monobank_lifecycle_router,
)
from grosh_ingestion.sources.monobank.router import (
    webhook_router as monobank_webhook_router,
)

logger = logging.getLogger(__name__)


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

    backfill_service = BackfillService()
    application.state.reprocess_dispatcher = ReprocessDispatcher(
        batch_api=backfill_service._batch_api
    )
    application.state.job_status_service = JobStatusService(
        batch_api=backfill_service._batch_api
    )

    service = CurrencyRateService(CurrencyRateRepo())

    application.state.rate_tasks = [
        asyncio.create_task(_rate_loop(application.state.pool, service, cfg))
        for cfg in RATE_PROVIDERS.values()
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
register_all_error_handlers(app)
app.include_router(monobank_webhook_router)
app.include_router(monobank_lifecycle_router, prefix="/v1")
app.include_router(manual_router, prefix="/v1")
app.include_router(admin_rates_router, prefix="/v1")
app.include_router(reprocess_router, prefix="/v1")
app.include_router(admin_reprocess_router, prefix="/v1")


@app.get("/health", tags=["ops"], status_code=200)
async def health() -> dict[str, str]:
    return {"status": "ok"}
