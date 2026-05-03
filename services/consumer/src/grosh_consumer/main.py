import asyncio
import logging

from grosh_consumer.consumers.normalization_consumer import run_normalization_consumer
from grosh_consumer.consumers.pipeline_consumer import run_pipeline_consumer
from grosh_consumer.db import create_pool
from grosh_consumer.repositories.account_repo import AccountRepo
from grosh_consumer.repositories.currency_rate_repo import CurrencyRateRepo
from grosh_consumer.repositories.transaction_repo import TransactionRepo
from grosh_consumer.services.currency_conversion_service import (
    CurrencyConversionService,
)
from grosh_consumer.services.pipeline import PipelineOrchestrator

logger = logging.getLogger(__name__)


async def run() -> None:
    pool = await create_pool()

    try:
        transaction_repo = TransactionRepo()
        account_repo = AccountRepo()
        rate_repo = CurrencyRateRepo()
        conversion_service = CurrencyConversionService(rate_repo)
        orchestrator = PipelineOrchestrator(
            transaction_repo, account_repo, conversion_service
        )

        await asyncio.gather(
            run_normalization_consumer(),
            run_pipeline_consumer(pool, orchestrator),
        )
    finally:
        await pool.close()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    logger.info("grosh-consumer starting")
    asyncio.run(run())


if __name__ == "__main__":
    main()
