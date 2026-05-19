import asyncio
import logging

from grosh_consumer.consumers.normalization_consumer import run_normalization_consumer
from grosh_consumer.consumers.pipeline_consumer import run_pipeline_consumer
from grosh_consumer.db import create_pool
from grosh_consumer.repositories.account_repo import AccountRepo
from grosh_consumer.repositories.anomaly_repo import AnomalyRepo
from grosh_consumer.repositories.currency_rate_repo import CurrencyRateRepo
from grosh_consumer.repositories.staging_repo import StagingRepo
from grosh_consumer.repositories.transaction_repo import TransactionRepo
from grosh_consumer.services.currency_conversion_service import (
    CurrencyConversionService,
)
from grosh_consumer.services.pipeline import PipelineOrchestrator
from grosh_consumer.services.staging_drain_service import StagingDrainService
from grosh_consumer.sources.monobank.transfer.detector import MonobankTransferDetection
from grosh_consumer.sources.monobank.transfer.repo import TransferQueryRepo

logger = logging.getLogger(__name__)


async def run() -> None:
    pool = await create_pool()

    try:
        transaction_repo = TransactionRepo()
        account_repo = AccountRepo()
        rate_repo = CurrencyRateRepo()
        transfer_query_repo = TransferQueryRepo()
        anomaly_repo = AnomalyRepo()

        conversion_service = CurrencyConversionService(rate_repo)
        monobank_transfer = MonobankTransferDetection(
            transfer_repo=transfer_query_repo,
            account_repo=account_repo,
            anomaly_repo=anomaly_repo,
        )

        transfer_strategies = {
            "monobank": monobank_transfer,
        }

        orchestrator = PipelineOrchestrator(
            transaction_repo=transaction_repo,
            account_repo=account_repo,
            conversion=conversion_service,
            anomaly_repo=anomaly_repo,
            transfer_strategies=transfer_strategies,
        )

        staging_repo = StagingRepo()
        async with StagingDrainService(pool, staging_repo):
            await asyncio.gather(
                run_normalization_consumer(pool, staging_repo),
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
