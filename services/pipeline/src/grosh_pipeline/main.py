import asyncio
import logging

from grosh_pipeline.consumers.pipeline_consumer import run_pipeline_consumer
from grosh_pipeline.db import create_pool
from grosh_pipeline.repositories.account_repo import AccountRepo
from grosh_pipeline.repositories.anomaly_repo import AnomalyRepo
from grosh_pipeline.repositories.currency_rate_repo import CurrencyRateRepo
from grosh_pipeline.repositories.transaction_repo import TransactionRepo
from grosh_pipeline.services.currency_conversion_service import (
    CurrencyConversionService,
)
from grosh_pipeline.services.pipeline import PipelineOrchestrator
from grosh_pipeline.sources.monobank.transfer.detector import MonobankTransferDetection
from grosh_pipeline.sources.monobank.transfer.repo import TransferQueryRepo

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

        await run_pipeline_consumer(pool, orchestrator)
    finally:
        await pool.close()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    logger.info("grosh-pipeline starting")
    asyncio.run(run())


if __name__ == "__main__":
    main()
