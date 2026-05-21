import asyncio
import logging

from grosh_normalizer.consumers.normalization_consumer import run_normalization_consumer
from grosh_normalizer.db import create_pool
from grosh_normalizer.repositories.staging_repo import StagingRepo
from grosh_normalizer.services.staging_drain_service import StagingDrainService

logger = logging.getLogger(__name__)


async def run() -> None:
    pool = await create_pool()

    try:
        staging_repo = StagingRepo()
        async with StagingDrainService(pool, staging_repo):
            await run_normalization_consumer(pool, staging_repo)
    finally:
        await pool.close()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    logger.info("grosh-normalizer starting")
    asyncio.run(run())


if __name__ == "__main__":
    main()
