import asyncio
import logging

from grosh_consumer.consumer import run

logger = logging.getLogger(__name__)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    logger.info("grosh-consumer starting")
    asyncio.run(run())


if __name__ == "__main__":
    main()
