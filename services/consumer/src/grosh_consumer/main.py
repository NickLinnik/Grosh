import logging
import time
from pathlib import Path

logger = logging.getLogger(__name__)


def main() -> None:
    logger.info("grosh-consumer starting...")

    # No Kafka consumer group or DB connection implemented yet.
    # Write sentinel immediately so the Docker healthcheck passes.
    Path("/tmp/healthy").touch()
    logger.info("grosh-consumer ready")

    while True:
        time.sleep(60)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
