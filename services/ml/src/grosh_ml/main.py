import logging
import time
from pathlib import Path

logger = logging.getLogger(__name__)


def main() -> None:
    logger.info("grosh-ml starting...")

    # No DB connection or model initialization implemented yet.
    # Write sentinel immediately so the Docker healthcheck passes.
    Path("/tmp/healthy").touch()
    logger.info("grosh-ml ready")

    while True:
        time.sleep(60)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
