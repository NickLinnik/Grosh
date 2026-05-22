"""Re-register webhooks for all active bank integrations.

Reads WEBHOOK_BASE_URL, ENCRYPTION_KEY, and DATABASE_URL from environment.
Iterates all providers in WEBHOOK_REREGISTRATION_PROVIDERS registry.

Invoked by the shell entrypoints (dev.sh / prod.sh), not directly.
"""

import asyncio
import logging
import os

import asyncpg
from grosh_shared.db.url import for_asyncpg

from grosh_ingestion.registry import WEBHOOK_REREGISTRATION_PROVIDERS

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def main() -> None:
    dsn = for_asyncpg(os.environ["DATABASE_URL"])
    webhook_base_url = os.environ["WEBHOOK_BASE_URL"]
    encryption_key = os.environ["ENCRYPTION_KEY"]

    pool = await asyncpg.create_pool(dsn)
    try:
        async with pool.acquire() as conn:
            for source, provider in WEBHOOK_REREGISTRATION_PROVIDERS.items():
                logger.info("[%s] Re-registering webhooks...", source)
                results = await provider.reregister_webhooks(
                    conn=conn,
                    webhook_base_url=webhook_base_url,
                    encryption_key=encryption_key,
                )
                if not results:
                    logger.info("[%s] No active integrations", source)
                for r in results:
                    status = r.get("status", "unknown")
                    iid = r.get("integration_id", "?")
                    if status == "ok":
                        logger.info(
                            "[%s]   ok  %s: %s",
                            source,
                            iid,
                            r.get("webhook_url", ""),
                        )
                    else:
                        logger.error(
                            "[%s]   ERR %s: %s — %s",
                            source,
                            iid,
                            status,
                            r.get("detail", ""),
                        )
    finally:
        await pool.close()

    logger.info("Webhook re-registration complete")


if __name__ == "__main__":
    asyncio.run(main())
