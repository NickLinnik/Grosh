import os

import asyncpg
from grosh_shared.db_url import for_asyncpg


async def create_pool() -> asyncpg.Pool:
    dsn = for_asyncpg(os.environ["DATABASE_URL"])
    return await asyncpg.create_pool(dsn, min_size=1, max_size=5, command_timeout=30)
