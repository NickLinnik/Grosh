"""Shared test database lifecycle: create, migrate, drop.

Each test session gets a fresh database named
``grosh_test_{service}_{YYYYMMDD_HHMMSS}``. Migrations run via Alembic
subprocess. The database is dropped on teardown.

Usage in a service's ``tests/integration/conftest.py``::

    from grosh_shared.test_db import create_test_db, drop_test_db

    @pytest_asyncio.fixture(loop_scope="session", scope="session")
    async def db_pool():
        pool, db_name = await create_test_db("consumer")
        yield pool
        await pool.close()
        await drop_test_db(db_name)
"""

import os
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlparse, urlunparse

import asyncpg

from grosh_shared.db_url import for_asyncpg

_MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "services" / "api"


def _base_dsn() -> str:
    raw = os.environ["DATABASE_URL"].replace("@timescaledb:", "@localhost:")
    return for_asyncpg(raw)


def _replace_dbname(dsn: str, new_dbname: str) -> str:
    parsed = urlparse(dsn)
    return urlunparse(parsed._replace(path=f"/{new_dbname}"))


def _maintenance_dsn(dsn: str) -> str:
    return _replace_dbname(dsn, "postgres")


async def create_test_db(service_name: str) -> tuple[asyncpg.Pool, str]:
    """Create a fresh test database, run migrations, return (pool, db_name)."""
    base = _base_dsn()
    timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    db_name = f"grosh_test_{service_name}_{timestamp}"

    maint_conn = await asyncpg.connect(_maintenance_dsn(base))
    try:
        await maint_conn.execute(f'CREATE DATABASE "{db_name}"')
    finally:
        await maint_conn.close()

    test_dsn = _replace_dbname(base, db_name)

    env = {**os.environ, "DATABASE_URL": test_dsn}
    result = subprocess.run(
        ["python", "-m", "alembic", "upgrade", "head"],
        cwd=str(_MIGRATIONS_DIR),
        env=env,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        # Clean up the empty database on migration failure
        cleanup_conn = await asyncpg.connect(_maintenance_dsn(base))
        try:
            await cleanup_conn.execute(f'DROP DATABASE IF EXISTS "{db_name}"')
        finally:
            await cleanup_conn.close()
        raise RuntimeError(f"Alembic migration failed for {db_name}:\n{result.stderr}")

    pool = await asyncpg.create_pool(test_dsn)
    return pool, db_name


async def drop_test_db(db_name: str) -> None:
    """Drop a test database. Safe to call even if the DB doesn't exist."""
    base = _base_dsn()
    conn = await asyncpg.connect(_maintenance_dsn(base))
    try:
        await conn.execute(f'DROP DATABASE IF EXISTS "{db_name}"')
    finally:
        await conn.close()
