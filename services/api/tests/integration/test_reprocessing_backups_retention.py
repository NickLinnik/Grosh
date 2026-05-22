"""Retention SQL coverage for the reprocessing_backups pg_cron job (migration 0015).

The migration registers a daily DELETE that prunes rows older than 30 days.
This test invokes the DELETE SQL directly rather than waiting for a cron
tick (pytest can't wait 12+ hours for the scheduler). Setup inserts one
row dated 31 days ago and one dated 29 days ago; the DELETE must drop the
first and preserve the second.

The cron-schedule correctness itself (jobname / schedule / command) is
validated separately at deploy time via
`SELECT * FROM cron.job WHERE jobname = 'reprocessing_backups_cleanup'`
and is not duplicated here.
"""

from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import asyncpg
import pytest

_RETENTION_SQL = (
    "DELETE FROM reprocessing_backups WHERE created_at < now() - interval '30 days'"
)


async def _insert_user(conn: asyncpg.Connection) -> UUID:
    user_id = uuid4()
    await conn.execute(
        """
        INSERT INTO users (id, email, password_hash, display_name, role)
        VALUES ($1, $2, 'hash', 'Test User', 'member')
        """,
        user_id,
        f"test-{user_id}@example.com",
    )
    return user_id


@pytest.mark.asyncio
async def test_retention_deletes_rows_older_than_30_days(
    conn: asyncpg.Connection,
) -> None:
    """31-day-old row is deleted; 29-day-old row survives."""
    user_id = await _insert_user(conn)
    backup_id_old = uuid4()
    backup_id_new = uuid4()
    now = datetime.now(UTC)

    await conn.execute(
        """
        INSERT INTO reprocessing_backups (id, user_id, data, created_at)
        VALUES ($1, $2, $3::jsonb, $4)
        """,
        backup_id_old,
        user_id,
        "{}",
        now - timedelta(days=31),
    )
    await conn.execute(
        """
        INSERT INTO reprocessing_backups (id, user_id, data, created_at)
        VALUES ($1, $2, $3::jsonb, $4)
        """,
        backup_id_new,
        user_id,
        "{}",
        now - timedelta(days=29),
    )

    await conn.execute(_RETENTION_SQL)

    old_count = await conn.fetchval(
        "SELECT count(*) FROM reprocessing_backups WHERE id = $1",
        backup_id_old,
    )
    new_count = await conn.fetchval(
        "SELECT count(*) FROM reprocessing_backups WHERE id = $1",
        backup_id_new,
    )
    assert old_count == 0, "Row older than 30 days should have been deleted"
    assert new_count == 1, "Row younger than 30 days should have survived"


@pytest.mark.skip(
    reason=(
        "Requires a separately-launched non-UTC Postgres container; covered "
        "by manual verification. Operator: spin a throwaway postgres "
        "container with PGTZ=EST5EDT, run `alembic upgrade head`, confirm "
        "the upgrade fails with 'pg_cron job reprocessing_backups_cleanup "
        "requires server TZ=UTC'."
    )
)
@pytest.mark.asyncio
async def test_tz_guard_fails_on_non_utc_server() -> None:
    """The migration's TZ guard raises on a non-UTC postgres instance.

    Automating this requires Docker-in-test orchestration to spin a fresh
    postgres container with a non-UTC TZ. That investment is not justified
    for a single fail-fast guard. The test stays here as a documented
    placeholder so the manual verification step is discoverable.
    """
