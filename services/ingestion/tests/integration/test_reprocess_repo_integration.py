from datetime import UTC, datetime
from uuid import UUID, uuid4

import asyncpg
import pytest

from grosh_ingestion.repositories.reprocess_repo import ReprocessRepo


@pytest.fixture(scope="function")
def repo() -> ReprocessRepo:
    return ReprocessRepo()


async def _insert_user(conn: asyncpg.Connection) -> UUID:
    user_id: UUID = uuid4()
    await conn.execute(
        """
        INSERT INTO users
            (id, email, password_hash, display_name, role)
        VALUES
            ($1, $2, 'x', 'Test User', 'member')
        """,
        user_id,
        f"test-{user_id}@example.com",
    )
    return user_id


@pytest.mark.asyncio
async def test_lock_exists_returns_false_when_no_lock(
    conn: asyncpg.Connection, repo: ReprocessRepo
) -> None:
    user_id = await _insert_user(conn)
    result = await repo.lock_exists(conn, user_id)
    assert result is False


@pytest.mark.asyncio
async def test_lock_exists_returns_true_after_insert(
    conn: asyncpg.Connection, repo: ReprocessRepo
) -> None:
    user_id = await _insert_user(conn)
    await conn.execute(
        "INSERT INTO reprocessing_locks (user_id) VALUES ($1)",
        user_id,
    )
    result = await repo.lock_exists(conn, user_id)
    assert result is True


@pytest.mark.asyncio
async def test_last_reprocess_at_returns_none_when_no_backups(
    conn: asyncpg.Connection, repo: ReprocessRepo
) -> None:
    user_id = await _insert_user(conn)
    result = await repo.last_reprocess_at(conn, user_id)
    assert result is None


@pytest.mark.asyncio
async def test_last_reprocess_at_returns_max_created_at(
    conn: asyncpg.Connection, repo: ReprocessRepo
) -> None:
    user_id = await _insert_user(conn)
    earlier = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
    later = datetime(2025, 6, 15, 8, 30, 0, tzinfo=UTC)

    await conn.execute(
        "INSERT INTO reprocessing_backups (user_id, data, created_at)"
        " VALUES ($1, $2::jsonb, $3)",
        user_id,
        "[]",
        earlier,
    )
    await conn.execute(
        "INSERT INTO reprocessing_backups (user_id, data, created_at)"
        " VALUES ($1, $2::jsonb, $3)",
        user_id,
        "[]",
        later,
    )

    result = await repo.last_reprocess_at(conn, user_id)
    assert result is not None
    assert result == later
