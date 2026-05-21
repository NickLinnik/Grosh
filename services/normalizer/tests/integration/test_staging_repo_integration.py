"""Integration tests for StagingRepo and normalization-consumer routing logic."""

from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest

from grosh_normalizer.consumers.normalization_consumer import _route
from grosh_normalizer.repositories.staging_repo import StagedRow, StagingRepo
from tests.helpers import make_event
from tests.integration.conftest import insert_user

pytestmark = pytest.mark.asyncio


# ---------------------------------------------------------------------------
# StagingRepo unit operations
# ---------------------------------------------------------------------------


async def test_insert_and_select_staged(conn):
    staging_repo = StagingRepo()
    user_id = await insert_user(conn)
    payload = {"id": str(uuid4()), "source": "monobank", "amount_cents": 500}

    await staging_repo.insert_staged(conn, user_id, payload)

    rows = await staging_repo.select_staged_for_user(conn, user_id)
    assert len(rows) == 1
    assert isinstance(rows[0], StagedRow)
    assert rows[0].payload == payload


async def test_select_staged_ordered_by_created_at(conn):
    staging_repo = StagingRepo()
    user_id = await insert_user(conn)
    payload_a = {"seq": 1}
    payload_b = {"seq": 2}

    await staging_repo.insert_staged(conn, user_id, payload_a)
    await staging_repo.insert_staged(conn, user_id, payload_b)

    rows = await staging_repo.select_staged_for_user(conn, user_id)
    assert len(rows) == 2
    assert rows[0].payload["seq"] == 1
    assert rows[1].payload["seq"] == 2


async def test_delete_staged_removes_row(conn):
    staging_repo = StagingRepo()
    user_id = await insert_user(conn)
    payload = {"x": 1}

    await staging_repo.insert_staged(conn, user_id, payload)
    rows = await staging_repo.select_staged_for_user(conn, user_id)
    assert len(rows) == 1

    await staging_repo.delete_staged(conn, rows[0].id)

    rows_after = await staging_repo.select_staged_for_user(conn, user_id)
    assert rows_after == []


async def test_select_staged_isolates_by_user(conn):
    staging_repo = StagingRepo()
    user_a = await insert_user(conn)
    user_b = await insert_user(conn)

    await staging_repo.insert_staged(conn, user_a, {"owner": "a"})
    await staging_repo.insert_staged(conn, user_b, {"owner": "b"})

    rows_a = await staging_repo.select_staged_for_user(conn, user_a)
    rows_b = await staging_repo.select_staged_for_user(conn, user_b)

    assert len(rows_a) == 1
    assert rows_a[0].payload["owner"] == "a"
    assert len(rows_b) == 1
    assert rows_b[0].payload["owner"] == "b"


# ---------------------------------------------------------------------------
# Routing logic: locked user → stage; unlocked user → publish
# ---------------------------------------------------------------------------


async def test_route_stages_when_user_is_locked(conn):
    """When reprocessing_locks row exists for user, event goes to staging table."""
    staging_repo = StagingRepo()
    user_id = await insert_user(conn)
    normalized = make_event(user_id=user_id)

    # Insert a reprocessing_locks row to simulate an in-progress reprocess.
    await conn.execute(
        "INSERT INTO reprocessing_locks (user_id) VALUES ($1)",
        user_id,
    )

    producer = MagicMock()

    await _route(conn, producer, staging_repo, normalized)

    # Must NOT have published to Kafka.
    producer.produce.assert_not_called()

    # Must have written to the staging table.
    rows = await staging_repo.select_staged_for_user(conn, user_id)
    assert len(rows) == 1
    assert rows[0].payload["id"] == str(normalized.id)


async def test_route_publishes_when_user_is_not_locked(conn):
    """When no reprocessing_locks row, event is published to Kafka."""
    staging_repo = StagingRepo()
    user_id = await insert_user(conn)
    normalized = make_event(user_id=user_id)

    producer = MagicMock()

    with patch(
        "grosh_normalizer.consumers.normalization_consumer._produce"
    ) as mock_produce:
        await _route(conn, producer, staging_repo, normalized)

    mock_produce.assert_called_once_with(producer, normalized)

    # Must NOT have written to the staging table.
    rows = await staging_repo.select_staged_for_user(conn, user_id)
    assert rows == []
