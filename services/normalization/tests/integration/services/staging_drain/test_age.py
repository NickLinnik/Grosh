"""Staging-drain age observability — Slice 35.

The sweep loop emits a WARN log line when the oldest staged row exceeds the
5-minute threshold; otherwise it emits a DEBUG line with the same key=value
shape. Empty-table sweeps log only `staged_row_count=0` at DEBUG.

These tests invoke `_sweep_once()` directly and inspect `caplog.records` for
the structured log lines.
"""

import json
import logging
from unittest.mock import MagicMock

import pytest
import pytest_asyncio

from grosh_normalization.repositories.staging_repo import StagingRepo
from grosh_normalization.services.staging_drain_service import (
    STAGING_AGE_WARN_THRESHOLD_SECONDS,
    StagingDrainService,
)
from tests.integration.conftest import SingleConnectionPool, insert_user

pytestmark = pytest.mark.asyncio


@pytest.fixture
def staging_repo() -> StagingRepo:
    return StagingRepo()


@pytest_asyncio.fixture
async def drain(conn, staging_repo):
    """StagingDrainService with a mock Kafka producer + SingleConnectionPool.

    The same fixture used by test_staging_drain_integration.py — duplicated
    here so this module is self-contained.
    """
    instance = StagingDrainService(SingleConnectionPool(conn), staging_repo)
    instance._producer = MagicMock()
    return instance


async def _insert_staged_row_with_age(conn, user_id, *, interval: str) -> None:
    """Insert one staged row with `created_at = now() - interval`.

    The interval string is interpolated into the SQL because asyncpg's
    parameter binding does not accept str for the `interval` type and the
    values are test-controlled literals (not user input).
    """
    await conn.execute(
        f"""
        INSERT INTO staging_normalized_transactions (user_id, payload, created_at)
        VALUES ($1, $2::jsonb, now() - interval '{interval}')
        """,
        user_id,
        json.dumps({"placeholder": "test"}),
    )


async def test_warn_fires_when_oldest_row_is_over_threshold(
    conn,
    drain,
    caplog,
) -> None:
    """A staged row older than 5 minutes triggers a WARN log line."""
    caplog.set_level(
        logging.DEBUG, logger="grosh_normalization.services.staging_drain_service"
    )
    user_id = await insert_user(conn)
    await _insert_staged_row_with_age(conn, user_id, interval="6 minutes")

    await drain._sweep_once()

    warn_records = [
        r
        for r in caplog.records
        if r.levelno == logging.WARNING
        and "staging_drain.oldest_staged_age_seconds=" in r.getMessage()
    ]
    assert warn_records, (
        f"Expected at least one WARN log line containing "
        f"staging_drain.oldest_staged_age_seconds=, got: "
        f"{[r.getMessage() for r in caplog.records]}"
    )
    msg = warn_records[0].getMessage()
    assert "staging_drain.staged_row_count=1" in msg

    # Extract the age value and assert >= 360 (6 minutes = 360 seconds).
    # Format: "oldest_staged_age_seconds=N staging_drain.staged_row_count=1"
    after_marker = msg.split("staging_drain.oldest_staged_age_seconds=", 1)[1]
    age_token = after_marker.split(" ", 1)[0]
    age = int(age_token)
    assert (
        age >= STAGING_AGE_WARN_THRESHOLD_SECONDS + 60
    ), f"Expected age_seconds >= {STAGING_AGE_WARN_THRESHOLD_SECONDS + 60}, got {age}"


async def test_empty_table_logs_debug_with_no_age_field(
    conn,
    drain,
    caplog,
) -> None:
    """An empty staging table sweeps healthily — DEBUG only, no age field."""
    caplog.set_level(
        logging.DEBUG, logger="grosh_normalization.services.staging_drain_service"
    )

    await drain._sweep_once()

    debug_records = [
        r
        for r in caplog.records
        if r.levelno == logging.DEBUG
        and r.getMessage() == "staging_drain.staged_row_count=0"
    ]
    assert debug_records, (
        f"Expected DEBUG line `staging_drain.staged_row_count=0`, got: "
        f"{[(r.levelname, r.getMessage()) for r in caplog.records]}"
    )
    # No WARN, no age field anywhere.
    assert not any(
        "oldest_staged_age_seconds=" in r.getMessage() for r in caplog.records
    ), "Empty table should not emit oldest_staged_age_seconds"


async def test_fresh_row_logs_debug_with_both_fields(
    conn,
    drain,
    caplog,
) -> None:
    """A staged row well below the 5-minute threshold logs both fields at DEBUG."""
    caplog.set_level(
        logging.DEBUG, logger="grosh_normalization.services.staging_drain_service"
    )
    user_id = await insert_user(conn)
    await _insert_staged_row_with_age(conn, user_id, interval="30 seconds")

    await drain._sweep_once()

    debug_records = [
        r
        for r in caplog.records
        if r.levelno == logging.DEBUG
        and "staging_drain.oldest_staged_age_seconds=" in r.getMessage()
        and "staging_drain.staged_row_count=1" in r.getMessage()
    ]
    assert debug_records, (
        f"Expected DEBUG line with both fields, got: "
        f"{[(r.levelname, r.getMessage()) for r in caplog.records]}"
    )
    # No WARN for an under-threshold row.
    assert not any(
        r.levelno == logging.WARNING for r in caplog.records
    ), "Under-threshold row should not emit WARN"
