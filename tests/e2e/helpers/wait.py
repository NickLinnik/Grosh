"""Polling helpers — for tests that wait on async consumer pipelines.

Use in tests where the assertion target only exists after Kafka publish →
consumer read → DB write completes. Default timeout = 10s.

Pattern:
    tx = await wait_for_transaction(pg, source_id="abc", timeout=5.0)
    assert tx is not None
"""

from __future__ import annotations

import asyncio
import time
from typing import Any
from uuid import UUID

import asyncpg


async def wait_for(
    predicate,
    *,
    timeout: float = 10.0,
    interval: float = 0.1,
    description: str = "predicate",
) -> Any:
    """Poll predicate() until truthy or timeout. Returns predicate's result.

    Raises TimeoutError on timeout with the description in the message.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = await predicate()
        if result:
            return result
        await asyncio.sleep(interval)
    raise TimeoutError(f"Timed out after {timeout}s waiting for: {description}")


async def wait_for_transaction(
    pg: asyncpg.Connection,
    *,
    source_id: str | None = None,
    user_id: UUID | None = None,
    transaction_id: UUID | None = None,
    timeout: float = 10.0,
) -> asyncpg.Record | None:
    """Wait for a transactions row matching the given filters.

    At least one of source_id, user_id, transaction_id must be provided.
    Returns the row when it appears, or raises TimeoutError.
    """
    if not any((source_id, user_id, transaction_id)):
        raise ValueError(
            "must provide at least one of source_id, user_id, transaction_id"
        )

    where_parts = []
    args: list[Any] = []
    if source_id is not None:
        args.append(source_id)
        where_parts.append(f"source_id = ${len(args)}")
    if user_id is not None:
        args.append(user_id)
        where_parts.append(f"user_id = ${len(args)}")
    if transaction_id is not None:
        args.append(transaction_id)
        where_parts.append(f"id = ${len(args)}")
    query = f"SELECT * FROM transactions WHERE {' AND '.join(where_parts)} LIMIT 1"

    async def _check() -> asyncpg.Record | None:
        return await pg.fetchrow(query, *args)

    return await wait_for(
        _check, timeout=timeout, description=f"transaction matching {where_parts}"
    )


async def wait_for_transaction_count(
    pg: asyncpg.Connection,
    *,
    user_id: UUID,
    expected: int,
    timeout: float = 10.0,
) -> int:
    """Wait for COUNT(*) FROM transactions WHERE user_id=$1 to equal `expected`."""

    async def _check() -> int | None:
        n = await pg.fetchval(
            "SELECT count(*) FROM transactions WHERE user_id = $1", user_id
        )
        if n == expected:
            return n
        return None

    return await wait_for(
        _check,
        timeout=timeout,
        description=f"transaction count for {user_id} = {expected}",
    )


async def wait_for_anomaly(
    pg: asyncpg.Connection,
    *,
    transaction_id: UUID | None = None,
    reason_code: str | None = None,
    timeout: float = 10.0,
) -> asyncpg.Record | None:
    """Wait for a transfer_match_anomalies row matching the filters.

    At least one filter must be provided. Returns the row when it appears,
    or raises TimeoutError.
    """
    if not any((transaction_id, reason_code)):
        raise ValueError("must provide at least one of transaction_id, reason_code")

    where_parts: list[str] = []
    args: list[Any] = []
    if transaction_id is not None:
        args.append(transaction_id)
        where_parts.append(f"transaction_id = ${len(args)}")
    if reason_code is not None:
        args.append(reason_code)
        where_parts.append(f"reason_code = ${len(args)}")
    query = (
        "SELECT * FROM transfer_match_anomalies "
        f"WHERE {' AND '.join(where_parts)} LIMIT 1"
    )

    async def _check() -> asyncpg.Record | None:
        return await pg.fetchrow(query, *args)

    return await wait_for(
        _check, timeout=timeout, description=f"anomaly matching {where_parts}"
    )


async def wait_for_account(
    pg: asyncpg.Connection,
    *,
    user_id: UUID,
    external_id: str | None = None,
    timeout: float = 10.0,
) -> asyncpg.Record | None:
    """Wait for an account row matching the filters."""
    where_parts = ["user_id = $1"]
    args: list[Any] = [user_id]
    if external_id is not None:
        args.append(external_id)
        where_parts.append(f"external_id = ${len(args)}")
    query = f"SELECT * FROM accounts WHERE {' AND '.join(where_parts)} LIMIT 1"

    async def _check() -> asyncpg.Record | None:
        return await pg.fetchrow(query, *args)

    return await wait_for(
        _check, timeout=timeout, description=f"account matching {where_parts}"
    )
