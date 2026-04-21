"""Integration tests for CurrencyRateRepo.load_source_chain.

Tests run against real TimescaleDB. Each test gets a rolled-back transaction.
"""

import pytest

from grosh_consumer.repositories.currency_rate_repo import RateSourceChainError
from tests.integration.helpers import insert_source_config

pytestmark = pytest.mark.asyncio


async def test_single_source_no_fallback(conn, rate_repo):
    await insert_source_config(conn, source="monobank", fallback_source=None)
    chain = await rate_repo.load_source_chain(conn, "monobank")
    assert len(chain) == 1
    assert chain[0].source == "monobank"


async def test_two_source_chain(conn, rate_repo):
    await insert_source_config(conn, source="nbu", fallback_source=None)
    await insert_source_config(conn, source="monobank", fallback_source="nbu")
    chain = await rate_repo.load_source_chain(conn, "monobank")
    assert len(chain) == 2
    assert chain[0].source == "monobank"
    assert chain[1].source == "nbu"


async def test_three_source_chain(conn, rate_repo):
    await insert_source_config(conn, source="c", fallback_source=None)
    await insert_source_config(conn, source="b", fallback_source="c")
    await insert_source_config(conn, source="a", fallback_source="b")
    chain = await rate_repo.load_source_chain(conn, "a")
    assert len(chain) == 3
    assert [c.source for c in chain] == ["a", "b", "c"]


async def test_ordered_by_depth(conn, rate_repo):
    await insert_source_config(conn, source="z", fallback_source=None)
    await insert_source_config(conn, source="y", fallback_source="z")
    await insert_source_config(conn, source="x", fallback_source="y")
    chain = await rate_repo.load_source_chain(conn, "x")
    assert [c.source for c in chain] == ["x", "y", "z"]


async def test_depth_exactly_10_does_not_raise(conn, rate_repo):
    # Insert leaf first, then backwards
    for i in range(10, 0, -1):
        fallback = f"s{i + 1}" if i < 10 else None
        await insert_source_config(conn, source=f"s{i}", fallback_source=fallback)
    chain = await rate_repo.load_source_chain(conn, "s1")
    assert len(chain) == 10


async def test_depth_gt_10_raises_rate_source_chain_error(conn, rate_repo):
    # Insert leaf first, then backwards
    for i in range(11, 0, -1):
        fallback = f"s{i + 1}" if i < 11 else None
        await insert_source_config(conn, source=f"s{i}", fallback_source=fallback)
    with pytest.raises(RateSourceChainError) as exc_info:
        await rate_repo.load_source_chain(conn, "s1")
    msg = str(exc_info.value)
    assert "depth cap" in msg or "10" in msg
    # All 10 sources that fit should be in the message
    for i in range(1, 11):
        assert f"s{i}" in msg


async def test_cyclic_chain_raises_rate_source_chain_error(conn, rate_repo):
    # For cyclic, we need to defer FKs or insert without FK first
    # Insert all three with fallback=NULL, then update to create cycle
    await insert_source_config(conn, source="a", fallback_source=None)
    await insert_source_config(conn, source="b", fallback_source=None)
    await insert_source_config(conn, source="c", fallback_source=None)
    # Now update to create cycle
    await conn.execute(
        "UPDATE rate_source_config SET fallback_source = 'b' WHERE source = 'a'"
    )
    await conn.execute(
        "UPDATE rate_source_config SET fallback_source = 'c' WHERE source = 'b'"
    )
    await conn.execute(
        "UPDATE rate_source_config SET fallback_source = 'a' WHERE source = 'c'"
    )
    with pytest.raises(RateSourceChainError) as exc_info:
        await rate_repo.load_source_chain(conn, "a")
    msg = str(exc_info.value)
    assert "a" in msg
    assert "b" in msg
    assert "c" in msg


async def test_unknown_entry_source_returns_empty(conn, rate_repo):
    chain = await rate_repo.load_source_chain(conn, "nonexistent")
    assert chain == []


async def test_base_currencies_preserves_array_order(conn, rate_repo):
    await insert_source_config(
        conn,
        source="monobank",
        fallback_source=None,
        base_currencies=["EUR", "UAH", "USD"],
    )
    chain = await rate_repo.load_source_chain(conn, "monobank")
    assert chain[0].base_currencies == ["EUR", "UAH", "USD"]
