"""Integration tests for CurrencyRateRepo.load_source_chain.

Section 4.3 — 10 test functions.
"""

import asyncpg
import pytest

from grosh_consumer.repositories.currency_rate_repo import (
    CurrencyRateRepo,
    RateSourceChainError,
)
from tests.integration.helpers import insert_source_config

# ---------------------------------------------------------------------------
# 4.3.1  Single source with no fallback
# ---------------------------------------------------------------------------


async def test_single_source_no_fallback(
    conn: asyncpg.Connection, rate_repo: CurrencyRateRepo
) -> None:
    await insert_source_config(conn, source="nbu", fallback_source=None)

    chain = await rate_repo.load_source_chain(conn, "nbu")

    assert len(chain) == 1
    assert chain[0].source == "nbu"


# ---------------------------------------------------------------------------
# 4.3.2  Two-source chain
# ---------------------------------------------------------------------------


async def test_two_source_chain(
    conn: asyncpg.Connection, rate_repo: CurrencyRateRepo
) -> None:
    await insert_source_config(conn, source="nbu", fallback_source=None)
    await insert_source_config(conn, source="monobank", fallback_source="nbu")

    chain = await rate_repo.load_source_chain(conn, "monobank")

    assert len(chain) == 2
    assert chain[0].source == "monobank"
    assert chain[1].source == "nbu"


# ---------------------------------------------------------------------------
# 4.3.3  Three-source chain
# ---------------------------------------------------------------------------


async def test_three_source_chain(
    conn: asyncpg.Connection, rate_repo: CurrencyRateRepo
) -> None:
    await insert_source_config(conn, source="ecb", fallback_source=None)
    await insert_source_config(conn, source="nbu", fallback_source="ecb")
    await insert_source_config(conn, source="monobank", fallback_source="nbu")

    chain = await rate_repo.load_source_chain(conn, "monobank")

    assert len(chain) == 3
    assert chain[0].source == "monobank"
    assert chain[1].source == "nbu"
    assert chain[2].source == "ecb"


# ---------------------------------------------------------------------------
# 4.3.4  Ordered by depth (entry source first)
# ---------------------------------------------------------------------------


async def test_ordered_by_depth(
    conn: asyncpg.Connection, rate_repo: CurrencyRateRepo
) -> None:
    await insert_source_config(conn, source="ecb", fallback_source=None)
    await insert_source_config(conn, source="nbu", fallback_source="ecb")
    await insert_source_config(conn, source="monobank", fallback_source="nbu")

    chain = await rate_repo.load_source_chain(conn, "monobank")

    sources = [c.source for c in chain]
    assert sources.index("monobank") < sources.index("nbu") < sources.index("ecb")


# ---------------------------------------------------------------------------
# 4.3.5  Chain of exactly depth 10 does not raise
# ---------------------------------------------------------------------------


async def test_chain_of_depth_10_does_not_raise(
    conn: asyncpg.Connection, rate_repo: CurrencyRateRepo
) -> None:
    # Insert leaf-first so FK is satisfied: s10 → NULL, s9 → s10, ..., s1 → s2
    sources = [f"s{i}" for i in range(1, 11)]
    for name in reversed(sources):
        idx = sources.index(name)
        fallback = sources[idx + 1] if idx + 1 < len(sources) else None
        await insert_source_config(conn, source=name, fallback_source=fallback)

    chain = await rate_repo.load_source_chain(conn, "s1")

    assert len(chain) == 10
    assert chain[0].source == "s1"
    assert chain[-1].source == "s10"


# ---------------------------------------------------------------------------
# 4.3.6  Chain exceeding depth cap raises RateSourceChainError
# ---------------------------------------------------------------------------


async def test_chain_exceeding_depth_cap_raises(
    conn: asyncpg.Connection, rate_repo: CurrencyRateRepo
) -> None:
    # Build s1 → s2 → ... → s11 (11 nodes — one beyond cap)
    sources = [f"d{i}" for i in range(1, 12)]
    for name in reversed(sources):
        idx = sources.index(name)
        fallback = sources[idx + 1] if idx + 1 < len(sources) else None
        await insert_source_config(conn, source=name, fallback_source=fallback)

    with pytest.raises(RateSourceChainError):
        await rate_repo.load_source_chain(conn, "d1")


# ---------------------------------------------------------------------------
# 4.3.7  Cyclic chain raises RateSourceChainError
# ---------------------------------------------------------------------------


async def test_cyclic_chain_raises(
    conn: asyncpg.Connection, rate_repo: CurrencyRateRepo
) -> None:
    # Build cycle a → b → c → a using deferred FK update
    # Insert a with no fallback first (satisfies FK for b/c references)
    await insert_source_config(conn, source="ca", fallback_source=None)
    await insert_source_config(conn, source="cb", fallback_source="ca")
    await insert_source_config(conn, source="cc", fallback_source="cb")
    # Close the cycle: a → c
    await conn.execute(
        "UPDATE rate_source_config SET fallback_source = 'cc' WHERE source = 'ca'"
    )

    with pytest.raises(RateSourceChainError) as exc_info:
        await rate_repo.load_source_chain(conn, "ca")

    message = str(exc_info.value)
    assert "ca" in message
    assert "cb" in message
    assert "cc" in message


# ---------------------------------------------------------------------------
# 4.3.8  Unknown entry source returns empty list
# ---------------------------------------------------------------------------


async def test_unknown_entry_source_returns_empty(
    conn: asyncpg.Connection, rate_repo: CurrencyRateRepo
) -> None:
    chain = await rate_repo.load_source_chain(conn, "does_not_exist")

    assert chain == []


# ---------------------------------------------------------------------------
# 4.3.9  base_currencies preserved in order
# ---------------------------------------------------------------------------


async def test_base_currencies_preserved_in_order(
    conn: asyncpg.Connection, rate_repo: CurrencyRateRepo
) -> None:
    await insert_source_config(
        conn,
        source="multi",
        fallback_source=None,
        base_currencies=["UAH", "USD", "EUR"],
    )

    chain = await rate_repo.load_source_chain(conn, "multi")

    assert chain[0].base_currencies == ["UAH", "USD", "EUR"]


# ---------------------------------------------------------------------------
# 4.3.10  max_staleness_seconds round-trips
# ---------------------------------------------------------------------------


async def test_max_staleness_seconds_round_trips(
    conn: asyncpg.Connection, rate_repo: CurrencyRateRepo
) -> None:
    await insert_source_config(
        conn,
        source="slowpoll",
        fallback_source=None,
        max_staleness_seconds=86400,
    )

    chain = await rate_repo.load_source_chain(conn, "slowpoll")

    assert chain[0].max_staleness_seconds == 86400
