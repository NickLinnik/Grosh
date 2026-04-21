"""Unit test fixtures for grosh-consumer, including InMemoryRateRepo."""

from datetime import timedelta
from decimal import Decimal

import pytest

from grosh_consumer.repositories.currency_rate_repo import (
    RateRow,
    RateSourceChainError,
    SourceConfig,
)
from grosh_consumer.services.currency_conversion_service import (
    CurrencyConversionService,
)

_MAX_CLOSEST_RATE_AGE = timedelta(days=7)
_MAX_CHAIN_DEPTH = 10


class InMemoryRateRepo:
    """In-memory implementation of CurrencyRateRepo for unit tests.

    Does not inherit from CurrencyRateRepo and does not touch conn.
    conn is accepted for interface compatibility and ignored.
    """

    def __init__(self):
        self._sources: dict[str, dict] = {}  # source -> {fallback, base_currencies}
        self._rates: list[dict] = []
        self._next_id = 1

    # -- Seed helpers --

    def add_source(self, source, fallback=None, base_currencies=("UAH",)):
        self._sources[source] = {
            "fallback": fallback,
            "base_currencies": list(base_currencies),
        }

    def add_rate(
        self,
        source,
        from_,
        to_,
        *,
        mid,
        buy=None,
        sell=None,
        valid_from,
        valid_to=None,
        polled=None,
        interval=None,
    ):
        self._rates.append(
            {
                "id": self._next_id,
                "source": source,
                "currency_from": from_,
                "currency_to": to_,
                "rate_mid": Decimal(str(mid)),
                "rate_buy": Decimal(str(buy)) if buy is not None else None,
                "rate_sell": Decimal(str(sell)) if sell is not None else None,
                "valid_from": valid_from,
                "valid_to": valid_to,
                "last_polled_at": polled,
                "update_cadence_seconds": interval,
            }
        )
        self._next_id += 1

    # -- Repo interface --

    async def find_fresh_rate(
        self, conn, source, currency_from, currency_to, at_time, poll_tolerance
    ):
        best = None
        for r in self._rates:
            if r["source"] != source:
                continue
            if r["currency_from"] != currency_from or r["currency_to"] != currency_to:
                continue
            if r["last_polled_at"] is None or r["update_cadence_seconds"] is None:
                continue
            if r["valid_from"] > at_time:
                continue
            if r["valid_to"] is not None and r["valid_to"] <= at_time:
                continue
            grace = r["last_polled_at"] + timedelta(
                seconds=poll_tolerance * r["update_cadence_seconds"]
            )
            if grace < at_time:
                continue
            if best is None or r["valid_from"] > best["valid_from"]:
                best = r
        if best is None:
            return None
        return self._to_row(best)

    async def find_closest_rate(
        self, conn, currency_from, currency_to, at_time, sources
    ):
        candidates = []
        for r in self._rates:
            if r["currency_from"] != currency_from or r["currency_to"] != currency_to:
                continue
            if r["source"] not in sources:
                continue
            proximity = self._compute_proximity(r, at_time)
            if proximity is None:
                continue
            if proximity > _MAX_CLOSEST_RATE_AGE.total_seconds():
                continue
            source_idx = sources.index(r["source"])
            candidates.append((proximity, source_idx, r["id"], r))

        if not candidates:
            return None

        candidates.sort(key=lambda x: (x[0], x[1], x[2]))
        _, _, _, best = candidates[0]
        proximity_seconds = int(candidates[0][0])
        return self._to_row(best, proximity_seconds=proximity_seconds)

    async def load_source_chain(self, conn, entry_source):
        if entry_source not in self._sources:
            return []

        chain = []
        visited = set()
        current = entry_source
        while current is not None and len(chain) < _MAX_CHAIN_DEPTH:
            if current not in self._sources:
                break
            if current in visited:
                # Cycle detected — but we only raise if depth cap hit
                chain.append(
                    SourceConfig(
                        source=current,
                        base_currencies=self._sources[current]["base_currencies"],
                    )
                )
                break
            visited.add(current)
            cfg = self._sources[current]
            chain.append(
                SourceConfig(source=current, base_currencies=cfg["base_currencies"])
            )
            current = cfg["fallback"]

        # Mirror production: if we hit depth cap and tail still has a fallback, raise
        if len(chain) == _MAX_CHAIN_DEPTH:
            last_source = chain[-1].source
            if (
                last_source in self._sources
                and self._sources[last_source]["fallback"] is not None
            ):
                raise RateSourceChainError(
                    f"Fallback chain for source={entry_source!r} hit depth cap "
                    f"({_MAX_CHAIN_DEPTH}); possible cycle or misconfiguration. "
                    f"Chain: {[c.source for c in chain]}"
                )

        return chain

    # -- Internal --

    @staticmethod
    def _compute_proximity(r, at_time):
        if r["last_polled_at"] is not None:
            vf = r["valid_from"]
            lp = r["last_polled_at"]
            if vf <= at_time <= lp:
                return 0.0
            if at_time > lp:
                return (at_time - lp).total_seconds()
            return (vf - at_time).total_seconds()
        else:
            return abs((at_time - r["valid_from"]).total_seconds())

    @staticmethod
    def _to_row(r, proximity_seconds=None):
        return RateRow(
            id=r["id"],
            source=r["source"],
            rate_mid=r["rate_mid"],
            rate_buy=r["rate_buy"],
            rate_sell=r["rate_sell"],
            last_polled_at=r["last_polled_at"],
            update_cadence_seconds=r["update_cadence_seconds"],
            valid_from=r["valid_from"],
            valid_to=r["valid_to"],
            proximity_seconds=proximity_seconds,
        )


@pytest.fixture
def repo():
    return InMemoryRateRepo()


@pytest.fixture
def service(repo):
    return CurrencyConversionService(repo)
