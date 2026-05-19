"""Unit test fixtures for grosh-consumer.

Contains:
- InMemoryRateRepo + repo/service fixtures (currency conversion — unchanged)
- v2 transfer detection helpers: test constants, shadow dataclasses,
  factory functions, and IBAN-lookup stubs.
"""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Literal
from uuid import UUID, uuid4

import pytest

from grosh_consumer.models.normalized import NormalizedTransaction
from grosh_consumer.repositories.currency_rate_repo import (
    RateRow,
    RateSourceChainError,
    SourceConfig,
)
from grosh_consumer.services.currency_conversion_service import (
    CurrencyConversionService,
)
from grosh_consumer.sources.monobank.transfer.flags import RowFlags

_MAX_CLOSEST_RATE_AGE = timedelta(days=7)
_MAX_CHAIN_DEPTH = 10


# ---------------------------------------------------------------------------
# InMemoryRateRepo (unchanged — currency conversion tests)
# ---------------------------------------------------------------------------


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
                "id": UUID(int=self._next_id),
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


# ---------------------------------------------------------------------------
# §1.4  Test constants
# ---------------------------------------------------------------------------

T = datetime(2026, 6, 1, 12, 0, 0, tzinfo=UTC)
T_PLUS_1S = T + timedelta(seconds=1)
T_MINUS_1S = T - timedelta(seconds=1)
T_PLUS_2S = T + timedelta(seconds=2)
T_MINUS_2S = T - timedelta(seconds=2)
T_PLUS_2S_1MS = T + timedelta(seconds=2, milliseconds=1)  # boundary just outside
T_PLUS_3S = T + timedelta(seconds=3)  # outside window

WINDOW_SECONDS = 2
MCC_TRANSFER = "4829"  # MccCode.WIRE_TRANSFER.code
MCC_GROCERY = "5411"


@pytest.fixture
def user_id() -> UUID:
    """Fresh UUID per test — never shared across tests."""
    return uuid4()


# ---------------------------------------------------------------------------
# §1.2  Test-local dataclass shadows
#
# These mirrors the v2 production dataclasses that don't exist yet.
# T7/T11/T12 will replace each one by exposing the real dataclass from
# the production paths; conftest will then import it and the shadow is deleted.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class AccountProps:
    """Test-local shadow of the AccountProps dataclass used by the decision module.

    T7/T11/T12 will replace this by exposing the real dataclass from
    production paths; conftest will then import it and the shadow is deleted.
    """

    type: str
    currency_code: str
    iban: str | None


@dataclass(frozen=True)
class CandidateRow:
    """Test-local shadow of the CandidateRow returned by the universal fetch.

    T7/T11/T12 will replace this by exposing the real dataclass from
    production paths; conftest will then import it and the shadow is deleted.
    """

    id: UUID
    account_id: UUID
    direction: Literal["income", "expense"]
    counterparty_iban: str | None
    description: str | None
    amount_cents: int
    operation_amount_cents: int | None
    time: datetime


# ---------------------------------------------------------------------------
# §1.2  Account fixtures — module-level constants (pure data, no UUID state)
# ---------------------------------------------------------------------------

UAH_FOP = AccountProps(
    type="fop", currency_code="UAH", iban="UA111000000000000000000111"
)
USD_FOP = AccountProps(
    type="fop", currency_code="USD", iban="UA222000000000000000000222"
)
EUR_FOP = AccountProps(
    type="fop", currency_code="EUR", iban="UA333000000000000000000333"
)
UAH_BLACK = AccountProps(
    type="black", currency_code="UAH", iban="UA444000000000000000000444"
)
UAH_WHITE = AccountProps(
    type="white", currency_code="UAH", iban="UA555000000000000000000555"
)
EUR_CARD = AccountProps(
    type="black", currency_code="EUR", iban="UA666000000000000000000666"
)
EXTERNAL = "UA999000000000000000000999"  # never owned by any test user


# ---------------------------------------------------------------------------
# §1.2  Helper factories
# ---------------------------------------------------------------------------


def make_normalized_tx(**overrides) -> NormalizedTransaction:
    """Factory for NormalizedTransaction with sensible MCC-4829 defaults."""
    defaults = dict(
        id=uuid4(),
        source="monobank",
        source_id="src-id",
        user_id=uuid4(),
        account_id=uuid4(),
        time=T,
        amount_cents=10_000,
        operation_amount_cents=10_000,
        operation_currency_code="UAH",
        description=None,
        mcc=MCC_TRANSFER,
        cashback_amount_cents=0,
        balance_cents=None,
        hold=None,
        direction="expense",
        counterparty_iban=None,
        rate_source=None,
        metadata=None,
    )
    return NormalizedTransaction(**{**defaults, **overrides})


def make_row_flags(**overrides) -> RowFlags:
    """Factory for RowFlags with all-False defaults."""
    defaults = dict(
        description_matched=False, multi_hop_description=False, cp_iban_status="null"
    )
    return RowFlags(**{**defaults, **overrides})


def make_candidate(**overrides) -> CandidateRow:
    """Factory for CandidateRow matching an unclaimed unilateral candidate."""
    defaults = dict(
        id=uuid4(),
        account_id=uuid4(),
        direction="income",
        counterparty_iban=None,
        description=None,
        amount_cents=10_000,
        operation_amount_cents=10_000,
        time=T,
    )
    return CandidateRow(**{**defaults, **overrides})


def make_account_props(**overrides) -> AccountProps:
    """Factory for AccountProps."""
    defaults = dict(type="fop", currency_code="UAH", iban="UA111000000000000000000111")
    return AccountProps(**{**defaults, **overrides})


# ---------------------------------------------------------------------------
# §1.2  IBAN-to-account lookup helpers
# ---------------------------------------------------------------------------
# compute_row_flags / is_consistent / classify_pair_evidence take a
# `iban_to_account: dict[str, UUID]` argument directly. Tests build the
# dict per-scenario. Empty `{}` means "no IBANs resolve" (everything is
# unlinked); explicit `{iban: account_id}` means "this IBAN resolves to
# this account." No factory/closure indirection.
