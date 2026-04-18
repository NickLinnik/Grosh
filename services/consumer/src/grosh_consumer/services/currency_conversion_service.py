"""Currency conversion for raw transaction events.

Resolves (currency_from -> currency_to) at a given transaction time into
a rate path using the transaction's source and its configured fallback
chain.

Resolution order (outer to inner):

  For each tier in (FRESH, STALE, CLOSEST):
    1. 1-hop at this tier.
    2. 2-hop via each pivot currency; both legs must resolve at this
       same tier.

Within FRESH/STALE: source-major, direction-minor. The account's bank
is tried before its fallbacks; for each source, a direct lookup is
tried before a reverse-and-divide lookup.

Within CLOSEST: ranked by date distance across the whole chain. Under
degraded conditions, proximity to the transaction time beats source
authority.

Tier semantics:
  FRESH   - validity window covers at_time AND last_polled_at is within
            the source's max_staleness_seconds of at_time.
  STALE   - validity window covers at_time BUT last_polled_at is older.
  CLOSEST - no validity window covers at_time; use nearest rate by
            valid_from distance within the repo's fallback window.

Rate-side selection (liquidation semantics):

  For converting a held currency `leg_from` to a display currency
  `leg_to`, the question is "how much `leg_to` would I realize if I
  liquidated my `leg_from` holding now?" That is the side of the bank's
  quote that moves value *away* from the user.

    divide=False (rate stored as leg_from/leg_to):
        bank buys leg_from from user -> rate_buy
    divide=True (rate stored as leg_to/leg_from):
        bank sells leg_to to user    -> rate_sell

  If the chosen side is NULL (NBU, ECB, or any pair where bid/ask was
  not published), fall back to rate_mid. Never fall through to the
  opposite side - that would invert the sign of the spread error.

  In multi-hop paths, each leg applies the rule independently. A
  PLN -> UAH -> USD path through Monobank compounds the spread twice;
  that is honest (it reflects the true cost of routing through an
  intermediate) but more pessimistic than a direct cross-quote.

Pivots are derived from rate_source_config.base_currencies, ordered by
chain depth (authoritative source first) then array order, first-seen
wins.
"""

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import ROUND_HALF_UP, Decimal
from enum import IntEnum, StrEnum
from typing import Any

import asyncpg
from grosh_shared.events import RawTransactionEvent
from grosh_shared.models import Currency
from pydantic import BaseModel, ConfigDict

from grosh_consumer.repositories.currency_rate_repo import (
    CurrencyRateRepo,
    RateRow,
    SourceConfig,
)

logger = logging.getLogger(__name__)

_DISPLAY_CURRENCIES: tuple[Currency, ...] = (Currency.UAH, Currency.USD, Currency.EUR)
_ZERO = Decimal(0)
_ONE = Decimal(1)


class RateTier(IntEnum):
    """Rate quality tiers, in increasing order of degradation."""

    FRESH = 0
    STALE = 1
    CLOSEST = 2


class RateSide(StrEnum):
    """Which side of the bank's quote was applied."""

    BUY = "buy"  # bank buys the from-currency from the user
    SELL = "sell"  # bank sells the to-currency to the user
    MID = "mid"  # fallback: chosen side was NULL on the row


class ConversionResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    amounts: dict[Currency, int | None]
    rate_metadata: dict[str, Any]


@dataclass(frozen=True)
class _RateStep:
    """One leg of a rate path.

    `currency_from` / `currency_to` are the logical direction of the
    conversion this step represents. If `divide` is True the stored rate
    was for `currency_to -> currency_from` and is inverted when applied.
    """

    currency_from: str
    currency_to: str
    source: str
    rate_id: int
    rate: Decimal
    rate_side: RateSide
    tier: RateTier
    divide: bool = False

    def apply(self, amount: Decimal) -> Decimal:
        return amount / self.rate if self.divide else amount * self.rate


@dataclass(frozen=True)
class _RatePath:
    steps: tuple[_RateStep, ...]

    @property
    def max_tier(self) -> RateTier:
        return max(step.tier for step in self.steps)

    @property
    def effective_rate(self) -> Decimal:
        rate = _ONE
        for step in self.steps:
            rate = step.apply(rate)
        return rate

    def apply(self, amount: Decimal) -> Decimal:
        for step in self.steps:
            amount = step.apply(amount)
        return amount


class CurrencyConversionService:
    def __init__(self, rate_repo: CurrencyRateRepo) -> None:
        self._rate_repo = rate_repo

    async def convert(
        self, conn: asyncpg.Connection, event: RawTransactionEvent
    ) -> ConversionResult:
        """Convert event.amount_cents into each display currency (UAH, USD, EUR).

        Identity conversions (event already in target currency) pass through.
        Missing rates yield None — the transaction still gets inserted.
        """
        chain = await self._rate_repo.load_source_chain(conn, event.source)
        pivots = _ordered_pivots(chain)
        at_time = _ensure_tz(event.time)

        amounts: dict[Currency, int | None] = {}
        rate_metadata: dict[str, Any] = {}

        for target in _DISPLAY_CURRENCIES:
            if event.currency_code == target:
                amounts[target] = event.amount_cents
                continue

            path = await self._resolve_path(
                conn, event.currency_code, target, at_time, chain, pivots
            )
            if path is None:
                logger.warning(
                    "No rate path %s->%s for transaction %s",
                    event.currency_code,
                    target,
                    event.id,
                )
                amounts[target] = None
                continue

            amount = path.apply(Decimal(event.amount_cents))
            amounts[target] = int(amount.quantize(_ZERO, rounding=ROUND_HALF_UP))
            rate_metadata[f"rate_{target.lower()}"] = _path_metadata(path)

        return ConversionResult(amounts=amounts, rate_metadata=rate_metadata)

    # ------------------------------------------------------------------
    # Path resolution
    # ------------------------------------------------------------------

    async def _resolve_path(
        self,
        conn: asyncpg.Connection,
        src: str,
        tgt: str,
        at_time: datetime,
        source_chain: list[SourceConfig],
        pivots: list[str],
    ) -> _RatePath | None:
        """Outer loop: for each tier (FRESH→STALE→CLOSEST), try 1-hop then 2-hop.

        First path found wins — tier-major means freshness beats hop count.
        """
        for tier in RateTier:
            step = await self._resolve_leg(conn, src, tgt, at_time, source_chain, tier)
            if step is not None:
                return _RatePath(steps=(step,))

            for pivot in pivots:
                if pivot == src or pivot == tgt:
                    continue
                left = await self._resolve_leg(
                    conn, src, pivot, at_time, source_chain, tier
                )
                if left is None:
                    continue
                right = await self._resolve_leg(
                    conn, pivot, tgt, at_time, source_chain, tier
                )
                if right is None:
                    continue
                return _RatePath(steps=(left, right))

        return None

    async def _resolve_leg(
        self,
        conn: asyncpg.Connection,
        leg_from: str,
        leg_to: str,
        at_time: datetime,
        chain: list[SourceConfig],
        tier: RateTier,
    ) -> _RateStep | None:
        """Resolve a single leg at exactly `tier`."""
        if tier is RateTier.CLOSEST:
            return await self._resolve_closest_across_chain(
                conn, leg_from, leg_to, at_time, chain
            )

        for config in chain:
            step = await self._try_pair(
                conn, leg_from, leg_to, at_time, config, tier, divide=False
            )
            if step is not None:
                return step
            step = await self._try_pair(
                conn, leg_from, leg_to, at_time, config, tier, divide=True
            )
            if step is not None:
                return step
        return None

    async def _try_pair(
        self,
        conn: asyncpg.Connection,
        leg_from: str,
        leg_to: str,
        at_time: datetime,
        config: SourceConfig,
        tier: RateTier,
        *,
        divide: bool,
    ) -> _RateStep | None:
        """One (source, tier, direction) probe. FRESH or STALE only."""
        query_from, query_to = (leg_to, leg_from) if divide else (leg_from, leg_to)
        row = await self._rate_repo.find_rate_at_time(
            conn, config.source, query_from, query_to, at_time
        )
        if row is None or row.last_polled_at is None:
            return None

        lag = (at_time - _ensure_tz(row.last_polled_at)).total_seconds()
        is_fresh = lag <= config.max_staleness_seconds

        if tier is RateTier.FRESH and not is_fresh:
            return None
        if tier is RateTier.STALE and is_fresh:
            # Already returned at the FRESH iteration for this (source, pair).
            return None

        return _step(leg_from, leg_to, row, tier, divide=divide)

    async def _resolve_closest_across_chain(
        self,
        conn: asyncpg.Connection,
        leg_from: str,
        leg_to: str,
        at_time: datetime,
        chain: list[SourceConfig],
    ) -> _RateStep | None:
        """Last resort: find the nearest rate by date distance (within 7 days).

        Ignores staleness — proximity to transaction time is all that matters.
        Tries direct pair first, then reverse. Logs a warning on every hit.
        """
        sources = [c.source for c in chain]
        row = await self._rate_repo.find_closest_rate(
            conn, leg_from, leg_to, at_time, sources
        )
        if row is not None:
            logger.warning(
                "Closest-rate fallback %s->%s at %s (source=%s, rate_id=%d)",
                leg_from,
                leg_to,
                at_time,
                row.source,
                row.id,
            )
            return _step(leg_from, leg_to, row, RateTier.CLOSEST, divide=False)

        row = await self._rate_repo.find_closest_rate(
            conn, leg_to, leg_from, at_time, sources
        )
        if row is not None:
            logger.warning(
                "Closest-rate fallback %s->%s (reverse) at %s (source=%s, rate_id=%d)",
                leg_from,
                leg_to,
                at_time,
                row.source,
                row.id,
            )
            return _step(leg_from, leg_to, row, RateTier.CLOSEST, divide=True)

        return None


# ----------------------------------------------------------------------
# Rate-side selection
# ----------------------------------------------------------------------


def _pick_rate(row: RateRow, *, divide: bool) -> tuple[Decimal, RateSide]:
    """Pick the liquidation-side rate with mid as fallback.

      divide=False -> rate_buy  (bank buys the held currency from user)
      divide=True  -> rate_sell (bank sells the display currency to user)

    If the chosen side is NULL, fall back to rate_mid. Never substitute
    the opposite side: that would flip the sign of the spread error and
    overstate the user's balance.
    """
    preferred = row.rate_sell if divide else row.rate_buy
    if preferred is not None:
        return preferred, (RateSide.SELL if divide else RateSide.BUY)
    return row.rate_mid, RateSide.MID


def _step(
    leg_from: str,
    leg_to: str,
    row: RateRow,
    tier: RateTier,
    *,
    divide: bool,
) -> _RateStep:
    """Build a _RateStep from a DB row, selecting the liquidation-side rate."""
    rate, side = _pick_rate(row, divide=divide)
    return _RateStep(
        currency_from=leg_from,
        currency_to=leg_to,
        source=row.source,
        rate_id=row.id,
        rate=rate,
        rate_side=side,
        tier=tier,
        divide=divide,
    )


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------


def _ordered_pivots(chain: list[SourceConfig]) -> list[str]:
    """Deterministic pivot order: chain depth, then array order."""
    seen: set[str] = set()
    result: list[str] = []
    for config in chain:
        for currency in config.base_currencies:
            if currency not in seen:
                seen.add(currency)
                result.append(currency)
    return result


def _path_metadata(path: _RatePath) -> dict[str, Any]:
    """Serialize rate path into a JSON-ready audit trail."""
    return {
        "path": [
            {
                "from": step.currency_from,
                "to": step.currency_to,
                "source": step.source,
                "rate_id": step.rate_id,
                "rate": str(step.rate),
                "rate_side": step.rate_side.value,
                "tier": step.tier.name.lower(),
                "op": "divide" if step.divide else "multiply",
            }
            for step in path.steps
        ],
        "effective_rate": str(path.effective_rate),
        "hops": len(path.steps),
        "quality": path.max_tier.name.lower(),
        "sides": sorted({step.rate_side.value for step in path.steps}),
    }


def _ensure_tz(dt: datetime) -> datetime:
    """Normalize to UTC. Naive datetimes are assumed UTC."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)
