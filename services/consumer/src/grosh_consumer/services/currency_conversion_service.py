"""Currency conversion for raw transaction events.

Two-tier rate model:

  FRESH   — direct evidence the rate applied at at_time. Only poll-based
            rows can be FRESH. The FRESH predicate is enforced in SQL
            by find_fresh_rate; historical rows never qualify.

  CLOSEST — no FRESH match; fall back to the nearest row by proximity
            to the row's confirmed-live interval, capped at 7 days.

Resolution order (outer to inner):

  for tier in (FRESH, CLOSEST):
      try 1-hop at this tier
      try 2-hop via each pivot at this tier (both legs must share tier)
      if found → return

Within FRESH, 1-hop iteration is source-major (chain order), then
direction-minor (direct before reverse). Within CLOSEST, the repo
ranks across the whole chain at once by row-type-appropriate
proximity, with tie-breaking by chain order then id.

Rate-side selection (liquidation semantics): direct conversion uses
rate_buy, reverse uses rate_sell. Either side NULL falls back to
rate_mid; the opposite side is never substituted (that would invert
the sign of the spread error).

Metadata includes per-step `proximity_seconds`: 0 for FRESH steps
(grace-window proximity is operationally noise when the tier already
marks evidence as direct), and the row's actual distance-to-at_time
for CLOSEST steps.
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
_POLL_INTERVAL_TOLERANCE = 2  # K: missed-poll grace multiplier, passed to repo


class RateTier(IntEnum):
    FRESH = 0
    CLOSEST = 1


class RateSide(StrEnum):
    BUY = "buy"  # bank buys the held currency from the user
    SELL = "sell"  # bank sells the display currency to the user
    MID = "mid"  # fallback: chosen side was NULL on the row


class ConversionResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    amounts: dict[Currency, int | None]
    rate_metadata: dict[str, Any]


@dataclass(frozen=True)
class _RateStep:
    currency_from: str
    currency_to: str
    source: str
    rate_id: int
    rate: Decimal
    rate_side: RateSide
    tier: RateTier
    proximity_seconds: int
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
    def max_proximity_seconds(self) -> int:
        return max(step.proximity_seconds for step in self.steps)

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
        self,
        conn: asyncpg.Connection,
        event: RawTransactionEvent,
        account_currency: str,
    ) -> ConversionResult:
        """Convert event.amount_cents into each display currency (UAH, USD, EUR).

        amount_cents is always in the account's base currency (account_currency),
        not the operation currency. Identity conversions pass through without a
        rate lookup. Missing rates yield None — the transaction still gets inserted.
        """
        entry_source = event.rate_source or event.source
        chain = await self._rate_repo.load_source_chain(conn, entry_source)
        pivots = _ordered_pivots(chain)
        at_time = _ensure_tz(event.time)

        amounts: dict[Currency, int | None] = {}
        rate_metadata: dict[str, Any] = {}

        for target in _DISPLAY_CURRENCIES:
            if account_currency == target:
                amounts[target] = event.amount_cents
                continue

            path = await self._resolve_path(
                conn, account_currency, target, at_time, chain, pivots
            )
            if path is None:
                logger.warning(
                    "No rate path %s->%s for transaction %s",
                    account_currency,
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
        """Outer loop: for each tier (FRESH→CLOSEST), try 1-hop then 2-hop.

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

        # FRESH: walk chain in source priority, direct before reverse.
        for config in chain:
            step = await self._try_fresh(
                conn, leg_from, leg_to, at_time, config, divide=False
            )
            if step is not None:
                return step
            step = await self._try_fresh(
                conn, leg_from, leg_to, at_time, config, divide=True
            )
            if step is not None:
                return step
        return None

    async def _try_fresh(
        self,
        conn: asyncpg.Connection,
        leg_from: str,
        leg_to: str,
        at_time: datetime,
        config: SourceConfig,
        *,
        divide: bool,
    ) -> _RateStep | None:
        """One (source, tier, direction) probe. FRESH only."""
        query_from, query_to = (leg_to, leg_from) if divide else (leg_from, leg_to)
        row = await self._rate_repo.find_fresh_rate(
            conn, config.source, query_from, query_to, at_time, _POLL_INTERVAL_TOLERANCE
        )
        if row is None:
            return None
        # FRESH proximity is reported as 0: tier already signals "direct
        # evidence," and grace-window distance is operationally noise.
        return _step(
            leg_from, leg_to, row, RateTier.FRESH, proximity_seconds=0, divide=divide
        )

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
                "Closest-rate fallback %s->%s at %s"
                " (source=%s, rate_id=%d, proximity=%ds)",
                leg_from,
                leg_to,
                at_time,
                row.source,
                row.id,
                row.proximity_seconds,
            )
            return _step(
                leg_from,
                leg_to,
                row,
                RateTier.CLOSEST,
                proximity_seconds=row.proximity_seconds or 0,
                divide=False,
            )

        row = await self._rate_repo.find_closest_rate(
            conn, leg_to, leg_from, at_time, sources
        )
        if row is not None:
            logger.warning(
                "Closest-rate fallback %s->%s (reverse) at %s"
                " (source=%s, rate_id=%d, proximity=%ds)",
                leg_from,
                leg_to,
                at_time,
                row.source,
                row.id,
                row.proximity_seconds,
            )
            return _step(
                leg_from,
                leg_to,
                row,
                RateTier.CLOSEST,
                proximity_seconds=row.proximity_seconds or 0,
                divide=True,
            )

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
    proximity_seconds: int,
    divide: bool,
) -> _RateStep:
    rate, side = _pick_rate(row, divide=divide)
    return _RateStep(
        currency_from=leg_from,
        currency_to=leg_to,
        source=row.source,
        rate_id=row.id,
        rate=rate,
        rate_side=side,
        tier=tier,
        proximity_seconds=proximity_seconds,
        divide=divide,
    )


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------


def _ordered_pivots(chain: list[SourceConfig]) -> list[str]:
    """Pivot priority: chain depth major, base-array order minor, first-seen wins."""
    seen: set[str] = set()
    result: list[str] = []
    for config in chain:
        for currency in config.base_currencies:
            if currency not in seen:
                seen.add(currency)
                result.append(currency)
    return result


def _path_metadata(path: _RatePath) -> dict[str, Any]:
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
                "proximity_seconds": step.proximity_seconds,
                "op": "divide" if step.divide else "multiply",
            }
            for step in path.steps
        ],
        "effective_rate": str(path.effective_rate),
        "hops": len(path.steps),
        "quality": path.max_tier.name.lower(),
        "max_proximity_seconds": path.max_proximity_seconds,
        "sides": sorted({step.rate_side.value for step in path.steps}),
    }


def _ensure_tz(dt: datetime) -> datetime:
    return dt if dt.tzinfo is not None else dt.replace(tzinfo=UTC)
