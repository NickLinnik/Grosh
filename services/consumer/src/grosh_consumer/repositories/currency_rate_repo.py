from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal

import asyncpg


@dataclass(frozen=True)
class RateRow:
    id: int
    source: str
    rate_mid: Decimal
    rate_buy: Decimal | None = None
    rate_sell: Decimal | None = None
    last_polled_at: datetime | None = None
    update_cadence_seconds: int | None = None
    valid_from: datetime | None = None
    valid_to: datetime | None = None
    proximity_seconds: int | None = None


@dataclass(frozen=True)
class SourceConfig:
    source: str
    base_currencies: list[str]


class RateSourceChainError(RuntimeError):
    """Fallback chain is cyclic, too long, or otherwise misconfigured."""


_MAX_CLOSEST_RATE_AGE = timedelta(days=7)
_MAX_CHAIN_DEPTH = 10


class CurrencyRateRepo:
    async def find_fresh_rate(
        self,
        conn: asyncpg.Connection,
        source: str,
        currency_from: str,
        currency_to: str,
        at_time: datetime,
        poll_tolerance: int,
    ) -> RateRow | None:
        """Return a FRESH poll-based row for this pair at at_time, or None.

        A row is FRESH when:
          - It is poll-based (last_polled_at and update_cadence_seconds both set).
          - Its SCD2 validity window covers at_time.
          - The grace window (last_polled_at + tolerance * polling_interval)
            extends to or past at_time.

        The SCD2 check matters: a row whose grace window reaches past a
        closure timestamp has been superseded. Trusting it when a
        successor row exists (or would, under correct ingestion) yields
        stale rates. Capping FRESH by valid_to prevents that.

        Historical rows (last_polled_at NULL) are never returned here —
        they resolve at CLOSEST via find_closest_rate.
        """
        row = await conn.fetchrow(
            """
            SELECT
                id,
                rate_mid,
                rate_buy,
                rate_sell,
                last_polled_at,
                update_cadence_seconds,
                valid_from,
                valid_to
            FROM currency_rates
            WHERE
                source = $1
                AND currency_from = $2
                AND currency_to = $3
                AND last_polled_at IS NOT NULL
                AND update_cadence_seconds IS NOT NULL
                AND valid_from <= $4
                AND (valid_to IS NULL OR valid_to > $4)
                AND last_polled_at
                    + (update_cadence_seconds * $5) * INTERVAL '1 second' >= $4
            ORDER BY valid_from DESC
            LIMIT 1
            """,
            source,
            currency_from,
            currency_to,
            at_time,
            poll_tolerance,
        )
        if row is None:
            return None
        return _row(source, row)

    async def find_closest_rate(
        self,
        conn: asyncpg.Connection,
        currency_from: str,
        currency_to: str,
        at_time: datetime,
        sources: list[str],
    ) -> RateRow | None:
        """Return the row with minimum proximity to at_time.

        Proximity against the row's confirmed-live interval:
          - Poll-based row (last_polled_at NOT NULL): [valid_from, last_polled_at].
          - Historical row (last_polled_at NULL): single point valid_from.

        Distance is zero if at_time is inside the interval, otherwise to
        the nearer endpoint. valid_to is never consulted — it is SCD2
        bookkeeping, not a validity-as-proxy claim.

        Eligibility: proximity must be within _MAX_CLOSEST_RATE_AGE.

        Tie-breaking on equal proximity:
          1. Chain order (via array_position against `sources`, which must
             be passed in chain-depth order).
          2. Polled wins over historical (last_polled_at IS NULL sorts last).
          3. Row id (deterministic within the above).

        The returned RateRow carries proximity_seconds.
        """
        row = await conn.fetchrow(
            """
            WITH candidates AS (
                SELECT
                    id,
                    source,
                    rate_mid,
                    rate_buy,
                    rate_sell,
                    last_polled_at,
                    update_cadence_seconds,
                    valid_from,
                    valid_to,
                    CASE
                        WHEN last_polled_at IS NOT NULL THEN
                            CASE
                                WHEN $4 BETWEEN valid_from AND last_polled_at
                                    THEN INTERVAL '0'
                                WHEN $4 > last_polled_at THEN $4 - last_polled_at
                                ELSE valid_from - $4
                            END
                        ELSE GREATEST(valid_from - $4, $4 - valid_from)
                    END AS proximity
                FROM currency_rates
                WHERE
                    currency_from = $1
                    AND currency_to = $2
                    AND source = ANY($3)
            )
            SELECT
                id,
                source,
                rate_mid,
                rate_buy,
                rate_sell,
                last_polled_at,
                update_cadence_seconds,
                valid_from,
                valid_to,
                EXTRACT(EPOCH FROM proximity)::BIGINT AS proximity_seconds
            FROM candidates
            WHERE proximity <= $5
            ORDER BY
                proximity,
                array_position($3::text[], source),
                (last_polled_at IS NULL),
                id
            LIMIT 1
            """,
            currency_from,
            currency_to,
            sources,
            at_time,
            _MAX_CLOSEST_RATE_AGE,
        )
        if row is None:
            return None
        return _row(row["source"], row)

    async def load_source_chain(
        self, conn: asyncpg.Connection, entry_source: str
    ) -> list[SourceConfig]:
        rows = await conn.fetch(
            """
            WITH RECURSIVE chain AS (
                SELECT
                    source,
                    fallback_source,
                    base_currencies,
                    1 AS depth
                FROM rate_source_config
                WHERE source = $1

                UNION ALL

                SELECT
                    r.source,
                    r.fallback_source,
                    r.base_currencies,
                    c.depth + 1
                FROM rate_source_config r
                JOIN chain c ON r.source = c.fallback_source
                WHERE c.depth < $2
            )
            SELECT
                source,
                fallback_source,
                base_currencies,
                depth
            FROM chain
            ORDER BY depth
            """,
            entry_source,
            _MAX_CHAIN_DEPTH,
        )

        if (
            rows
            and rows[-1]["depth"] == _MAX_CHAIN_DEPTH
            and rows[-1]["fallback_source"] is not None
        ):
            raise RateSourceChainError(
                f"Fallback chain for source={entry_source!r} hit depth cap "
                f"({_MAX_CHAIN_DEPTH}); possible cycle or misconfiguration. "
                f"Chain: {[r['source'] for r in rows]}"
            )

        return [
            SourceConfig(
                source=row["source"],
                base_currencies=list(row["base_currencies"]),
            )
            for row in rows
        ]


def _row(source: str, row: asyncpg.Record) -> RateRow:
    def _dec(key: str) -> Decimal | None:
        val = row[key] if key in row else None
        return Decimal(str(val)) if val is not None else None

    def _opt(key: str):
        return row[key] if key in row else None

    return RateRow(
        id=row["id"],
        source=source,
        rate_mid=Decimal(str(row["rate_mid"])),
        rate_buy=_dec("rate_buy"),
        rate_sell=_dec("rate_sell"),
        last_polled_at=_opt("last_polled_at"),
        update_cadence_seconds=_opt("update_cadence_seconds"),
        valid_from=_opt("valid_from"),
        valid_to=_opt("valid_to"),
        proximity_seconds=_opt("proximity_seconds"),
    )
