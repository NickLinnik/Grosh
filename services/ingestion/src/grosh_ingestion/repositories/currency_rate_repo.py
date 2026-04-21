"""Ingestion-side repository for currency rate writes.

Maintains two independent SCD2 sequences per (source, currency_from, currency_to):
  - Polled rows: produced by scheduled pollers; carry last_polled_at and
    update_cadence_seconds.
  - Historical rows: produced by sources that publish rates for declared
    periods; no polling metadata.

Uniqueness is per type: (source, currency_from, currency_to, valid_from, is_polled),
where is_polled = (last_polled_at IS NOT NULL). The two sequences are free to
overlap each other in time; the consumer's find_closest_rate tie-breaks in
favor of polled rows when proximity is equal.

The two upsert methods never touch rows of the other type.
"""

from datetime import datetime
from decimal import Decimal

import asyncpg


class CurrencyRateRepo:
    async def upsert_polled(
        self,
        conn: asyncpg.Connection,
        source: str,
        currency_from: str,
        currency_to: str,
        rate_buy: Decimal | None,
        rate_sell: Decimal | None,
        rate_mid: Decimal,
        update_cadence_seconds: int,
        at_time: datetime,
    ) -> None:
        """SCD2 upsert for a poll-based observation, anchored at at_time.

        Behavior:
          - If there is an open polled row (valid_to IS NULL) and its rates match:
            bump last_polled_at to at_time; update update_cadence_seconds in case
            the cadence changed.
          - If there is an open polled row with different rates: close it at at_time,
            insert a new open polled row at at_time.
          - If there is no open polled row: insert a new open polled row at at_time.

        Historical rows are ignored entirely — they live in an independent sequence.
        """
        async with conn.transaction():
            current = await conn.fetchrow(
                """
                SELECT id, rate_buy, rate_sell, rate_mid
                FROM currency_rates
                WHERE
                    source = $1
                    AND currency_from = $2
                    AND currency_to = $3
                    AND last_polled_at IS NOT NULL
                    AND valid_to IS NULL
                FOR UPDATE
                """,
                source,
                currency_from,
                currency_to,
            )

            if current is not None and _rates_equal(
                current, rate_buy, rate_sell, rate_mid
            ):
                await conn.execute(
                    """
                    UPDATE currency_rates
                    SET last_polled_at = $2,
                        update_cadence_seconds = $3
                    WHERE id = $1
                    """,
                    current["id"],
                    at_time,
                    update_cadence_seconds,
                )
                return

            if current is not None:
                await conn.execute(
                    "UPDATE currency_rates SET valid_to = $2 WHERE id = $1",
                    current["id"],
                    at_time,
                )

            await conn.execute(
                """
                INSERT INTO currency_rates (
                    source,
                    currency_from,
                    currency_to,
                    rate_buy,
                    rate_sell,
                    rate_mid,
                    valid_from,
                    last_polled_at,
                    update_cadence_seconds
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $7, $8)
                """,
                source,
                currency_from,
                currency_to,
                rate_buy,
                rate_sell,
                rate_mid,
                at_time,
                update_cadence_seconds,
            )

    async def upsert_historical(
        self,
        conn: asyncpg.Connection,
        source: str,
        currency_from: str,
        currency_to: str,
        rate_buy: Decimal | None,
        rate_sell: Decimal | None,
        rate_mid: Decimal,
        at_time: datetime,
    ) -> None:
        """SCD2 upsert for a historical publication with explicit at_time as valid_from.

        Identity key: (source, currency_from, currency_to, valid_from) within
        the historical sequence. If a historical row already exists at this
        valid_from, the upsert is a replacement (same rates → no-op; different
        rates → overwrite in place, leaving valid_to untouched).

        Otherwise, slot into the historical sequence:
          - If valid_from falls inside an existing row's [valid_from, valid_to)
            interval: split that row. The existing row shrinks to end at the
            new valid_from; the new row spans [new_valid_from, old_valid_to).
          - If valid_from is later than all existing historical rows: close the
            currently-open historical row at valid_from (if any), insert new
            open row.
          - If valid_from is earlier than all existing historical rows: insert
            a new row spanning [valid_from, first_existing.valid_from).

        Polled rows are ignored entirely — they live in an independent sequence.
        """
        async with conn.transaction():
            await conn.execute(
                """
                SELECT pg_advisory_xact_lock(
                    hashtextextended($1 || $2 || $3, 0)
                )
                """,
                source,
                currency_from,
                currency_to,
            )

            existing_at_valid_from = await conn.fetchrow(
                """
                SELECT
                    id,
                    rate_buy,
                    rate_sell,
                    rate_mid
                FROM currency_rates
                WHERE
                    source = $1
                    AND currency_from = $2
                    AND currency_to = $3
                    AND last_polled_at IS NULL
                    AND valid_from = $4
                """,
                source,
                currency_from,
                currency_to,
                at_time,
            )

            # CASE 1: Same time window
            if existing_at_valid_from is not None:
                if _rates_equal(existing_at_valid_from, rate_buy, rate_sell, rate_mid):
                    return
                await conn.execute(
                    """
                    UPDATE currency_rates
                    SET
                        rate_buy = $2,
                        rate_sell = $3,
                        rate_mid = $4
                    WHERE id = $1
                    """,
                    existing_at_valid_from["id"],
                    rate_buy,
                    rate_sell,
                    rate_mid,
                )
                return

            # CASE 2: Insert inside another historical span
            covering = await conn.fetchrow(
                """
                SELECT
                    id,
                    valid_from,
                    valid_to
                FROM currency_rates
                WHERE
                    source = $1
                    AND currency_from = $2
                    AND currency_to = $3
                    AND last_polled_at IS NULL
                    AND valid_from < $4
                    AND (valid_to IS NULL OR valid_to > $4)
                """,
                source,
                currency_from,
                currency_to,
                at_time,
            )

            if covering is not None:
                await conn.execute(
                    "UPDATE currency_rates SET valid_to = $2 WHERE id = $1",
                    covering["id"],
                    at_time,
                )
                await conn.execute(
                    """
                    INSERT INTO currency_rates (
                        source,
                        currency_from,
                        currency_to,
                        rate_buy,
                        rate_sell,
                        rate_mid,
                        valid_from,
                        valid_to
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                    """,
                    source,
                    currency_from,
                    currency_to,
                    rate_buy,
                    rate_sell,
                    rate_mid,
                    at_time,
                    covering["valid_to"],
                )
                return

            # CASE 3: Insert at the start of the history
            next_historical = await conn.fetchrow(
                """
                SELECT valid_from
                FROM currency_rates
                WHERE
                    source = $1
                    AND currency_from = $2
                    AND currency_to = $3
                    AND last_polled_at IS NULL
                    AND valid_from > $4
                ORDER BY valid_from ASC
                LIMIT 1
                """,
                source,
                currency_from,
                currency_to,
                at_time,
            )

            if next_historical is not None:
                await conn.execute(
                    """
                    INSERT INTO currency_rates (
                        source,
                        currency_from,
                        currency_to,
                        rate_buy,
                        rate_sell,
                        rate_mid,
                        valid_from,
                        valid_to
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                    """,
                    source,
                    currency_from,
                    currency_to,
                    rate_buy,
                    rate_sell,
                    rate_mid,
                    at_time,
                    next_historical["valid_from"],
                )
                return

            orphan_count = await conn.fetchval(
                """
                SELECT COUNT(*)
                FROM currency_rates
                WHERE
                    source = $1
                    AND currency_from = $2
                    AND currency_to = $3
                    AND last_polled_at IS NULL
                """,
                source,
                currency_from,
                currency_to,
            )
            if orphan_count:
                raise RuntimeError(
                    f"upsert_historical invariant violated: no covering or successor "
                    f"row found for ({source}, {currency_from}, {currency_to}, "
                    f"valid_from={at_time}), yet {orphan_count} historical row(s) "
                    f"exist for this pair. This indicates a bug in the slot-in logic."
                )
            await conn.execute(
                """
                INSERT INTO currency_rates (
                    source,
                    currency_from,
                    currency_to,
                    rate_buy,
                    rate_sell,
                    rate_mid,
                    valid_from,
                    valid_to
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, NULL)
                """,
                source,
                currency_from,
                currency_to,
                rate_buy,
                rate_sell,
                rate_mid,
                at_time,
            )


def _rates_equal(
    row: asyncpg.Record,
    rate_buy: Decimal | None,
    rate_sell: Decimal | None,
    rate_mid: Decimal,
) -> bool:
    return (
        row["rate_buy"] == rate_buy
        and row["rate_sell"] == rate_sell
        and row["rate_mid"] == rate_mid
    )
