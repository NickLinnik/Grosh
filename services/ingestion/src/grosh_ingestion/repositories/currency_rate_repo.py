from decimal import Decimal

import asyncpg


class CurrencyRateRepo:
    async def upsert(
        self,
        conn: asyncpg.Connection,
        source: str,
        currency_from: str,
        currency_to: str,
        rate_buy: Decimal | None,
        rate_sell: Decimal | None,
        rate_mid: Decimal,
    ) -> None:
        """SCD2 upsert: close the current row if the rate changed, insert a new one."""
        async with conn.transaction():
            current = await conn.fetchrow(
                """
                SELECT id, rate_buy, rate_sell, rate_mid
                FROM currency_rates
                WHERE source = $1
                  AND currency_from = $2
                  AND currency_to = $3
                  AND valid_to IS NULL
                """,
                source,
                currency_from,
                currency_to,
            )

            if current is not None:
                if (
                    current["rate_buy"] == rate_buy
                    and current["rate_sell"] == rate_sell
                    and current["rate_mid"] == rate_mid
                ):
                    return
                await conn.execute(
                    "UPDATE currency_rates SET valid_to = now() WHERE id = $1",
                    current["id"],
                )

            await conn.execute(
                """
                INSERT INTO currency_rates (
                    source, currency_from, currency_to,
                    rate_buy, rate_sell, rate_mid, valid_from
                ) VALUES ($1, $2, $3, $4, $5, $6, now())
                """,
                source,
                currency_from,
                currency_to,
                rate_buy,
                rate_sell,
                rate_mid,
            )
