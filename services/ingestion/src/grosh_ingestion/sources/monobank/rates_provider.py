from datetime import UTC, datetime
from decimal import Decimal

from grosh_shared.iso_4217 import numeric_to_alpha
from grosh_shared.models import RateSource

from grosh_ingestion.models import NormalizedRate
from grosh_ingestion.sources.monobank.client import fetch_currency_rates


async def fetch_rates() -> list[NormalizedRate]:
    raw = await fetch_currency_rates()
    result: list[NormalizedRate] = []
    for r in raw:
        try:
            currency_from = numeric_to_alpha(r.currency_code_a)
            currency_to = numeric_to_alpha(r.currency_code_b)
        except ValueError:
            continue

        buy = Decimal(str(r.rate_buy)) if r.rate_buy is not None else None
        sell = Decimal(str(r.rate_sell)) if r.rate_sell is not None else None

        if r.rate_cross is not None:
            mid = Decimal(str(r.rate_cross))
        elif buy is not None and sell is not None:
            mid = (buy + sell) / 2
        else:
            continue

        result.append(
            NormalizedRate(
                source=RateSource.monobank,
                currency_from=currency_from,
                currency_to=currency_to,
                rate_buy=buy,
                rate_sell=sell,
                rate_mid=mid,
                at_time=datetime.fromtimestamp(r.date, tz=UTC),
            )
        )
    return result
