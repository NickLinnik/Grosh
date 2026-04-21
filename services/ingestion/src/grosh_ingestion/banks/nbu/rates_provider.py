from datetime import UTC, date, datetime
from decimal import Decimal

from grosh_shared.iso_4217 import all_alpha_codes
from grosh_shared.models import Currency, RateSource

from grosh_ingestion.banks.nbu.client import fetch_nbu_historical_rates, fetch_nbu_rates
from grosh_ingestion.models import NormalizedRate


def _parse_nbu_date(s: str) -> datetime:
    """Parse NBU date string 'DD.MM.YYYY' into a UTC datetime at midnight."""
    return datetime.strptime(s, "%d.%m.%Y").replace(tzinfo=UTC)


async def fetch_rates() -> list[NormalizedRate]:
    raw = await fetch_nbu_rates()
    return [
        NormalizedRate(
            source=RateSource.nbu,
            currency_from=r.cc,
            currency_to=Currency.UAH,
            rate_buy=None,
            rate_sell=None,
            rate_mid=Decimal(str(r.rate)),
            at_time=_parse_nbu_date(r.exchange_date),
        )
        for r in raw
    ]


async def fetch_historical_rates(
    from_date: date, to_date: date
) -> list[NormalizedRate]:
    currencies = [c for c in all_alpha_codes() if c != Currency.UAH]
    result: list[NormalizedRate] = []
    for val_code in currencies:
        raw = await fetch_nbu_historical_rates(from_date, to_date, val_code)
        for r in raw:
            result.append(
                NormalizedRate(
                    source=RateSource.nbu,
                    currency_from=r.currency_code_l,
                    currency_to=Currency.UAH,
                    rate_buy=None,
                    rate_sell=None,
                    rate_mid=Decimal(str(r.amount)) / Decimal(str(r.units)),
                    at_time=_parse_nbu_date(r.start_date),
                )
            )
    return result
