from decimal import Decimal

from grosh_ingestion.banks.nbu.client import fetch_nbu_rates
from grosh_ingestion.models import NormalizedRate


async def fetch_rates() -> list[NormalizedRate]:
    raw = await fetch_nbu_rates()
    return [
        NormalizedRate(
            source="nbu",
            currency_from=r.cc,
            currency_to="UAH",
            rate_buy=None,
            rate_sell=None,
            rate_mid=Decimal(str(r.rate)),
        )
        for r in raw
    ]
