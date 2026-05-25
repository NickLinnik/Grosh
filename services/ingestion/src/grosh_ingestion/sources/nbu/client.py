from datetime import date

import httpx
from pydantic import BaseModel, ConfigDict, Field

_NBU_URL = "https://bank.gov.ua/NBUStatService/v1/statdirectory/exchange?json"
_NBU_HISTORICAL_URL = "https://bank.gov.ua/NBU_Exchange/exchange_site"
_TIMEOUT = httpx.Timeout(30.0, connect=5.0)


class NbuRate(BaseModel):
    model_config = ConfigDict(frozen=True, populate_by_name=True)

    r030: int
    txt: str
    rate: float
    cc: str
    exchange_date: str = Field(alias="exchangedate")
    special: str | None = None


class NbuHistoricalRate(BaseModel):
    model_config = ConfigDict(frozen=True, populate_by_name=True)

    start_date: str = Field(alias="exchangedate")
    currency_code_l: str = Field(alias="cc")
    units: int = Field(alias="units")
    amount: float = Field(alias="rate_per_unit")


async def fetch_nbu_rates() -> list[NbuRate]:
    """Fetch daily exchange rates from the National Bank of Ukraine."""
    async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
        resp = await client.get(_NBU_URL)
        resp.raise_for_status()
        return [NbuRate.model_validate(item) for item in resp.json()]


async def fetch_nbu_historical_rates(
    from_date: date, to_date: date, val_code: str
) -> list[NbuHistoricalRate]:
    params = {
        "start": from_date.strftime("%Y%m%d"),
        "end": to_date.strftime("%Y%m%d"),
        "valcode": val_code,
        "json": "",
    }
    async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
        resp = await client.get(_NBU_HISTORICAL_URL, params=params)
        resp.raise_for_status()
        return [NbuHistoricalRate.model_validate(item) for item in resp.json()]
