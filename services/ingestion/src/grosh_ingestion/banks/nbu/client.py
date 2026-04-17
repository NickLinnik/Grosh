import httpx
from pydantic import BaseModel, ConfigDict, Field

_NBU_URL = "https://bank.gov.ua/NBUStatService/v1/statdirectory/exchange?json"
_TIMEOUT = httpx.Timeout(30.0, connect=5.0)


class NbuRate(BaseModel):
    model_config = ConfigDict(frozen=True, populate_by_name=True)

    r030: int
    txt: str
    rate: float
    cc: str
    exchange_date: str = Field(alias="exchangedate")
    special: str | None = None


async def fetch_nbu_rates() -> list[NbuRate]:
    """Fetch daily exchange rates from the National Bank of Ukraine."""
    async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
        resp = await client.get(_NBU_URL)
        resp.raise_for_status()
        return [NbuRate.model_validate(item) for item in resp.json()]
