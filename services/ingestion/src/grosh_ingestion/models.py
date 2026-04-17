from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class NormalizedRate:
    source: str
    currency_from: str
    currency_to: str
    rate_buy: Decimal | None
    rate_sell: Decimal | None
    rate_mid: Decimal
