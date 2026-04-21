from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import StrEnum


@dataclass(frozen=True)
class NormalizedRate:
    source: str
    currency_from: str
    currency_to: str
    rate_buy: Decimal | None
    rate_sell: Decimal | None
    rate_mid: Decimal
    at_time: datetime


class RateKind(StrEnum):
    POLLED = "polled"
    HISTORICAL = "historical"


RateProvider = Callable[[], Coroutine[None, None, list[NormalizedRate]]]


@dataclass(frozen=True)
class RateProviderConfig:
    source: str
    fetch: RateProvider
    interval_seconds: int
    kind: RateKind
