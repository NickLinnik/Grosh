from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any, Protocol
from uuid import UUID

import asyncpg
from confluent_kafka import Producer


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

HistoricalRateProvider = Callable[
    [date, date], Coroutine[None, None, list[NormalizedRate]]
]


@dataclass(frozen=True)
class RateProviderConfig:
    source: str
    fetch: RateProvider
    interval_seconds: int
    kind: RateKind
    fetch_historical: HistoricalRateProvider | None = None


class TransactionBackfillProvider(Protocol):
    async def run_backfill(
        self,
        pool: asyncpg.Pool,
        producer: Producer,
        integration_id: UUID,
        user_id: UUID,
        account_external_id: str,
        from_timestamp: int,
        to_timestamp: int,
    ) -> None: ...


class WebhookReregistrationProvider(Protocol):
    async def reregister_webhooks(
        self,
        conn: asyncpg.Connection,
        webhook_base_url: str,
        encryption_key: str,
    ) -> list[dict[str, Any]]: ...
