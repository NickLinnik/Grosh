"""End-to-end integration tests for CurrencyRateService.ingest_rates.

The service acquires its own connection from the pool and commits internally,
so each test uses a unique currency_from code to avoid cross-test pollution,
then deletes by (source, currency_from, currency_to) in teardown.
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import asyncpg
from grosh_shared.domain.models import RateSource

from grosh_ingestion.models import NormalizedRate, RateKind, RateProviderConfig
from grosh_ingestion.services.currency_rate_service import CurrencyRateService

T = datetime(2025, 6, 1, 12, 0, tzinfo=UTC)
T_minus_5m = T - timedelta(minutes=5)

_TO = "UAH"


# Dummy RateProvider callable used only to satisfy the dataclass field type.
async def _noop_fetch() -> list[NormalizedRate]:
    return []


def _polled_config(
    source: str = RateSource.monobank, interval: int = 300
) -> RateProviderConfig:
    return RateProviderConfig(
        source=source,
        fetch=_noop_fetch,
        interval_seconds=interval,
        kind=RateKind.POLLED,
    )


def _historical_config(source: str = RateSource.nbu) -> RateProviderConfig:
    return RateProviderConfig(
        source=source,
        fetch=_noop_fetch,
        interval_seconds=0,
        kind=RateKind.HISTORICAL,
    )


async def _query_rows(
    pool: asyncpg.Pool,
    *,
    source: str,
    currency_from: str,
    currency_to: str,
    kind: str | None = None,
) -> list[asyncpg.Record]:
    if kind == "polled":
        kind_filter = "AND last_polled_at IS NOT NULL"
    elif kind == "historical":
        kind_filter = "AND last_polled_at IS NULL"
    else:
        kind_filter = ""
    async with pool.acquire() as c:
        return await c.fetch(
            f"""
            SELECT * FROM currency_rates
            WHERE source = $1
              AND currency_from = $2
              AND currency_to = $3
              {kind_filter}
            ORDER BY valid_from ASC
            """,
            source,
            currency_from,
            currency_to,
        )


async def _cleanup(
    pool: asyncpg.Pool, *, source: str, currency_from: str, currency_to: str
) -> None:
    async with pool.acquire() as c:
        await c.execute(
            "DELETE FROM currency_rates"
            " WHERE source=$1 AND currency_from=$2 AND currency_to=$3",
            source,
            currency_from,
            currency_to,
        )


# --- Test 1: polled config routes to polled upsert ---


async def test_polled_config_routes_to_polled_upsert(
    db_pool: asyncpg.Pool, rate_service: CurrencyRateService
) -> None:
    currency_from = "T01"
    source = RateSource.monobank
    rate = NormalizedRate(
        source=source,
        currency_from=currency_from,
        currency_to=_TO,
        rate_buy=Decimal("39"),
        rate_sell=Decimal("41"),
        rate_mid=Decimal("40"),
        at_time=T,
    )
    config = _polled_config(source=source, interval=300)

    try:
        await rate_service.ingest_rates(db_pool, [rate], config)
        rows = await _query_rows(
            db_pool, source=source, currency_from=currency_from, currency_to=_TO
        )
        assert len(rows) == 1
        assert rows[0]["last_polled_at"] is not None
        assert rows[0]["update_cadence_seconds"] == 300
    finally:
        await _cleanup(
            db_pool, source=source, currency_from=currency_from, currency_to=_TO
        )


# --- Test 2: historical config routes to historical upsert ---


async def test_historical_config_routes_to_historical_upsert(
    db_pool: asyncpg.Pool, rate_service: CurrencyRateService
) -> None:
    currency_from = "T02"
    source = RateSource.nbu
    rate = NormalizedRate(
        source=source,
        currency_from=currency_from,
        currency_to=_TO,
        rate_buy=None,
        rate_sell=None,
        rate_mid=Decimal("40"),
        at_time=T,
    )
    config = _historical_config(source=source)

    try:
        await rate_service.ingest_rates(db_pool, [rate], config)
        rows = await _query_rows(
            db_pool, source=source, currency_from=currency_from, currency_to=_TO
        )
        assert len(rows) == 1
        assert rows[0]["last_polled_at"] is None
        assert rows[0]["update_cadence_seconds"] is None
    finally:
        await _cleanup(
            db_pool, source=source, currency_from=currency_from, currency_to=_TO
        )


# --- Test 3: two successive polled cycles with unchanged rates ---


async def test_two_polled_cycles_unchanged_rates(
    db_pool: asyncpg.Pool, rate_service: CurrencyRateService
) -> None:
    currency_from = "T03"
    source = RateSource.monobank
    config = _polled_config(source=source)

    def _rate(at_time: datetime) -> NormalizedRate:
        return NormalizedRate(
            source=source,
            currency_from=currency_from,
            currency_to=_TO,
            rate_buy=None,
            rate_sell=None,
            rate_mid=Decimal("40"),
            at_time=at_time,
        )

    try:
        await rate_service.ingest_rates(db_pool, [_rate(T_minus_5m)], config)
        await rate_service.ingest_rates(db_pool, [_rate(T)], config)

        rows = await _query_rows(
            db_pool, source=source, currency_from=currency_from, currency_to=_TO
        )
        assert len(rows) == 1
        assert rows[0]["last_polled_at"] == T
    finally:
        await _cleanup(
            db_pool, source=source, currency_from=currency_from, currency_to=_TO
        )


# --- Test 4: two successive polled cycles with changing rates ---


async def test_two_polled_cycles_changing_rates(
    db_pool: asyncpg.Pool, rate_service: CurrencyRateService
) -> None:
    currency_from = "T04"
    source = RateSource.monobank
    config = _polled_config(source=source)

    try:
        rate1 = NormalizedRate(
            source=source,
            currency_from=currency_from,
            currency_to=_TO,
            rate_buy=None,
            rate_sell=None,
            rate_mid=Decimal("40"),
            at_time=T_minus_5m,
        )
        await rate_service.ingest_rates(db_pool, [rate1], config)

        rate2 = NormalizedRate(
            source=source,
            currency_from=currency_from,
            currency_to=_TO,
            rate_buy=None,
            rate_sell=None,
            rate_mid=Decimal("41"),
            at_time=T,
        )
        await rate_service.ingest_rates(db_pool, [rate2], config)

        rows = await _query_rows(
            db_pool, source=source, currency_from=currency_from, currency_to=_TO
        )
        assert len(rows) == 2
        closed = next(r for r in rows if r["valid_to"] is not None)
        open_row = next(r for r in rows if r["valid_to"] is None)
        assert closed["rate_mid"] == Decimal("40")
        assert open_row["rate_mid"] == Decimal("41")
    finally:
        await _cleanup(
            db_pool, source=source, currency_from=currency_from, currency_to=_TO
        )


# --- Test 5: historical backfill spanning 5 days ---


async def test_historical_backfill_five_days(
    db_pool: asyncpg.Pool, rate_service: CurrencyRateService
) -> None:
    currency_from = "T05"
    source = RateSource.nbu
    config = _historical_config(source=source)

    base = datetime(2025, 5, 27, 0, 0, tzinfo=UTC)
    rates = [
        NormalizedRate(
            source=source,
            currency_from=currency_from,
            currency_to=_TO,
            rate_buy=None,
            rate_sell=None,
            rate_mid=Decimal(str(40 + i)),
            at_time=base + timedelta(days=i),
        )
        for i in range(5)
    ]

    try:
        await rate_service.ingest_rates(db_pool, rates, config)

        rows = await _query_rows(
            db_pool,
            source=source,
            currency_from=currency_from,
            currency_to=_TO,
            kind="historical",
        )
        assert len(rows) == 5

        for i in range(4):
            assert rows[i]["valid_to"] == rows[i + 1]["valid_from"]
        assert rows[-1]["valid_to"] is None
    finally:
        await _cleanup(
            db_pool, source=source, currency_from=currency_from, currency_to=_TO
        )


# --- Test 6: historical reingestion is idempotent ---


async def test_historical_reingestion_is_idempotent(
    db_pool: asyncpg.Pool, rate_service: CurrencyRateService
) -> None:
    currency_from = "T06"
    source = RateSource.nbu
    config = _historical_config(source=source)

    base = datetime(2025, 5, 27, 0, 0, tzinfo=UTC)
    rates = [
        NormalizedRate(
            source=source,
            currency_from=currency_from,
            currency_to=_TO,
            rate_buy=None,
            rate_sell=None,
            rate_mid=Decimal(str(40 + i)),
            at_time=base + timedelta(days=i),
        )
        for i in range(5)
    ]

    try:
        await rate_service.ingest_rates(db_pool, rates, config)
        await rate_service.ingest_rates(db_pool, rates, config)

        rows = await _query_rows(
            db_pool,
            source=source,
            currency_from=currency_from,
            currency_to=_TO,
            kind="historical",
        )
        assert len(rows) == 5
    finally:
        await _cleanup(
            db_pool, source=source, currency_from=currency_from, currency_to=_TO
        )


# --- Test 7: historical reingestion with corrections ---


async def test_historical_reingestion_with_corrections(
    db_pool: asyncpg.Pool, rate_service: CurrencyRateService
) -> None:
    currency_from = "T07"
    source = RateSource.nbu
    config = _historical_config(source=source)

    base = datetime(2025, 5, 27, 0, 0, tzinfo=UTC)
    original_rates = [
        NormalizedRate(
            source=source,
            currency_from=currency_from,
            currency_to=_TO,
            rate_buy=None,
            rate_sell=None,
            rate_mid=Decimal(str(40 + i)),
            at_time=base + timedelta(days=i),
        )
        for i in range(5)
    ]

    # Correct day index 2 (rate changes from 42 to 99)
    corrected_rates = list(original_rates)
    corrected_rates[2] = NormalizedRate(
        source=source,
        currency_from=currency_from,
        currency_to=_TO,
        rate_buy=None,
        rate_sell=None,
        rate_mid=Decimal("99"),
        at_time=base + timedelta(days=2),
    )

    try:
        await rate_service.ingest_rates(db_pool, original_rates, config)
        await rate_service.ingest_rates(db_pool, corrected_rates, config)

        rows = await _query_rows(
            db_pool,
            source=source,
            currency_from=currency_from,
            currency_to=_TO,
            kind="historical",
        )
        assert len(rows) == 5

        corrected_day = next(
            r for r in rows if r["valid_from"] == base + timedelta(days=2)
        )
        assert corrected_day["rate_mid"] == Decimal("99")
    finally:
        await _cleanup(
            db_pool, source=source, currency_from=currency_from, currency_to=_TO
        )


# --- Test 8: mixed provider behavior ---


async def test_mixed_provider_both_sequences_independent(
    db_pool: asyncpg.Pool, rate_service: CurrencyRateService
) -> None:
    currency_from = "T08"
    to = _TO

    polled_source = RateSource.monobank
    historical_source = RateSource.nbu

    polled_rate = NormalizedRate(
        source=polled_source,
        currency_from=currency_from,
        currency_to=to,
        rate_buy=Decimal("39"),
        rate_sell=Decimal("41"),
        rate_mid=Decimal("40"),
        at_time=T,
    )
    historical_rate = NormalizedRate(
        source=historical_source,
        currency_from=currency_from,
        currency_to=to,
        rate_buy=None,
        rate_sell=None,
        rate_mid=Decimal("40"),
        at_time=T,
    )

    try:
        await rate_service.ingest_rates(
            db_pool, [polled_rate], _polled_config(source=polled_source)
        )
        await rate_service.ingest_rates(
            db_pool, [historical_rate], _historical_config(source=historical_source)
        )

        polled_rows = await _query_rows(
            db_pool, source=polled_source, currency_from=currency_from, currency_to=to
        )
        historical_rows = await _query_rows(
            db_pool,
            source=historical_source,
            currency_from=currency_from,
            currency_to=to,
        )

        assert len(polled_rows) == 1
        assert polled_rows[0]["last_polled_at"] is not None

        assert len(historical_rows) == 1
        assert historical_rows[0]["last_polled_at"] is None
    finally:
        await _cleanup(
            db_pool, source=polled_source, currency_from=currency_from, currency_to=to
        )
        await _cleanup(
            db_pool,
            source=historical_source,
            currency_from=currency_from,
            currency_to=to,
        )
