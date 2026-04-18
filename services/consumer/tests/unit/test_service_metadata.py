"""Service-level tests: metadata shape and naming.

Covers spec section 3.8, tests 29-33.
"""

from datetime import timedelta
from decimal import Decimal

from tests.helpers import T, make_event

VALID_FROM = T - timedelta(hours=1)
FRESH_POLLED = T - timedelta(seconds=60)
STALE_POLLED = T - timedelta(seconds=300 + 3600)


async def test_metadata_key_naming(repo, service):
    """rate_uah, rate_usd, rate_eur keys — lowercase target currency."""
    repo.add_source("monobank", max_staleness_seconds=300, base_currencies=["UAH"])
    repo.add_rate(
        "monobank",
        "PLN",
        "UAH",
        mid=4.0,
        buy=3.9,
        sell=4.1,
        valid_from=VALID_FROM,
        polled=FRESH_POLLED,
    )
    repo.add_rate(
        "monobank",
        "UAH",
        "USD",
        mid=0.025,
        buy=0.024,
        sell=0.026,
        valid_from=VALID_FROM,
        polled=FRESH_POLLED,
    )
    repo.add_rate(
        "monobank",
        "UAH",
        "EUR",
        mid=0.023,
        buy=0.022,
        sell=0.024,
        valid_from=VALID_FROM,
        polled=FRESH_POLLED,
    )
    # PLN event, 2-hop to USD/EUR via UAH pivot
    result = await service.convert(None, make_event(currency_code="PLN"))
    assert "rate_uah" in result.rate_metadata
    assert "rate_usd" in result.rate_metadata
    assert "rate_eur" in result.rate_metadata


async def test_metadata_quality_matches_path_max_tier(repo, service):
    repo.add_source("monobank", max_staleness_seconds=300, base_currencies=["UAH"])
    # FRESH rate
    repo.add_rate(
        "monobank",
        "PLN",
        "UAH",
        mid=4.0,
        buy=3.9,
        sell=4.1,
        valid_from=VALID_FROM,
        polled=FRESH_POLLED,
    )
    result_fresh = await service.convert(None, make_event())
    assert result_fresh.rate_metadata["rate_uah"]["quality"] == "fresh"

    repo2 = type(repo)()
    repo2.add_source("monobank", max_staleness_seconds=300, base_currencies=["UAH"])
    # STALE rate
    repo2.add_rate(
        "monobank",
        "PLN",
        "UAH",
        mid=4.0,
        buy=3.9,
        sell=4.1,
        valid_from=VALID_FROM,
        polled=STALE_POLLED,
    )
    from grosh_consumer.services.currency_conversion_service import (
        CurrencyConversionService,
    )

    svc2 = CurrencyConversionService(repo2)
    result_stale = await svc2.convert(None, make_event())
    assert result_stale.rate_metadata["rate_uah"]["quality"] == "stale"

    repo3 = type(repo)()
    repo3.add_source("monobank", max_staleness_seconds=300, base_currencies=["UAH"])
    # CLOSEST rate (valid window outside T, polled non-NULL for eligibility)
    repo3.add_rate(
        "monobank",
        "PLN",
        "UAH",
        mid=4.0,
        buy=3.9,
        sell=4.1,
        valid_from=T - timedelta(days=2),
        valid_to=T - timedelta(days=1),
        polled=T - timedelta(days=1),
    )
    svc3 = CurrencyConversionService(repo3)
    result_closest = await svc3.convert(None, make_event())
    assert result_closest.rate_metadata["rate_uah"]["quality"] == "closest"


async def test_metadata_hops_matches_step_count(repo, service):
    repo.add_source("monobank", max_staleness_seconds=300, base_currencies=["UAH"])
    # 1-hop
    repo.add_rate(
        "monobank",
        "PLN",
        "UAH",
        mid=4.0,
        buy=3.9,
        sell=4.1,
        valid_from=VALID_FROM,
        polled=FRESH_POLLED,
    )
    result = await service.convert(None, make_event())
    assert result.rate_metadata["rate_uah"]["hops"] == 1

    # Add UAH→USD for 2-hop
    repo.add_rate(
        "monobank",
        "UAH",
        "USD",
        mid=0.025,
        buy=0.024,
        sell=0.026,
        valid_from=VALID_FROM,
        polled=FRESH_POLLED,
    )
    result2 = await service.convert(None, make_event(currency_code="PLN"))
    assert result2.rate_metadata["rate_usd"]["hops"] == 2


async def test_metadata_effective_rate_is_stringified_decimal(repo, service):
    repo.add_source("monobank", max_staleness_seconds=300, base_currencies=["UAH"])
    repo.add_rate(
        "monobank",
        "PLN",
        "UAH",
        mid=4.0,
        buy=3.9,
        sell=4.1,
        valid_from=VALID_FROM,
        polled=FRESH_POLLED,
    )
    result = await service.convert(None, make_event())
    meta = result.rate_metadata["rate_uah"]
    assert isinstance(meta["effective_rate"], str)
    # Must be parseable as Decimal without error
    parsed = Decimal(meta["effective_rate"])
    assert parsed > 0


async def test_metadata_passthrough_currency_absent(repo, service):
    """UAH event: rate_uah absent from metadata, rate_usd/rate_eur may be present."""
    repo.add_source("monobank", max_staleness_seconds=300, base_currencies=["UAH"])
    repo.add_rate(
        "monobank",
        "UAH",
        "USD",
        mid=0.025,
        buy=0.024,
        sell=0.026,
        valid_from=VALID_FROM,
        polled=FRESH_POLLED,
    )
    result = await service.convert(
        None, make_event(currency_code="UAH", amount_cents=55500)
    )
    assert result.amounts["UAH"] == 55500
    assert "rate_uah" not in result.rate_metadata
    assert "rate_usd" in result.rate_metadata
