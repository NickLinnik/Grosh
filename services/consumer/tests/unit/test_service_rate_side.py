"""Service-level tests: rate-side selection in full conversion paths.

Covers spec section 3.6, tests 20-26.
"""

from datetime import timedelta
from decimal import Decimal

from tests.helpers import T, make_event

VALID_FROM = T - timedelta(hours=1)
FRESH_POLLED = T - timedelta(seconds=60)


async def test_one_hop_direct_uses_buy_side(repo, service):
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
    result = await service.convert(None, make_event(amount_cents=10000))
    assert result.amounts["UAH"] == 39000
    meta = result.rate_metadata["rate_uah"]
    assert meta["path"][0]["rate_side"] == "buy"


async def test_one_hop_reverse_uses_sell_side(repo, service):
    repo.add_source("monobank", max_staleness_seconds=300, base_currencies=["UAH"])
    repo.add_rate(
        "monobank",
        "UAH",
        "PLN",
        mid=0.25,
        buy=0.24,
        sell=0.26,
        valid_from=VALID_FROM,
        polled=FRESH_POLLED,
    )
    result = await service.convert(None, make_event(amount_cents=10000))
    # 10000 / 0.26 ≈ 38462
    assert result.amounts["UAH"] is not None
    meta = result.rate_metadata["rate_uah"]
    assert meta["path"][0]["rate_side"] == "sell"
    assert meta["path"][0]["op"] == "divide"


async def test_two_hop_leg1_direct_leg2_direct_both_buy(repo, service):
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
    result = await service.convert(None, make_event(currency_code="PLN"))
    meta = result.rate_metadata["rate_usd"]
    assert meta["path"][0]["rate_side"] == "buy"
    assert meta["path"][1]["rate_side"] == "buy"
    assert meta["sides"] == ["buy"]


async def test_two_hop_leg1_reverse_leg2_direct(repo, service):
    """Leg1: UAH/PLN divide=True → sell side. Leg2: UAH/USD direct → buy side."""
    repo.add_source("monobank", max_staleness_seconds=300, base_currencies=["UAH"])
    # Leg1: only reverse stored for PLN→UAH
    repo.add_rate(
        "monobank",
        "UAH",
        "PLN",
        mid=0.25,
        buy=0.24,
        sell=0.26,
        valid_from=VALID_FROM,
        polled=FRESH_POLLED,
    )
    # Leg2: direct UAH→USD
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
    result = await service.convert(None, make_event(currency_code="PLN"))
    meta = result.rate_metadata["rate_usd"]
    assert meta["path"][0]["rate_side"] == "sell"
    assert meta["path"][1]["rate_side"] == "buy"
    assert meta["sides"] == ["buy", "sell"]


async def test_falls_back_to_mid_when_buy_null(repo, service):
    repo.add_source("monobank", max_staleness_seconds=300, base_currencies=["UAH"])
    repo.add_rate(
        "monobank",
        "PLN",
        "UAH",
        mid=4.0,
        buy=None,
        sell=4.1,
        valid_from=VALID_FROM,
        polled=FRESH_POLLED,
    )
    result = await service.convert(None, make_event(amount_cents=10000))
    assert result.amounts["UAH"] == 40000
    meta = result.rate_metadata["rate_uah"]
    assert meta["path"][0]["rate_side"] == "mid"
    assert meta["sides"] == ["mid"]


async def test_never_falls_to_opposite_side(repo, service):
    """PLN/UAH direct (divide=False) with buy=None must use mid, not sell."""
    repo.add_source("monobank", max_staleness_seconds=300, base_currencies=["UAH"])
    repo.add_rate(
        "monobank",
        "PLN",
        "UAH",
        mid=4.0,
        buy=None,
        sell=4.1,
        valid_from=VALID_FROM,
        polled=FRESH_POLLED,
    )
    result = await service.convert(None, make_event(amount_cents=10000))
    # mid used → 10000 * 4.0 = 40000, not 10000 * 4.1 = 41000
    assert result.amounts["UAH"] == 40000


async def test_compounded_spread_in_two_hop(repo, service):
    """Both buy sides compound: effective_rate = 3.9 * 0.025 = 0.0975."""
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
        mid=0.026,
        buy=0.025,
        sell=0.027,
        valid_from=VALID_FROM,
        polled=FRESH_POLLED,
    )
    result = await service.convert(
        None, make_event(currency_code="PLN", amount_cents=10000)
    )
    meta = result.rate_metadata["rate_usd"]
    effective = Decimal(meta["effective_rate"])
    assert effective == Decimal("3.9") * Decimal("0.025")
    # 10000 * 0.0975 = 975
    assert result.amounts["USD"] == 975
    assert meta["quality"] == "fresh"
    assert meta["sides"] == ["buy"]
