"""Tests for _ordered_pivots — deterministic pivot ordering."""

from grosh_consumer.repositories.currency_rate_repo import SourceConfig
from grosh_consumer.services.currency_conversion_service import _ordered_pivots


def _src(source, base_currencies):
    return SourceConfig(
        source=source, max_staleness_seconds=300, base_currencies=list(base_currencies)
    )


def test_empty_chain_yields_empty_list():
    assert _ordered_pivots([]) == []


def test_single_source_single_base():
    chain = [_src("monobank", ["UAH"])]
    assert _ordered_pivots(chain) == ["UAH"]


def test_single_source_multiple_bases_preserves_array_order():
    chain = [_src("monobank", ["UAH", "EUR", "USD"])]
    assert _ordered_pivots(chain) == ["UAH", "EUR", "USD"]


def test_multiple_sources_disjoint_bases_in_chain_order():
    chain = [
        _src("monobank", ["UAH"]),
        _src("nbu", ["PLN"]),
        _src("ecb", ["GBP"]),
    ]
    assert _ordered_pivots(chain) == ["UAH", "PLN", "GBP"]


def test_overlapping_bases_first_seen_wins():
    chain = [
        _src("monobank", ["UAH", "EUR"]),
        _src("nbu", ["EUR", "USD"]),
        _src("ecb", ["USD", "GBP"]),
    ]
    assert _ordered_pivots(chain) == ["UAH", "EUR", "USD", "GBP"]


def test_deterministic_across_calls():
    chain = [
        _src("monobank", ["UAH", "EUR"]),
        _src("nbu", ["EUR", "USD"]),
        _src("ecb", ["USD", "GBP"]),
    ]
    result1 = _ordered_pivots(chain)
    result2 = _ordered_pivots(chain)
    assert result1 == result2
