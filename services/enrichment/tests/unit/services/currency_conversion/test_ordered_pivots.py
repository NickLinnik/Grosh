"""Tests for _ordered_pivots — deterministic pivot ordering."""

from grosh_enrichment.repositories.currency_rate_repo import SourceConfig
from grosh_enrichment.services.currency_conversion_service import _ordered_pivots


def test_empty_chain_yields_empty_list():
    assert _ordered_pivots([]) == []


def test_single_source_single_base():
    chain = [SourceConfig(source="monobank", base_currencies=["UAH"])]
    assert _ordered_pivots(chain) == ["UAH"]


def test_single_source_multiple_bases_preserves_array_order():
    chain = [SourceConfig(source="monobank", base_currencies=["UAH", "EUR", "USD"])]
    assert _ordered_pivots(chain) == ["UAH", "EUR", "USD"]


def test_multiple_sources_disjoint_bases_chain_order():
    chain = [
        SourceConfig(source="monobank", base_currencies=["UAH"]),
        SourceConfig(source="nbu", base_currencies=["EUR", "USD"]),
    ]
    assert _ordered_pivots(chain) == ["UAH", "EUR", "USD"]


def test_multiple_sources_overlapping_bases_first_seen_wins():
    chain = [
        SourceConfig(source="monobank", base_currencies=["UAH", "EUR"]),
        SourceConfig(source="nbu", base_currencies=["EUR", "USD"]),
    ]
    result = _ordered_pivots(chain)
    assert result == ["UAH", "EUR", "USD"]
    # EUR appears once, from monobank (first-seen)
    assert result.count("EUR") == 1


def test_deterministic_across_calls():
    chain = [
        SourceConfig(source="monobank", base_currencies=["UAH", "EUR"]),
        SourceConfig(source="nbu", base_currencies=["EUR", "USD"]),
    ]
    a = _ordered_pivots(chain)
    b = _ordered_pivots(chain)
    assert a == b
