"""Unit tests for grosh_shared.messaging.kafka_resilience.TransientErrorBudget.

Pure-stdlib state machine — no Kafka SDK involved. Verifies the budget
correctly tracks contiguous transient errors, resets on success, and
returns False once past the tolerance.
"""

import time

from grosh_shared.messaging.kafka_resilience import TransientErrorBudget


def test_initial_state_first_seen_is_none() -> None:
    budget = TransientErrorBudget(tolerance_seconds=5.0)
    assert budget._first_seen is None


def test_first_record_sets_first_seen_and_returns_true() -> None:
    budget = TransientErrorBudget(tolerance_seconds=5.0)
    assert budget.record() is True
    assert budget._first_seen is not None


def test_record_within_tolerance_returns_true() -> None:
    budget = TransientErrorBudget(tolerance_seconds=5.0)
    budget.record()  # arm
    # Subsequent immediate records remain within budget.
    assert budget.record() is True
    assert budget.record() is True


def test_record_past_tolerance_returns_false() -> None:
    budget = TransientErrorBudget(tolerance_seconds=5.0)
    # Simulate a budget that started 10s ago.
    budget._first_seen = time.monotonic() - 10.0
    assert budget.record() is False


def test_record_past_tolerance_does_not_clear_first_seen() -> None:
    """Once over budget, _first_seen stays set; further calls keep returning False."""
    budget = TransientErrorBudget(tolerance_seconds=5.0)
    started = time.monotonic() - 10.0
    budget._first_seen = started
    assert budget.record() is False
    assert budget._first_seen == started
    assert budget.record() is False  # Still over budget.


def test_reset_clears_first_seen() -> None:
    budget = TransientErrorBudget(tolerance_seconds=5.0)
    budget.record()
    assert budget._first_seen is not None
    budget.reset()
    assert budget._first_seen is None


def test_record_after_reset_starts_a_fresh_window() -> None:
    budget = TransientErrorBudget(tolerance_seconds=5.0)
    budget._first_seen = time.monotonic() - 10.0
    assert budget.record() is False  # Past budget.
    budget.reset()
    assert budget.record() is True  # Fresh window — within tolerance.
