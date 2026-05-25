"""Resilience helpers for Kafka consumer poll loops.

Currently exports `TransientErrorBudget` — a small clock-based budget that
lets a consumer tolerate a contiguous burst of "this should resolve itself"
errors (e.g. `UNKNOWN_TOPIC_OR_PART` during controlled topic recreation,
broker reshuffles) without crashing, while still escalating if the error
persists long enough to indicate a real misconfiguration.

Lives in `grosh_shared.messaging` because both the normalization and
enrichment consumers need identical semantics; duplicating the state
machine in each service invites drift. Pure stdlib — no confluent_kafka
dependency — so shared can keep its current dep surface.
"""

import time


class TransientErrorBudget:
    """Tracks how long a transient error has been recurring.

    State is per-process (one budget per consumer process). A single
    successful poll resets the budget — that matches the failure model:
    librdkafka re-resolves metadata on its own, so transient errors arrive
    in contiguous bursts and then either resolve or persist.
    """

    def __init__(self, tolerance_seconds: float) -> None:
        self._tolerance = tolerance_seconds
        self._first_seen: float | None = None

    def record(self) -> bool:
        """Record a transient error. Returns True if still within budget.

        Once over budget, subsequent calls keep returning False until
        `reset()` is called (typically on a successful poll). This lets the
        caller log + raise repeatedly so the failure stays visible.
        """
        now = time.monotonic()
        if self._first_seen is None:
            self._first_seen = now
        return (now - self._first_seen) < self._tolerance

    def reset(self) -> None:
        """Clear the budget. Called on successful poll or partition EOF."""
        self._first_seen = None
