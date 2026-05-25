"""Unit tests for grosh_normalization.kafka utility functions.

Targets on_delivery and poll_message — the SDK boundary functions that wrap
confluent_kafka's delivery callback and polling loop.

All Kafka SDK objects (Consumer, Message, KafkaError) are mocked at the SDK
boundary. The system under test (kafka.py) is NOT mocked.
"""

import logging
from unittest.mock import MagicMock

import pytest
from confluent_kafka import KafkaError, KafkaException

from grosh_normalization.kafka import on_delivery, poll_message

# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


def _make_message(*, error: KafkaError | None = None) -> MagicMock:
    """Return a minimal confluent_kafka.Message fake."""
    msg = MagicMock()
    msg.error.return_value = error
    msg.topic.return_value = "normalized_transactions"
    msg.value.return_value = b'{"id": "abc"}'
    return msg


def _make_kafka_error(code: int) -> KafkaError:
    """Return a real KafkaError for the given error code."""
    err = MagicMock(spec=KafkaError)
    err.code.return_value = code
    return err


def _make_consumer(*, message: MagicMock | None) -> MagicMock:
    """Return a minimal Consumer fake whose poll() returns the given message."""
    consumer = MagicMock()
    consumer.poll.return_value = message
    return consumer


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


async def test_poll_message_returns_none_on_partition_eof() -> None:
    """EOF treated as fatal kills the consumer loop on every partition boundary."""
    eof_error = _make_kafka_error(KafkaError._PARTITION_EOF)
    msg = _make_message(error=eof_error)
    consumer = _make_consumer(message=msg)

    result = await poll_message(consumer)

    assert result is None


async def test_poll_message_raises_on_non_eof_error() -> None:
    """Real broker errors swallowed as empty polls, masking broker outages."""
    transport_error = _make_kafka_error(KafkaError._TRANSPORT)
    msg = _make_message(error=transport_error)
    consumer = _make_consumer(message=msg)

    with pytest.raises(KafkaException):
        await poll_message(consumer)


async def test_poll_message_returns_none_when_consumer_poll_returns_none() -> None:
    """Consumer timeout (no messages available) must return None, not raise."""
    consumer = _make_consumer(message=None)

    result = await poll_message(consumer)

    assert result is None


async def test_poll_message_returns_value_and_message_on_success() -> None:
    """Happy path: a valid message returns (raw_value, msg) tuple for the caller."""
    msg = _make_message(error=None)
    consumer = _make_consumer(message=msg)

    result = await poll_message(consumer)

    assert result is not None
    raw_value, returned_msg = result
    assert raw_value == b'{"id": "abc"}'
    assert returned_msg is msg


async def test_poll_message_tolerates_unknown_topic_within_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """UNKNOWN_TOPIC_OR_PART within tolerance window logs+returns None, not raise."""
    from grosh_normalization import kafka as kafka_module

    # Reset the module-level budget so prior tests don't poison this one.
    kafka_module._unknown_topic_budget.reset()
    # Compress the budget to keep the test fast.
    monkeypatch.setattr(
        kafka_module._unknown_topic_budget, "_tolerance", 5.0, raising=True
    )

    err = _make_kafka_error(KafkaError.UNKNOWN_TOPIC_OR_PART)
    msg = _make_message(error=err)
    consumer = _make_consumer(message=msg)

    result = await poll_message(consumer)

    assert result is None
    # Budget should have started ticking — first_seen set after the recorded error.
    assert kafka_module._unknown_topic_budget._first_seen is not None
    # Reset for downstream tests.
    kafka_module._unknown_topic_budget.reset()


async def test_poll_message_escalates_unknown_topic_after_budget_exhausted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If UNKNOWN_TOPIC_OR_PART persists beyond the tolerance, raise."""
    import time as _time

    from grosh_normalization import kafka as kafka_module

    kafka_module._unknown_topic_budget.reset()
    monkeypatch.setattr(
        kafka_module._unknown_topic_budget, "_tolerance", 5.0, raising=True
    )

    # Simulate the budget having started 10s ago (well past the 5s tolerance).
    kafka_module._unknown_topic_budget._first_seen = _time.monotonic() - 10.0

    err = _make_kafka_error(KafkaError.UNKNOWN_TOPIC_OR_PART)
    msg = _make_message(error=err)
    consumer = _make_consumer(message=msg)

    with pytest.raises(KafkaException):
        await poll_message(consumer)

    kafka_module._unknown_topic_budget.reset()


async def test_poll_message_resets_budget_on_successful_poll(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A single successful poll between transient errors must reset the budget."""
    import time as _time

    from grosh_normalization import kafka as kafka_module

    kafka_module._unknown_topic_budget.reset()
    monkeypatch.setattr(
        kafka_module._unknown_topic_budget, "_tolerance", 5.0, raising=True
    )
    # Place us near the budget edge.
    kafka_module._unknown_topic_budget._first_seen = _time.monotonic() - 4.0

    msg = _make_message(error=None)
    consumer = _make_consumer(message=msg)
    result = await poll_message(consumer)
    assert result is not None  # Success path.

    # Budget should now be reset; a fresh transient error starts a new window.
    assert kafka_module._unknown_topic_budget._first_seen is None

    kafka_module._unknown_topic_budget.reset()


async def test_on_delivery_logs_error_and_does_not_raise(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Unhandled exception in Kafka delivery callback kills the producer thread."""
    caplog.set_level(logging.ERROR, logger="grosh_normalization.kafka")

    err = _make_kafka_error(KafkaError._MSG_TIMED_OUT)
    msg = MagicMock()
    msg.topic.return_value = "normalized_transactions"

    # Must not raise — delivery callbacks run in the producer thread; an
    # uncaught exception there terminates the producer silently.
    result = on_delivery(err, msg)

    assert result is None
    error_records = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert (
        error_records
    ), "Expected an ERROR log from on_delivery when err is non-None; got none"
