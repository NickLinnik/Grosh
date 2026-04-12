from uuid import UUID

from grosh_shared.id_utils import generate_transaction_id


def test_generate_transaction_id_is_deterministic() -> None:
    first = generate_transaction_id("monobank", "abc123")
    second = generate_transaction_id("monobank", "abc123")

    assert first == second


def test_generate_transaction_id_different_source_id_differs() -> None:
    a = generate_transaction_id("monobank", "abc123")
    b = generate_transaction_id("monobank", "abc124")

    assert a != b


def test_generate_transaction_id_different_source_differs() -> None:
    a = generate_transaction_id("monobank", "abc123")
    b = generate_transaction_id("manual", "abc123")

    assert a != b


def test_generate_transaction_id_returns_uuid() -> None:
    result = generate_transaction_id("monobank", "abc123")

    assert isinstance(result, UUID)
