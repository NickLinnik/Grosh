"""Unit tests for MonobankBackfillProvider.run_backfill.

Uses async fakes for MonobankClient and MonobankRepo to test:
- Single-page response → one API call with correct window
- Multi-page pagination → correct from/to window construction per chunk
- Out-of-bounds 400 response → loop stops cleanly (not an error)
- Other MonobankAPIError → propagates
- Empty response → no Kafka produce calls

The 31-day chunk boundary is the load-bearing constant (_CHUNK_SECONDS = 2_678_400).
Getting it wrong causes either API rate-limit errors (too-long windows) or
unnecessary API calls (too-short windows).
"""

import asyncio
from dataclasses import dataclass, field
from uuid import UUID, uuid4

import pytest

import grosh_ingestion.sources.monobank.backfill as backfill_mod
from grosh_ingestion.sources.monobank.backfill import (
    _CHUNK_SECONDS,
    MonobankBackfillProvider,
)
from grosh_ingestion.sources.monobank.client import MonobankAPIError
from grosh_ingestion.sources.monobank.models import MonobankStatementItem

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

USER_ID = UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
ACCOUNT_ID = UUID("11111111-2222-3333-4444-555555555555")
INTEGRATION_ID = UUID("12345678-1234-5678-1234-567812345678")
ACCOUNT_EXTERNAL_ID = "mono-ext-001"

# One 31-day window: to=T, from=T - _CHUNK_SECONDS
_T0 = 1_700_000_000
_T1 = _T0 + _CHUNK_SECONDS  # exactly one chunk ahead


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


@dataclass
class _FakeAccountRef:
    id: UUID = field(default_factory=uuid4)


class FakeMonobankRepo:
    """In-memory replacement for MonobankRepo — no DB."""

    def __init__(
        self, token: str = "test-token", account_id: UUID = ACCOUNT_ID
    ) -> None:
        self._token = token
        self._account_id = account_id

    async def decrypt_token(
        self, conn: object, integration_id: UUID, key: str
    ) -> str | None:
        return self._token

    async def get_account_by_external_id(
        self, conn: object, external_id: str, integration_id: UUID
    ) -> _FakeAccountRef | None:
        return _FakeAccountRef(id=self._account_id)


class FakeMonobankClient:
    """Async fake MonobankClient.

    Accepts a mapping of (account_id, from_ts, to_ts) → list[items] so each
    chunk can return different data. Unrecognised windows return [].
    Records every get_statements call for assertion.
    """

    def __init__(
        self,
        responses: dict[tuple[str, int, int], list[MonobankStatementItem]]
        | None = None,
        error_on: tuple[str, int, int] | None = None,
        error: MonobankAPIError | None = None,
    ) -> None:
        self._responses = responses or {}
        self._error_on = error_on
        self._error = error
        self.calls: list[tuple[str, int, int]] = []

    async def __aenter__(self) -> "FakeMonobankClient":
        return self

    async def __aexit__(self, *exc: object) -> None:
        pass

    async def get_statements(
        self, account_id: str, from_ts: int, to_ts: int
    ) -> list[MonobankStatementItem]:
        self.calls.append((account_id, from_ts, to_ts))
        if self._error_on == (account_id, from_ts, to_ts):
            raise self._error  # type: ignore[misc]
        return self._responses.get((account_id, from_ts, to_ts), [])


class FakeProducer:
    """Records produce() calls without real Kafka."""

    def __init__(self) -> None:
        self.produced: list[dict] = []

    def produce(
        self, *, topic: object, key: bytes, value: bytes, on_delivery: object
    ) -> None:
        self.produced.append({"topic": topic, "key": key, "value": value})

    def poll(self, timeout: float) -> None:
        pass

    def flush(self, timeout: float = 30) -> None:
        pass


class FakePool:
    """Minimal asyncpg pool fake — wraps a single no-op connection."""

    def acquire(self) -> "FakePool":
        return self

    async def __aenter__(self) -> "FakeConn":
        return FakeConn()

    async def __aexit__(self, *_: object) -> None:
        pass


class FakeConn:
    """Minimal asyncpg connection fake that supports nested transactions."""

    def transaction(self) -> "FakeTx":
        return FakeTx()


class FakeTx:
    async def __aenter__(self) -> "FakeTx":
        return self

    async def __aexit__(self, *_: object) -> None:
        pass


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_statement_item(txn_id: str = "txn-001") -> MonobankStatementItem:
    return MonobankStatementItem.model_validate(
        {
            "id": txn_id,
            "time": 1_700_000_100,
            "description": "Coffee",
            "mcc": 5812,
            "originalMcc": 5812,
            "hold": False,
            "amount": -5000,
            "operationAmount": -5000,
            "currencyCode": 980,
            "cashbackAmount": 50,
            "balance": 100000,
        }
    )


def _make_provider(
    fake_client: FakeMonobankClient,
    fake_repo: FakeMonobankRepo | None = None,
) -> MonobankBackfillProvider:
    """Build a MonobankBackfillProvider with injected fakes."""
    provider = MonobankBackfillProvider.__new__(MonobankBackfillProvider)
    provider._repo = fake_repo or FakeMonobankRepo()
    # Patch MonobankClient constructor to return our fake
    provider._client_factory = lambda token: fake_client  # type: ignore[attr-defined]
    return provider


# ---------------------------------------------------------------------------
# Patch MonobankClient at the module level
# ---------------------------------------------------------------------------


async def _noop_sleep(_: float) -> None:
    pass


@pytest.fixture(autouse=True)
def patch_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    """Replace asyncio.sleep with a no-op so rate-limit pauses don't block tests."""
    monkeypatch.setattr(asyncio, "sleep", _noop_sleep)


async def _run_backfill(
    fake_client: FakeMonobankClient,
    fake_repo: FakeMonobankRepo | None = None,
    from_timestamp: int = _T0,
    to_timestamp: int = _T1,
    monkeypatch: pytest.MonkeyPatch | None = None,
) -> FakeProducer:
    """Wire fakes and call run_backfill, returning the fake producer."""
    producer = FakeProducer()
    pool = FakePool()
    repo = fake_repo or FakeMonobankRepo()

    async def _noop_rls(conn: object, user_id: UUID) -> None:
        pass

    assert monkeypatch is not None, "monkeypatch fixture must be passed in"
    monkeypatch.setattr(
        backfill_mod,
        "MonobankClient",
        lambda token: fake_client,
    )
    monkeypatch.setattr(backfill_mod, "set_rls_user_id", _noop_rls)
    monkeypatch.setenv("ENCRYPTION_KEY", "test-key")

    provider = MonobankBackfillProvider.__new__(MonobankBackfillProvider)
    provider._repo = repo  # type: ignore[attr-defined]

    await provider.run_backfill(
        pool=pool,  # type: ignore[arg-type]
        producer=producer,  # type: ignore[arg-type]
        integration_id=INTEGRATION_ID,
        user_id=USER_ID,
        account_external_id=ACCOUNT_EXTERNAL_ID,
        from_timestamp=from_timestamp,
        to_timestamp=to_timestamp,
    )

    return producer


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


async def test_single_chunk_makes_one_api_call(monkeypatch: pytest.MonkeyPatch) -> None:
    """A window ≤ 31 days results in exactly one get_statements call.

    More calls than expected would burn through Monobank's rate limit (1 req/60s).
    """
    item = _make_statement_item("txn-1")
    fake_client = FakeMonobankClient(
        responses={(ACCOUNT_EXTERNAL_ID, _T0, _T1): [item]}
    )

    producer = await _run_backfill(
        fake_client, from_timestamp=_T0, to_timestamp=_T1, monkeypatch=monkeypatch
    )

    assert len(fake_client.calls) == 1
    assert fake_client.calls[0] == (ACCOUNT_EXTERNAL_ID, _T0, _T1)
    assert len(producer.produced) == 1


async def test_single_chunk_empty_response_produces_nothing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Empty statement response → no Kafka messages produced.

    If empty response still produced a message, the pipeline would process
    phantom transactions.
    """
    fake_client = FakeMonobankClient(responses={})

    producer = await _run_backfill(
        fake_client, from_timestamp=_T0, to_timestamp=_T1, monkeypatch=monkeypatch
    )

    assert len(producer.produced) == 0
    assert len(fake_client.calls) == 1  # the API was called, just returned nothing


async def test_multi_chunk_window_constructs_correct_boundaries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A 2-chunk window results in two API calls with non-overlapping half-open windows.

    Overlapping or gapped windows would cause duplicate or missing transactions.
    The second call's to_ts must equal the first call's from_ts - 1.
    """
    two_chunks_to = _T0 + 2 * _CHUNK_SECONDS

    fake_client = FakeMonobankClient(responses={})

    await _run_backfill(
        fake_client,
        from_timestamp=_T0,
        to_timestamp=two_chunks_to,
        monkeypatch=monkeypatch,
    )

    assert len(fake_client.calls) == 2

    # First chunk covers the most recent period
    first_call = fake_client.calls[0]
    second_call = fake_client.calls[1]

    # The first call's from_ts should be exactly one chunk before two_chunks_to
    assert first_call[2] == two_chunks_to  # to_ts of first chunk
    assert first_call[1] == two_chunks_to - _CHUNK_SECONDS  # from_ts of first chunk

    # The second call's to_ts must be one less than the first call's from_ts
    # (the loop sets current_to = current_from - 1)
    assert second_call[2] == first_call[1] - 1
    assert second_call[1] == _T0  # final chunk goes to original from_timestamp


async def test_out_of_bounds_400_stops_loop_cleanly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Monobank 400 'out of bounds' → loop stops without raising.

    This response means we've reached the account creation date. Raising instead
    of stopping would make every backfill that reaches account creation date fail.
    """
    error = MonobankAPIError(status_code=400, message="out of bounds for the account")
    fake_client = FakeMonobankClient(
        error_on=(ACCOUNT_EXTERNAL_ID, _T0, _T1),
        error=error,
    )

    # Should not raise — loop terminates cleanly
    producer = await _run_backfill(
        fake_client, from_timestamp=_T0, to_timestamp=_T1, monkeypatch=monkeypatch
    )

    assert len(producer.produced) == 0
    assert len(fake_client.calls) == 1


async def test_non_out_of_bounds_api_error_propagates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Non-out-of-bounds MonobankAPIError propagates to the caller.

    Swallowing unexpected errors (auth failures, server errors) would silently
    produce an incomplete backfill that looks successful.
    """
    error = MonobankAPIError(status_code=429, message="Too many requests")
    fake_client = FakeMonobankClient(
        error_on=(ACCOUNT_EXTERNAL_ID, _T0, _T1),
        error=error,
    )

    with pytest.raises(MonobankAPIError) as exc_info:
        await _run_backfill(
            fake_client, from_timestamp=_T0, to_timestamp=_T1, monkeypatch=monkeypatch
        )

    assert exc_info.value.status_code == 429


async def test_multiple_items_in_chunk_all_produced(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """All statement items in a chunk are published to Kafka, not just the first.

    Producing only the first item per chunk would silently lose transactions.
    """
    items = [_make_statement_item(f"txn-{i}") for i in range(5)]
    fake_client = FakeMonobankClient(responses={(ACCOUNT_EXTERNAL_ID, _T0, _T1): items})

    producer = await _run_backfill(
        fake_client, from_timestamp=_T0, to_timestamp=_T1, monkeypatch=monkeypatch
    )

    assert len(producer.produced) == 5
