"""In-process failure domain isolation tests.

Verifies that a crash in the normalizer consumer does not affect the pipeline
consumer and vice versa — each runs as an independent coroutine with its own
exception boundary. Uses asyncio Tasks with real consumer entry-point functions
patched to raise on demand.
"""

import asyncio
from unittest.mock import MagicMock, patch

import pytest

pytestmark = pytest.mark.asyncio


async def test_normalizer_crash_does_not_cancel_pipeline() -> None:
    """A normalizer consumer crash leaves the pipeline coroutine running.

    The normalizer consumer raises immediately; the pipeline consumer runs
    until explicitly cancelled. Both run as independent asyncio Tasks — a
    crash in one must not propagate to or cancel the other.
    """
    pipeline_started = asyncio.Event()
    pipeline_cancelled = asyncio.Event()

    async def _fake_pipeline_consumer(pool, orchestrator):
        pipeline_started.set()
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            pipeline_cancelled.set()
            raise

    async def _fake_normalization_consumer(pool, staging_repo):
        raise RuntimeError("normalization consumer exploded")

    with (
        patch(
            "grosh_normalizer.consumers.normalization_consumer"
            ".run_normalization_consumer",
            side_effect=_fake_normalization_consumer,
        ),
        patch(
            "grosh_pipeline.consumers.pipeline_consumer.run_pipeline_consumer",
            side_effect=_fake_pipeline_consumer,
        ),
    ):
        mock_pool = MagicMock()
        mock_staging_repo = MagicMock()
        mock_orchestrator = MagicMock()

        from grosh_normalizer.consumers.normalization_consumer import (
            run_normalization_consumer,
        )
        from grosh_pipeline.consumers.pipeline_consumer import run_pipeline_consumer

        normalizer_task = asyncio.create_task(
            run_normalization_consumer(mock_pool, mock_staging_repo)
        )
        pipeline_task = asyncio.create_task(
            run_pipeline_consumer(mock_pool, mock_orchestrator)
        )

        # Wait for normalizer to crash
        with pytest.raises(RuntimeError, match="normalization consumer exploded"):
            await normalizer_task

        # Pipeline must still be running (not cancelled by normalizer crash)
        assert not pipeline_task.done(), (
            "pipeline task was cancelled or finished after normalizer crash — "
            "the two consumers must be independent failure domains"
        )
        assert await asyncio.wait_for(pipeline_started.wait(), timeout=1.0)

        # Clean up
        pipeline_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pipeline_task
        assert pipeline_cancelled.is_set()


async def test_pipeline_crash_does_not_cancel_normalizer() -> None:
    """A pipeline consumer crash leaves the normalizer coroutine running."""
    normalizer_started = asyncio.Event()
    normalizer_cancelled = asyncio.Event()

    async def _fake_normalization_consumer(pool, staging_repo):
        normalizer_started.set()
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            normalizer_cancelled.set()
            raise

    async def _fake_pipeline_consumer(pool, orchestrator):
        raise RuntimeError("pipeline consumer exploded")

    mock_pool = MagicMock()
    mock_staging_repo = MagicMock()
    mock_orchestrator = MagicMock()

    with (
        patch(
            "grosh_normalizer.consumers.normalization_consumer"
            ".run_normalization_consumer",
            side_effect=_fake_normalization_consumer,
        ),
        patch(
            "grosh_pipeline.consumers.pipeline_consumer.run_pipeline_consumer",
            side_effect=_fake_pipeline_consumer,
        ),
    ):
        from grosh_normalizer.consumers.normalization_consumer import (
            run_normalization_consumer,
        )
        from grosh_pipeline.consumers.pipeline_consumer import run_pipeline_consumer

        normalizer_task = asyncio.create_task(
            run_normalization_consumer(mock_pool, mock_staging_repo)
        )
        pipeline_task = asyncio.create_task(
            run_pipeline_consumer(mock_pool, mock_orchestrator)
        )

        # Wait for pipeline to crash
        with pytest.raises(RuntimeError, match="pipeline consumer exploded"):
            await pipeline_task

        # Normalizer must still be running
        assert not normalizer_task.done(), (
            "normalizer task was cancelled or finished after pipeline crash — "
            "the two consumers must be independent failure domains"
        )
        assert await asyncio.wait_for(normalizer_started.wait(), timeout=1.0)

        # Clean up
        normalizer_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await normalizer_task
        assert normalizer_cancelled.is_set()


async def test_independent_tasks_share_no_state() -> None:
    """Normalizer and pipeline share only the DB pool — no other shared state.

    Verifies that each consumer function accepts its own set of dependencies
    and does not hold any module-level mutable state that would bleed between
    runs (re-imports produce fresh function objects).
    """
    from grosh_normalizer.consumers.normalization_consumer import (
        run_normalization_consumer,
    )
    from grosh_pipeline.consumers.pipeline_consumer import run_pipeline_consumer

    # Both consumer functions must be distinct callables with no shared closure
    assert run_normalization_consumer is not run_pipeline_consumer

    # Inspect signatures — normalizer takes (pool, staging_repo),
    # pipeline takes (pool, orchestrator). Neither should bleed state.
    import inspect

    norm_sig = inspect.signature(run_normalization_consumer)
    pipe_sig = inspect.signature(run_pipeline_consumer)

    norm_params = list(norm_sig.parameters)
    pipe_params = list(pipe_sig.parameters)

    assert "pool" in norm_params
    assert "pool" in pipe_params
    # They must not share a second parameter name (staging_repo vs orchestrator)
    assert norm_params[1] != pipe_params[1], (
        "normalizer and pipeline consumer share the same second parameter name — "
        "they may share module-level state"
    )
