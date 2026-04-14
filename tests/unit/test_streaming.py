"""Tests for @stream_step decorator and StreamingCheckpointer."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from gravtory.ai.streaming import StreamingCheckpointer, stream_step
from gravtory.core.errors import ConfigurationError
from gravtory.decorators.step import step

if TYPE_CHECKING:
    from gravtory.core.types import StepDefinition


class TestStreamingCheckpointer:
    """StreamingCheckpointer unit tests."""

    def test_collects_chunks(self) -> None:
        """feed() accumulates chunks in the buffer."""
        cp = StreamingCheckpointer(interval=100)
        cp.feed("Hello ")
        cp.feed("world")
        assert cp.buffer == "Hello world"

    def test_checkpoint_at_interval(self) -> None:
        """A snapshot is taken when token count crosses the interval boundary."""
        cp = StreamingCheckpointer(interval=10)
        # 10 tokens ~= 40 chars at 4 chars/token
        snap = cp.feed("A" * 44)  # > 10 tokens
        assert snap is not None
        assert snap.index == 0
        assert snap.content == "A" * 44
        assert len(cp.snapshots) == 1

    def test_no_checkpoint_before_interval(self) -> None:
        """No snapshot is taken before the interval boundary."""
        cp = StreamingCheckpointer(interval=100)
        snap = cp.feed("short")
        assert snap is None
        assert len(cp.snapshots) == 0

    def test_resume_point(self) -> None:
        """get_resume_point returns latest snapshot content."""
        cp = StreamingCheckpointer(interval=5)
        cp.feed("X" * 24)  # >=5 tokens at 4 chars/token → snapshot
        assert cp.get_resume_point() is not None
        assert cp.get_resume_point() == "X" * 24

    def test_resume_point_empty(self) -> None:
        """get_resume_point returns None when no snapshots exist."""
        cp = StreamingCheckpointer(interval=100)
        assert cp.get_resume_point() is None

    def test_finalize(self) -> None:
        """finalize() returns complete buffer and takes final snapshot."""
        cp = StreamingCheckpointer(interval=1000)
        cp.feed("chunk1 ")
        cp.feed("chunk2")
        result = cp.finalize()
        assert result == "chunk1 chunk2"
        assert len(cp.snapshots) >= 1

    def test_empty_chunks_ignored(self) -> None:
        """Empty strings passed to feed() are ignored."""
        cp = StreamingCheckpointer(interval=10)
        assert cp.feed("") is None
        assert cp.buffer == ""
        assert cp.token_count == 0

    def test_multiple_snapshots(self) -> None:
        """Multiple snapshots accumulate as more data is fed."""
        cp = StreamingCheckpointer(interval=5)
        # First: 24 chars = 6 tokens → snapshot 0
        cp.feed("A" * 24)
        # More: 48 chars total = 12 tokens → snapshot 1
        cp.feed("B" * 24)
        assert len(cp.snapshots) >= 2


class TestStreamStepDecorator:
    """@stream_step decorator tests."""

    def test_requires_step_decorator(self) -> None:
        """@stream_step without @step raises ConfigurationError."""
        with pytest.raises(ConfigurationError, match="outside @step"):

            @stream_step(checkpoint_interval=50)
            async def bare_gen(prompt: str):  # type: ignore[no-untyped-def]
                yield prompt

    @pytest.mark.asyncio()
    async def test_collects_async_generator(self) -> None:
        """@stream_step collects all yielded chunks into a single string."""

        @stream_step(checkpoint_interval=1000)
        @step(1)
        async def gen(prompt: str):  # type: ignore[no-untyped-def]
            yield "Hello "
            yield "from "
            yield "stream"

        sd: StepDefinition = gen.__gravtory_step__
        result = await sd.function("test")  # type: ignore[misc]
        assert result == "Hello from stream"


class TestStreamingGapFill:
    """Gap-fill tests for streaming edge cases."""

    @pytest.mark.asyncio()
    async def test_single_chunk(self) -> None:
        @stream_step(checkpoint_interval=1000)
        @step(1)
        async def gen(prompt: str):  # type: ignore[no-untyped-def]
            yield "only chunk"

        sd: StepDefinition = gen.__gravtory_step__
        result = await sd.function("test")  # type: ignore[misc]
        assert result == "only chunk"

    @pytest.mark.asyncio()
    async def test_empty_stream(self) -> None:
        @stream_step(checkpoint_interval=1000)
        @step(1)
        async def gen(prompt: str):  # type: ignore[no-untyped-def]
            return
            yield  # Make it a generator

        sd: StepDefinition = gen.__gravtory_step__
        result = await sd.function("test")  # type: ignore[misc]
        assert result == ""

    @pytest.mark.asyncio()
    async def test_many_small_chunks(self) -> None:
        @stream_step(checkpoint_interval=5)
        @step(1)
        async def gen(prompt: str):  # type: ignore[no-untyped-def]
            for c in "ABCDEFGHIJ":
                yield c

        sd: StepDefinition = gen.__gravtory_step__
        result = await sd.function("test")  # type: ignore[misc]
        assert result == "ABCDEFGHIJ"
