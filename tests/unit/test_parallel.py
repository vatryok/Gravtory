"""Unit tests for ParallelExecutor."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from gravtory.core.parallel import ParallelExecutor


class TestParallelExecutor:
    """Tests for parallel fan-out/fan-in execution."""

    @pytest.mark.asyncio
    async def test_parallel_all_items(self) -> None:
        """All items are processed and results returned."""
        executor = ParallelExecutor(max_concurrency=10)

        async def double(x: int) -> int:
            return x * 2

        results = await executor.execute(double, [1, 2, 3, 4, 5])
        assert results == [2, 4, 6, 8, 10]

    @pytest.mark.asyncio
    async def test_parallel_respects_concurrency(self) -> None:
        """Only max_concurrency items run at a time."""
        executor = ParallelExecutor(max_concurrency=2)
        active = 0
        max_active = 0

        async def track(x: int) -> int:
            nonlocal active, max_active
            active += 1
            max_active = max(max_active, active)
            await asyncio.sleep(0.01)
            active -= 1
            return x

        results = await executor.execute(track, [1, 2, 3, 4, 5])
        assert results == [1, 2, 3, 4, 5]
        assert max_active <= 2

    @pytest.mark.asyncio
    async def test_parallel_resume_completed(self) -> None:
        """Already-completed items are not re-processed."""
        executor = ParallelExecutor(max_concurrency=10)
        processed: list[int] = []

        async def track(x: int) -> int:
            processed.append(x)
            return x * 10

        completed = {0: 10, 2: 30}  # indices 0 and 2 already done
        results = await executor.execute(track, [1, 2, 3, 4, 5], completed=completed)

        # Items at index 0 and 2 should NOT have been re-processed
        assert 1 not in processed
        assert 3 not in processed
        # Items at index 1, 3, 4 were processed
        assert sorted(processed) == [2, 4, 5]
        # Results in correct order: completed values + new values
        assert results == [10, 20, 30, 40, 50]

    @pytest.mark.asyncio
    async def test_parallel_empty_items(self) -> None:
        """Empty items list returns empty results."""
        executor = ParallelExecutor(max_concurrency=10)

        async def noop(x: int) -> int:
            return x

        results = await executor.execute(noop, [])
        assert results == []

    @pytest.mark.asyncio
    async def test_parallel_single_item(self) -> None:
        """Single item works (degenerates to normal step)."""
        executor = ParallelExecutor(max_concurrency=10)

        async def inc(x: int) -> int:
            return x + 1

        results = await executor.execute(inc, [42])
        assert results == [43]

    @pytest.mark.asyncio
    async def test_parallel_preserves_order(self) -> None:
        """Results are returned in the same order as input items."""
        executor = ParallelExecutor(max_concurrency=3)

        async def delayed(x: int) -> int:
            # Vary delay so execution order differs from input order
            await asyncio.sleep(0.01 * (5 - x))
            return x

        results = await executor.execute(delayed, [1, 2, 3, 4, 5])
        assert results == [1, 2, 3, 4, 5]

    @pytest.mark.asyncio
    async def test_parallel_item_checkpoint_callback(self) -> None:
        """on_item_complete callback is called for each processed item."""
        executor = ParallelExecutor(max_concurrency=10)
        checkpointed: dict[int, Any] = {}

        def on_complete(index: int, result: Any) -> None:
            checkpointed[index] = result

        async def double(x: int) -> int:
            return x * 2

        results = await executor.execute(double, [10, 20, 30], on_item_complete=on_complete)
        assert results == [20, 40, 60]
        assert checkpointed == {0: 20, 1: 40, 2: 60}

    @pytest.mark.asyncio
    async def test_parallel_exception_propagates(self) -> None:
        """Exception in one item propagates (gather default)."""
        executor = ParallelExecutor(max_concurrency=10)

        async def maybe_fail(x: int) -> int:
            if x == 3:
                raise ValueError("item 3 failed")
            return x

        with pytest.raises(ValueError, match="item 3 failed"):
            await executor.execute(maybe_fail, [1, 2, 3, 4, 5])

    @pytest.mark.asyncio
    async def test_parallel_full_resume_no_execution(self) -> None:
        """If all items are already completed, no execution happens."""
        executor = ParallelExecutor(max_concurrency=10)
        called = False

        async def should_not_run(x: int) -> int:
            nonlocal called
            called = True
            return x

        completed = {0: 100, 1: 200, 2: 300}
        results = await executor.execute(should_not_run, [1, 2, 3], completed=completed)
        assert results == [100, 200, 300]
        assert not called


class TestParallelGapFill:
    """Gap-fill tests for parallel executor edge cases."""

    @pytest.mark.asyncio
    async def test_parallel_100_items(self) -> None:
        """100 items processed correctly with bounded concurrency."""
        executor = ParallelExecutor(max_concurrency=10)

        async def square(x: int) -> int:
            return x * x

        items = list(range(100))
        results = await executor.execute(square, items)
        assert results == [x * x for x in range(100)]

    @pytest.mark.asyncio
    async def test_parallel_concurrency_1(self) -> None:
        """Concurrency=1 means serial execution."""
        executor = ParallelExecutor(max_concurrency=1)
        execution_order: list[int] = []

        async def track_order(x: int) -> int:
            execution_order.append(x)
            return x

        results = await executor.execute(track_order, [1, 2, 3, 4, 5])
        assert results == [1, 2, 3, 4, 5]
        # With concurrency=1, items run sequentially in submission order
        assert execution_order == [1, 2, 3, 4, 5]

    @pytest.mark.asyncio
    async def test_parallel_with_sync_function(self) -> None:
        """Sync functions are executed via run_in_executor."""
        executor = ParallelExecutor(max_concurrency=5)

        def double_sync(x: int) -> int:
            return x * 2

        results = await executor.execute(double_sync, [1, 2, 3])
        assert results == [2, 4, 6]

    @pytest.mark.asyncio
    async def test_parallel_mixed_success_failure_propagates(self) -> None:
        """First failure propagates; successful items still complete."""
        executor = ParallelExecutor(max_concurrency=10)

        async def fail_on_3(x: int) -> int:
            if x == 3:
                raise RuntimeError("bad item")
            return x

        with pytest.raises(RuntimeError, match="bad item"):
            await executor.execute(fail_on_3, [1, 2, 3, 4, 5])


class TestParallelBatched:
    """Tests for batched parallel execution."""

    @pytest.mark.asyncio
    async def test_batched_execution(self) -> None:
        """Batched execution processes all items correctly."""
        executor = ParallelExecutor(max_concurrency=10)
        batches_received: list[dict[int, Any]] = []

        def on_batch(batch_results: dict[int, Any]) -> None:
            batches_received.append(dict(batch_results))

        async def double(x: int) -> int:
            return x * 2

        results = await executor.execute_batched(
            double, [1, 2, 3, 4, 5], batch_size=2, on_batch_complete=on_batch
        )
        assert results == [2, 4, 6, 8, 10]
        assert len(batches_received) == 3  # ceil(5/2) = 3 batches
