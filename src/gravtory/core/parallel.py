# SPDX-License-Identifier: AGPL-3.0-or-later OR Commercial
# Copyright (C) 2026 Gravtory Contributors
#
# This file is part of Gravtory, licensed under AGPL-3.0-or-later.
# See LICENSE file in the project root for full license information.

"""Parallel executor — bounded concurrency fan-out/fan-in with per-item checkpointing."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable


class ParallelExecutor:
    """Execute a step function for each item in parallel with bounded concurrency.

    Features:
      - Bounded concurrency via asyncio.Semaphore
      - Per-item checkpointing for resume support
      - Preserves original item order in results
      - Batched checkpointing option for large item lists
    """

    def __init__(self, max_concurrency: int = 10) -> None:
        self._max_concurrency = max_concurrency

    async def execute(
        self,
        func: Callable[..., Any],
        items: list[Any],
        *,
        completed: dict[int, Any] | None = None,
        on_item_complete: Callable[[int, Any], Any] | None = None,
    ) -> list[Any]:
        """Execute *func* for each item with bounded concurrency.

        Args:
            func: Async or sync callable to run on each item.
            items: List of input items.
            completed: Dict of already-completed {index: result} for resume.
            on_item_complete: Optional callback(index, result) called after each item.

        Returns:
            List of results in the same order as *items*.
        """
        if not items:
            return []

        completed = completed or {}

        # Filter to pending items only
        pending = [(i, item) for i, item in enumerate(items) if i not in completed]

        if not pending:
            # Full resume — all items already done
            return [completed[i] for i in range(len(items))]

        semaphore = asyncio.Semaphore(self._max_concurrency)

        async def process_one(index: int, item: Any) -> tuple[int, Any]:
            async with semaphore:
                if asyncio.iscoroutinefunction(func):
                    result = await func(item)
                else:
                    loop = asyncio.get_running_loop()
                    result = await loop.run_in_executor(None, func, item)
                if on_item_complete is not None:
                    cb_result = on_item_complete(index, result)
                    if asyncio.iscoroutine(cb_result):
                        await cb_result
                return index, result

        gather_results = await asyncio.gather(*[process_one(i, item) for i, item in pending])
        new_results = dict(gather_results)

        # Merge completed + new results in original order
        all_results: dict[int, Any] = {**completed, **new_results}
        return [all_results[i] for i in range(len(items))]

    async def execute_batched(
        self,
        func: Callable[..., Any],
        items: list[Any],
        batch_size: int = 100,
        *,
        completed: dict[int, Any] | None = None,
        on_batch_complete: Callable[[dict[int, Any]], Any] | None = None,
    ) -> list[Any]:
        """Execute with batched checkpointing for very large item lists.

        Processes items in batches of *batch_size*. After each batch,
        calls *on_batch_complete* with the batch results for bulk checkpoint.

        Trade-off: if crash mid-batch, up to batch_size items may need
        re-execution. But throughput is much higher.
        """
        if not items:
            return []

        completed = completed or {}
        pending = [(i, item) for i, item in enumerate(items) if i not in completed]

        if not pending:
            return [completed[i] for i in range(len(items))]

        all_new: dict[int, Any] = {}
        semaphore = asyncio.Semaphore(self._max_concurrency)

        # Process in batches
        for batch_start in range(0, len(pending), batch_size):
            batch = pending[batch_start : batch_start + batch_size]

            async def process_one(index: int, item: Any) -> tuple[int, Any]:
                async with semaphore:
                    if asyncio.iscoroutinefunction(func):
                        result = await func(item)
                    else:
                        loop = asyncio.get_running_loop()
                        result = await loop.run_in_executor(None, func, item)
                    return index, result

            gather_results = await asyncio.gather(*[process_one(i, item) for i, item in batch])
            batch_results = dict(gather_results)

            all_new.update(batch_results)

            if on_batch_complete is not None:
                cb_result = on_batch_complete(batch_results)
                if asyncio.iscoroutine(cb_result):
                    await cb_result

        merged = {**completed, **all_new}
        return [merged[i] for i in range(len(items))]
